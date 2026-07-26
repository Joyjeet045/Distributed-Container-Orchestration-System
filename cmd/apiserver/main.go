package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"minikube-orchestrator/internal/api"
	"minikube-orchestrator/internal/auth"
	"minikube-orchestrator/internal/config"
	"minikube-orchestrator/internal/controller"
	"minikube-orchestrator/internal/dnsserver"
	"minikube-orchestrator/internal/model"
	"minikube-orchestrator/internal/scheduler"
	"minikube-orchestrator/internal/service"
	"minikube-orchestrator/internal/store"
)

func main() {
	cfg := config.LoadAPIServerConfig()

	st, err := store.NewBadgerStateStore(cfg.DBPath)
	if err != nil {
		log.Fatalf("store init failed: %v", err)
	}
	defer st.Close()

	orch := service.NewOrchestrator(st, scheduler.NewPlanner(cfg.SchedulerPolicy), 20*time.Second)
	ctrl := controller.NewManager(
		orch,
		3*time.Second,
		cfg.ControllerID,
		cfg.ControllerShard,
		cfg.ControllerTotal,
		time.Duration(cfg.ControllerLease)*time.Second,
	)

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer cancel()

	go ctrl.Run(ctx)

	if cfg.EnableDNSServer {
		dnsAddrs := append([]string{cfg.DNSAddr}, cfg.DNSExtraAddrs...)
		for _, dnsAddr := range dnsAddrs {
			dnsAddr := dnsAddr
			dnsSrv := dnsserver.New(dnsAddr, func(resCtx context.Context, name string) (string, []model.ServiceEndpoint, int, error) {
				return orch.ResolveServiceDNS(resCtx, name)
			})
			go func() {
				log.Printf("DNS server listening on %s", dnsAddr)
				if err := dnsSrv.Start(ctx); err != nil {
					log.Printf("dns server stopped on %s: %v", dnsAddr, err)
				}
			}()
		}
	}

	verifier := auth.NewVerifier(cfg.AuthMode, cfg.APIToken, cfg.JWTSecret, cfg.JWTIssuer, cfg.JWTAudience, cfg.JWKSURL)
	httpServer := api.NewHTTPServer(orch, verifier)
	grpcServer := api.NewGRPCServer(orch)

	go func() {
		log.Printf("gRPC server listening on %s", cfg.GRPCAddr)
		if err := grpcServer.Start(cfg.GRPCAddr); err != nil {
			log.Fatalf("gRPC server failed: %v", err)
		}
	}()

	srv := &http.Server{
		Addr:    cfg.HTTPAddr,
		Handler: httpServer.Handler(),
	}
	serveFn := func() error {
		log.Printf("HTTP server listening on %s", cfg.HTTPAddr)
		return srv.ListenAndServe()
	}

	if cfg.HTTPSEnabled {
		reloader, err := newCertReloader(cfg.TLSCertFile, cfg.TLSKeyFile)
		if err != nil {
			log.Fatalf("tls configuration failed: %v", err)
		}
		srv.Addr = cfg.HTTPSAddr
		srv.TLSConfig = &tls.Config{
			MinVersion:     tls.VersionTLS12,
			GetCertificate: reloader.GetCertificate,
		}
		serveFn = func() error {
			if cfg.TLSClientCAFile != "" {
				caPEM, err := os.ReadFile(cfg.TLSClientCAFile)
				if err != nil {
					return fmt.Errorf("read client CA file: %w", err)
				}
				pool := x509.NewCertPool()
				if !pool.AppendCertsFromPEM(caPEM) {
					return fmt.Errorf("parse client CA PEM failed")
				}
				srv.TLSConfig.ClientCAs = pool
				srv.TLSConfig.ClientAuth = tls.RequireAndVerifyClientCert
				log.Printf("HTTPS mTLS enabled with client CA %s", cfg.TLSClientCAFile)
			}
			log.Printf("HTTPS server listening on %s (cert hot-reload enabled)", cfg.HTTPSAddr)
			return srv.ListenAndServeTLS("", "")
		}
	}

	go func() {
		<-ctx.Done()
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer shutdownCancel()
		if err := srv.Shutdown(shutdownCtx); err != nil {
			log.Printf("http shutdown error: %v", err)
		}
	}()

	if err := serveFn(); err != nil && err != http.ErrServerClosed {
		log.Fatalf("http server failed: %v", err)
	}
}

type certReloader struct {
	mu            sync.RWMutex
	certPath      string
	keyPath       string
	cert          *tls.Certificate
	certModTime   time.Time
	keyModTime    time.Time
	lastChecked   time.Time
	checkInterval time.Duration
}

func newCertReloader(certPath, keyPath string) (*certReloader, error) {
	if certPath == "" || keyPath == "" {
		return nil, fmt.Errorf("ORCH_TLS_CERT_FILE and ORCH_TLS_KEY_FILE are required when ORCH_HTTPS_ENABLED=true")
	}
	r := &certReloader{
		certPath:      certPath,
		keyPath:       keyPath,
		checkInterval: 5 * time.Second,
	}
	if err := r.reload(true); err != nil {
		return nil, err
	}
	return r, nil
}

func (r *certReloader) GetCertificate(_ *tls.ClientHelloInfo) (*tls.Certificate, error) {
	if err := r.reload(false); err != nil {
		return nil, err
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.cert == nil {
		return nil, fmt.Errorf("tls certificate is not loaded")
	}
	return r.cert, nil
}

func (r *certReloader) reload(force bool) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	now := time.Now()
	if !force && now.Sub(r.lastChecked) < r.checkInterval {
		return nil
	}
	r.lastChecked = now

	certInfo, err := os.Stat(r.certPath)
	if err != nil {
		return fmt.Errorf("stat cert file: %w", err)
	}
	keyInfo, err := os.Stat(r.keyPath)
	if err != nil {
		return fmt.Errorf("stat key file: %w", err)
	}

	if !force && !certInfo.ModTime().After(r.certModTime) && !keyInfo.ModTime().After(r.keyModTime) {
		return nil
	}

	loaded, err := tls.LoadX509KeyPair(r.certPath, r.keyPath)
	if err != nil {
		return fmt.Errorf("load tls key pair: %w", err)
	}
	r.cert = &loaded
	r.certModTime = certInfo.ModTime()
	r.keyModTime = keyInfo.ModTime()
	log.Printf("tls certificate reloaded cert=%s key=%s", r.certPath, r.keyPath)
	return nil
}
