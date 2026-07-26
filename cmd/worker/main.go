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
	"strconv"
	"strings"
	"syscall"
	"time"

	"minikube-orchestrator/internal/agent"
	"minikube-orchestrator/internal/config"
	"minikube-orchestrator/internal/model"
	"minikube-orchestrator/internal/runtime"
	"minikube-orchestrator/internal/worker"
)

func main() {
	cfg := config.LoadWorkerConfig()
	httpClient, err := buildWorkerHTTPClient(cfg)
	if err != nil {
		log.Fatalf("worker client tls configuration failed: %v", err)
	}
	client := agent.NewClientWithHTTPClient(cfg.ServerURL, cfg.AgentToken, httpClient)

	workerRuntime, err := runtime.NewRuntimeFromDriver(cfg.RuntimeDriver)
	if err != nil {
		log.Fatalf("runtime init failed: %v", err)
	}

	runner := worker.NewRunner(cfg.WorkerID, client, workerRuntime, cfg.RestartMaxRetries, time.Duration(cfg.RestartBackoffMS)*time.Millisecond)

	node := model.Node{
		ID:      cfg.WorkerID,
		Address: cfg.WorkerID,
		Labels:  cfg.NodeLabels,
		Taints:  cfg.NodeTaints,
		Capacity: model.Resource{
			MilliCPU: 4000,
			MemoryMB: 8192,
		},
		Allocatable: model.Resource{
			MilliCPU: 3500,
			MemoryMB: 7168,
		},
	}
	if err := runner.Register(node); err != nil {
		log.Fatalf("worker registration failed: %v", err)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer cancel()

	heartbeatTicker := time.NewTicker(cfg.HeartbeatInterval)
	defer heartbeatTicker.Stop()
	pollTicker := time.NewTicker(2 * time.Second)
	defer pollTicker.Stop()

	if err := runner.Heartbeat(); err != nil {
		log.Printf("initial heartbeat failed: %v", err)
	}

	if cfg.MetricsAddr != "" {
		mux := http.NewServeMux()
		mux.HandleFunc("/metrics", func(w http.ResponseWriter, _ *http.Request) {
			used := runner.UsedResource()
			w.Header().Set("Content-Type", "text/plain; version=0.0.4")
			_, _ = w.Write([]byte("# HELP worker_used_millicpu Current worker used CPU in millicores\n"))
			_, _ = w.Write([]byte("# TYPE worker_used_millicpu gauge\n"))
			_, _ = w.Write([]byte("worker_used_millicpu " + strconv.FormatInt(used.MilliCPU, 10) + "\n"))
			_, _ = w.Write([]byte("# HELP worker_used_memory_mb Current worker used memory in MB\n"))
			_, _ = w.Write([]byte("# TYPE worker_used_memory_mb gauge\n"))
			_, _ = w.Write([]byte("worker_used_memory_mb " + strconv.FormatInt(used.MemoryMB, 10) + "\n"))
			_, _ = w.Write([]byte("# HELP worker_identity Worker identity label\n"))
			_, _ = w.Write([]byte("# TYPE worker_identity gauge\n"))
			_, _ = w.Write([]byte("worker_identity{node_id=\"" + runner.NodeID() + "\"} 1\n"))
		})
		go func() {
			log.Printf("worker metrics endpoint listening on %s", cfg.MetricsAddr)
			if err := http.ListenAndServe(cfg.MetricsAddr, mux); err != nil {
				log.Printf("worker metrics server stopped: %v", err)
			}
		}()
	}

	for {
		select {
		case <-ctx.Done():
			log.Println("worker shutting down")
			return
		case <-heartbeatTicker.C:
			if err := runner.Heartbeat(); err != nil {
				log.Printf("heartbeat failed: %v", err)
			}
		case <-pollTicker.C:
			if _, err := runner.ProcessAssignments(ctx, 5); err != nil {
				log.Printf("poll assignments failed: %v", err)
			}
			if err := runner.ReconcileRuntime(ctx); err != nil {
				log.Printf("runtime reconcile failed: %v", err)
			}
		}
	}
}

func buildWorkerHTTPClient(cfg config.WorkerConfig) (*http.Client, error) {
	transport := &http.Transport{}

	if strings.HasPrefix(strings.ToLower(cfg.ServerURL), "https://") {
		tlsCfg := &tls.Config{MinVersion: tls.VersionTLS12}
		if cfg.TLSServerName != "" {
			tlsCfg.ServerName = cfg.TLSServerName
		}

		if cfg.TLSCACertFile != "" {
			caPEM, err := os.ReadFile(cfg.TLSCACertFile)
			if err != nil {
				return nil, fmt.Errorf("read CA cert file: %w", err)
			}
			pool := x509.NewCertPool()
			if !pool.AppendCertsFromPEM(caPEM) {
				return nil, fmt.Errorf("append CA cert PEM failed")
			}
			tlsCfg.RootCAs = pool
		}

		if (cfg.TLSClientCertFile == "") != (cfg.TLSClientKeyFile == "") {
			return nil, fmt.Errorf("ORCH_TLS_CLIENT_CERT_FILE and ORCH_TLS_CLIENT_KEY_FILE must both be set")
		}
		if cfg.TLSClientCertFile != "" && cfg.TLSClientKeyFile != "" {
			cert, err := tls.LoadX509KeyPair(cfg.TLSClientCertFile, cfg.TLSClientKeyFile)
			if err != nil {
				return nil, fmt.Errorf("load client keypair: %w", err)
			}
			tlsCfg.Certificates = []tls.Certificate{cert}
		}

		transport.TLSClientConfig = tlsCfg
	}

	return &http.Client{Timeout: 20 * time.Second, Transport: transport}, nil
}
