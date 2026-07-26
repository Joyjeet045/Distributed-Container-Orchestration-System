package dnsserver

import (
	"context"
	"log"
	"net"
	"strings"
	"time"

	"github.com/miekg/dns"

	"minikube-orchestrator/internal/model"
)

type Resolver func(ctx context.Context, name string) (string, []model.ServiceEndpoint, int, error)

type Server struct {
	addr     string
	resolver Resolver
	server   *dns.Server
}

func New(addr string, resolver Resolver) *Server {
	return &Server{addr: addr, resolver: resolver}
}

func (s *Server) Start(ctx context.Context) error {
	mux := dns.NewServeMux()
	mux.HandleFunc(".", s.handleQuery)

	s.server = &dns.Server{Addr: s.addr, Net: "udp", Handler: mux}

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = s.server.ShutdownContext(shutdownCtx)
	}()

	if err := s.server.ListenAndServe(); err != nil {
		return err
	}
	return nil
}

func (s *Server) handleQuery(w dns.ResponseWriter, req *dns.Msg) {
	resp := new(dns.Msg)
	resp.SetReply(req)
	resp.Authoritative = true

	for _, q := range req.Question {
		if q.Qtype != dns.TypeA {
			continue
		}
		name := strings.TrimSuffix(q.Name, ".")
		if !strings.HasSuffix(strings.ToLower(name), ".default.svc.cluster.local") {
			continue
		}
		svcName := strings.TrimSuffix(name, ".default.svc.cluster.local")
		_, endpoints, ttl, err := s.resolver(context.Background(), svcName)
		if err != nil {
			continue
		}
		if ttl <= 0 {
			ttl = 30
		}
		for _, ep := range endpoints {
			if !ep.Ready {
				continue
			}
			ip := net.ParseIP(ep.Address)
			if ip == nil {
				continue
			}
			if v4 := ip.To4(); v4 != nil {
				rr := &dns.A{
					Hdr: dns.RR_Header{Name: q.Name, Rrtype: dns.TypeA, Class: dns.ClassINET, Ttl: uint32(ttl)},
					A:   v4,
				}
				resp.Answer = append(resp.Answer, rr)
			}
		}
	}
	if err := w.WriteMsg(resp); err != nil {
		log.Printf("dns write response failed: %v", err)
	}
}
