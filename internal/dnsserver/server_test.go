package dnsserver

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/miekg/dns"

	"minikube-orchestrator/internal/model"
)

func TestDNSServerAnswersARecordForService(t *testing.T) {
	udpAddr, err := net.ResolveUDPAddr("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("resolve udp addr: %v", err)
	}
	conn, err := net.ListenUDP("udp", udpAddr)
	if err != nil {
		t.Fatalf("listen udp: %v", err)
	}
	addr := conn.LocalAddr().String()
	_ = conn.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	srv := New(addr, func(_ context.Context, _ string) (string, []model.ServiceEndpoint, int, error) {
		return "web.default.svc.cluster.local", []model.ServiceEndpoint{{Address: "127.0.0.1", Ready: true}}, 15, nil
	})
	go func() {
		_ = srv.Start(ctx)
	}()
	time.Sleep(150 * time.Millisecond)

	client := dns.Client{}
	msg := new(dns.Msg)
	msg.SetQuestion("web.default.svc.cluster.local.", dns.TypeA)
	resp, _, err := client.Exchange(msg, addr)
	if err != nil {
		t.Fatalf("dns exchange failed: %v", err)
	}
	if len(resp.Answer) == 0 {
		t.Fatal("expected at least one answer")
	}

	a, ok := resp.Answer[0].(*dns.A)
	if !ok {
		t.Fatalf("expected A answer, got %T", resp.Answer[0])
	}
	if got := a.A.String(); got != "127.0.0.1" {
		t.Fatalf("expected 127.0.0.1, got %s", got)
	}
	if gotTTL := a.Hdr.Ttl; gotTTL != 15 {
		t.Fatalf("expected ttl 15, got %d", gotTTL)
	}
}
