package loadbalancer

import (
	"testing"

	"minikube-orchestrator/internal/model"
)

func TestRoundRobinSelection(t *testing.T) {
	b := NewBalancer()
	endpoints := []model.ServiceEndpoint{
		{ID: "ep-a", Ready: true},
		{ID: "ep-b", Ready: true},
	}

	first, err := b.Select("svc-1", StrategyRoundRobin, endpoints)
	if err != nil {
		t.Fatalf("first select: %v", err)
	}
	second, err := b.Select("svc-1", StrategyRoundRobin, endpoints)
	if err != nil {
		t.Fatalf("second select: %v", err)
	}
	if first.ID == second.ID {
		t.Fatalf("expected round robin to rotate endpoints, got %s twice", first.ID)
	}
}

func TestLeastConnectionsSelection(t *testing.T) {
	b := NewBalancer()
	endpoints := []model.ServiceEndpoint{
		{ID: "ep-a", Ready: true},
		{ID: "ep-b", Ready: true},
	}

	selected, err := b.Select("svc-1", StrategyLeastConnections, endpoints)
	if err != nil {
		t.Fatalf("select least connections: %v", err)
	}
	if selected.ID != "ep-a" {
		t.Fatalf("expected ep-a first by lexical tie-break, got %s", selected.ID)
	}

	next, err := b.Select("svc-1", StrategyLeastConnections, endpoints)
	if err != nil {
		t.Fatalf("second select least connections: %v", err)
	}
	if next.ID != "ep-b" {
		t.Fatalf("expected ep-b after ep-a has active connection, got %s", next.ID)
	}
}

func TestHealthAwareFilteringByReadyFlag(t *testing.T) {
	b := NewBalancer()
	endpoints := []model.ServiceEndpoint{
		{ID: "ep-a", Ready: false},
		{ID: "ep-b", Ready: true},
	}

	selected, err := b.Select("svc-1", StrategyRoundRobin, endpoints)
	if err != nil {
		t.Fatalf("select: %v", err)
	}
	if selected.ID != "ep-b" {
		t.Fatalf("expected only ready endpoint ep-b, got %s", selected.ID)
	}
}
