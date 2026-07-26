package controller

import (
	"context"
	"log"
	"time"

	"minikube-orchestrator/internal/service"
)

type Manager struct {
	orchestrator *service.Orchestrator
	interval     time.Duration
	controllerID string
	shardIndex   int
	shardTotal   int
	lease        time.Duration
}

func NewManager(o *service.Orchestrator, interval time.Duration, controllerID string, shardIndex, shardTotal int, lease time.Duration) *Manager {
	if shardTotal <= 0 {
		shardTotal = 1
	}
	if shardIndex < 0 {
		shardIndex = 0
	}
	if shardIndex >= shardTotal {
		shardIndex = shardIndex % shardTotal
	}
	if lease <= 0 {
		lease = 10 * time.Second
	}
	return &Manager{orchestrator: o, interval: interval, controllerID: controllerID, shardIndex: shardIndex, shardTotal: shardTotal, lease: lease}
}

func (m *Manager) Run(ctx context.Context) {
	ticker := time.NewTicker(m.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			_ = m.orchestrator.ReleaseControllerLeadership(context.Background(), m.controllerID)
			return
		case <-ticker.C:
			leader, lease, err := m.orchestrator.TryAcquireControllerLeadership(ctx, m.controllerID, m.lease)
			if err != nil {
				log.Printf("controller leadership error: %v", err)
				continue
			}
			if !leader {
				log.Printf("controller standby id=%s leader=%s term=%d", m.controllerID, lease.LeaderID, lease.Term)
				continue
			}
			if err := m.orchestrator.ReconcileShard(ctx, m.shardIndex, m.shardTotal); err != nil {
				log.Printf("controller reconcile error: %v", err)
			}
		}
	}
}
