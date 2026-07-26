package loadbalancer

import (
	"errors"
	"sort"
	"sync"

	"minikube-orchestrator/internal/model"
)

const (
	StrategyRoundRobin       = "round-robin"
	StrategyLeastConnections = "least-connections"
)

type Balancer struct {
	mu         sync.Mutex
	rrIndex    map[string]int
	inflight   map[string]int
	filterOnly bool
}

func NewBalancer() *Balancer {
	return &Balancer{
		rrIndex:  map[string]int{},
		inflight: map[string]int{},
	}
}

func (b *Balancer) Select(serviceID, strategy string, endpoints []model.ServiceEndpoint) (model.ServiceEndpoint, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	ready := make([]model.ServiceEndpoint, 0, len(endpoints))
	for _, ep := range endpoints {
		if ep.Ready {
			ready = append(ready, ep)
		}
	}
	if len(ready) == 0 {
		return model.ServiceEndpoint{}, errors.New("no ready endpoints")
	}

	sort.Slice(ready, func(i, j int) bool {
		return ready[i].ID < ready[j].ID
	})

	switch strategy {
	case "", StrategyRoundRobin:
		idx := b.rrIndex[serviceID] % len(ready)
		b.rrIndex[serviceID] = (idx + 1) % len(ready)
		selected := ready[idx]
		b.inflight[selected.ID]++
		return selected, nil
	case StrategyLeastConnections:
		selected := ready[0]
		best := b.inflight[selected.ID]
		for _, ep := range ready[1:] {
			if c := b.inflight[ep.ID]; c < best {
				selected = ep
				best = c
			}
		}
		b.inflight[selected.ID]++
		return selected, nil
	default:
		return model.ServiceEndpoint{}, errors.New("unsupported balancing strategy")
	}
}

func (b *Balancer) Release(endpointID string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.inflight[endpointID] > 0 {
		b.inflight[endpointID]--
	}
}
