package runtime

import (
	"context"
	"fmt"
	"strings"
	"sync"
)

type DriverFactory func() (ContainerRuntime, error)

var (
	driverMu sync.RWMutex
	drivers  = map[string]DriverFactory{}
)

func init() {
	RegisterDriver("docker", func() (ContainerRuntime, error) {
		return NewDockerRuntime()
	})
	RegisterDriver("inmemory", func() (ContainerRuntime, error) {
		return NewInMemoryRuntime(), nil
	})
}

func RegisterDriver(name string, factory DriverFactory) {
	driverMu.Lock()
	defer driverMu.Unlock()
	drivers[strings.ToLower(strings.TrimSpace(name))] = factory
}

func NewRuntimeFromDriver(name string) (ContainerRuntime, error) {
	name = strings.ToLower(strings.TrimSpace(name))
	if name == "" {
		name = "docker"
	}
	driverMu.RLock()
	factory, ok := drivers[name]
	driverMu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("runtime driver not found: %s", name)
	}
	return factory()
}

type InMemoryRuntime struct {
	mu        sync.Mutex
	workloads map[string]string
}

func NewInMemoryRuntime() *InMemoryRuntime {
	return &InMemoryRuntime{workloads: map[string]string{}}
}

func (r *InMemoryRuntime) RunWorkload(_ context.Context, workloadID, _ string, _ string) (string, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	containerID := "mem-" + workloadID
	r.workloads[workloadID] = containerID
	return containerID, nil
}

func (r *InMemoryRuntime) StopWorkload(_ context.Context, workloadID, containerID string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if workloadID != "" {
		delete(r.workloads, workloadID)
		return nil
	}
	for k, v := range r.workloads {
		if v == containerID {
			delete(r.workloads, k)
			break
		}
	}
	return nil
}

func (r *InMemoryRuntime) ListManagedWorkloads(_ context.Context) (map[string]string, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := map[string]string{}
	for k, v := range r.workloads {
		out[k] = v
	}
	return out, nil
}
