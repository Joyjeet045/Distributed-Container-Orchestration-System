package worker

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"minikube-orchestrator/internal/model"
)

type fakeClient struct {
	mu              sync.Mutex
	assignments     []model.Assignment
	desiredWorkload []model.Workload
	reported        map[string]model.WorkloadStatus
}

func (c *fakeClient) RegisterNode(_ model.Node) error            { return nil }
func (c *fakeClient) Heartbeat(_ string, _ model.Resource) error { return nil }
func (c *fakeClient) PollAssignments(_ string, _ int) ([]model.Assignment, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]model.Assignment, len(c.assignments))
	copy(out, c.assignments)
	c.assignments = nil
	return out, nil
}
func (c *fakeClient) ListNodeWorkloads(_ string) ([]model.Workload, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]model.Workload, len(c.desiredWorkload))
	copy(out, c.desiredWorkload)
	return out, nil
}
func (c *fakeClient) ReportWorkloadStatus(workloadID string, status model.WorkloadStatus, _ string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.reported == nil {
		c.reported = map[string]model.WorkloadStatus{}
	}
	c.reported[workloadID] = status
	return nil
}

type fakeRuntime struct {
	mu            sync.Mutex
	failFirstRuns int
	managed       map[string]string
	stopped       map[string]bool
}

func (r *fakeRuntime) RunWorkload(_ context.Context, workloadID, _ string, _ string) (string, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.managed == nil {
		r.managed = map[string]string{}
	}
	if r.failFirstRuns > 0 {
		r.failFirstRuns--
		return "", errors.New("run failed")
	}
	id := "ctr-" + workloadID
	r.managed[workloadID] = id
	return id, nil
}
func (r *fakeRuntime) StopWorkload(_ context.Context, workloadID, _ string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.stopped == nil {
		r.stopped = map[string]bool{}
	}
	r.stopped[workloadID] = true
	delete(r.managed, workloadID)
	return nil
}
func (r *fakeRuntime) ListManagedWorkloads(_ context.Context) (map[string]string, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := map[string]string{}
	for k, v := range r.managed {
		out[k] = v
	}
	return out, nil
}

func TestRunnerCreateWithBackoffAndDeleteAction(t *testing.T) {
	client := &fakeClient{assignments: []model.Assignment{
		{Action: "create", WorkloadID: "w1", Image: "nginx:latest", Resources: model.Resource{MilliCPU: 100, MemoryMB: 100}},
		{Action: "delete", WorkloadID: "w2", Resources: model.Resource{MilliCPU: 50, MemoryMB: 50}},
	}}
	runtime := &fakeRuntime{failFirstRuns: 1}
	r := NewRunner("node-1", client, runtime, 3, 5*time.Millisecond)

	processed, err := r.ProcessAssignments(context.Background(), 10)
	if err != nil {
		t.Fatalf("process assignments: %v", err)
	}
	if processed != 2 {
		t.Fatalf("expected 2 processed, got %d", processed)
	}
	if client.reported["w1"] != model.WorkloadRunning {
		t.Fatalf("expected w1 running, got %s", client.reported["w1"])
	}
	if client.reported["w2"] != model.WorkloadTerminated {
		t.Fatalf("expected w2 terminated, got %s", client.reported["w2"])
	}
}

func TestRunnerRuntimeDriftReconcile(t *testing.T) {
	client := &fakeClient{desiredWorkload: []model.Workload{{ID: "w1", NodeID: "node-1", Status: model.WorkloadRunning}}}
	runtime := &fakeRuntime{managed: map[string]string{"w2": "ctr-w2"}}
	r := NewRunner("node-1", client, runtime, 2, 5*time.Millisecond)

	if err := r.ReconcileRuntime(context.Background()); err != nil {
		t.Fatalf("reconcile runtime: %v", err)
	}
	if client.reported["w1"] != model.WorkloadFailed {
		t.Fatalf("expected missing desired workload reported failed, got %s", client.reported["w1"])
	}
	if !runtime.stopped["w2"] {
		t.Fatal("expected orphan workload w2 to be stopped")
	}
}
