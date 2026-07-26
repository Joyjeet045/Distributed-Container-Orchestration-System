package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"minikube-orchestrator/internal/agent"
	"minikube-orchestrator/internal/api"
	"minikube-orchestrator/internal/auth"
	"minikube-orchestrator/internal/model"
	"minikube-orchestrator/internal/scheduler"
	"minikube-orchestrator/internal/service"
	"minikube-orchestrator/internal/store"
	"minikube-orchestrator/internal/worker"
)

type fakeRuntime struct{}

func (fakeRuntime) RunWorkload(_ context.Context, workloadID, _ string, _ string) (string, error) {
	return "container-" + workloadID, nil
}

func (fakeRuntime) StopWorkload(_ context.Context, _, _ string) error {
	return nil
}

func (fakeRuntime) ListManagedWorkloads(_ context.Context) (map[string]string, error) {
	return map[string]string{}, nil
}

func TestEndToEndDeploymentFlow(t *testing.T) {
	t.Parallel()

	dbPath := filepath.Join(t.TempDir(), "badger")
	st, err := store.NewBadgerStateStore(dbPath)
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })

	orch := service.NewOrchestrator(st, scheduler.NewPlanner("least-loaded"), 20*time.Second)
	httpServer := api.NewHTTPServer(orch, auth.NewVerifier("static", "dev-token", "", "", "", ""))
	ts := httptest.NewServer(httpServer.Handler())
	t.Cleanup(ts.Close)

	client := agent.NewClient(ts.URL, "dev-token")
	runner := worker.NewRunner("worker-e2e", client, fakeRuntime{}, 3, 50*time.Millisecond)

	node := model.Node{
		ID:      "worker-e2e",
		Address: "worker-e2e",
		Labels: map[string]string{
			"zone": "a",
		},
		Capacity:    model.Resource{MilliCPU: 4000, MemoryMB: 8192},
		Allocatable: model.Resource{MilliCPU: 3500, MemoryMB: 7168},
	}
	if err := runner.Register(node); err != nil {
		t.Fatalf("register node: %v", err)
	}
	if err := runner.Heartbeat(); err != nil {
		t.Fatalf("initial heartbeat: %v", err)
	}

	reqBody := map[string]any{
		"spec": map[string]any{
			"name":     "web",
			"image":    "nginx:latest",
			"replicas": 2,
			"resources": map[string]any{
				"milliCPU": 250,
				"memoryMB": 128,
			},
			"labels": map[string]any{
				"affinity.zone": "a",
			},
		},
	}
	status, _, err := doJSONRequest(ts.URL+"/v1/deployments", "dev-token", http.MethodPost, reqBody)
	if err != nil {
		t.Fatalf("create deployment request failed: %v", err)
	}
	if status != http.StatusCreated {
		t.Fatalf("expected create deployment status 201, got %d", status)
	}

	for i := 0; i < 3; i++ {
		processed, err := runner.ProcessAssignments(context.Background(), 10)
		if err != nil {
			t.Fatalf("process assignments: %v", err)
		}
		if processed == 0 {
			break
		}
	}
	if err := runner.Heartbeat(); err != nil {
		t.Fatalf("post-work heartbeat: %v", err)
	}

	status, clusterBody, err := doJSONRequest(ts.URL+"/v1/cluster", "dev-token", http.MethodGet, nil)
	if err != nil {
		t.Fatalf("get cluster: %v", err)
	}
	if status != http.StatusOK {
		t.Fatalf("expected cluster status 200, got %d", status)
	}

	var state model.ClusterState
	if err := json.Unmarshal(clusterBody, &state); err != nil {
		t.Fatalf("decode cluster state: %v", err)
	}

	if len(state.Nodes) != 1 {
		t.Fatalf("expected 1 node, got %d", len(state.Nodes))
	}
	if len(state.Deployments) != 1 {
		t.Fatalf("expected 1 deployment, got %d", len(state.Deployments))
	}
	if len(state.Workloads) != 2 {
		t.Fatalf("expected 2 workloads, got %d", len(state.Workloads))
	}

	for _, w := range state.Workloads {
		if w.Status != model.WorkloadRunning {
			t.Fatalf("expected workload running, got %s", w.Status)
		}
		if w.ContainerID == "" {
			t.Fatal("expected container id to be set")
		}
	}
}

func TestAuthRequired(t *testing.T) {
	t.Parallel()

	dbPath := filepath.Join(t.TempDir(), "badger")
	st, err := store.NewBadgerStateStore(dbPath)
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })

	orch := service.NewOrchestrator(st, scheduler.NewPlanner("least-loaded"), 20*time.Second)
	httpServer := api.NewHTTPServer(orch, auth.NewVerifier("static", "dev-token", "", "", "", ""))
	ts := httptest.NewServer(httpServer.Handler())
	t.Cleanup(ts.Close)

	status, _, err := doJSONRequest(ts.URL+"/v1/cluster", "", http.MethodGet, nil)
	if err != nil {
		t.Fatalf("cluster request: %v", err)
	}
	if status != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", status)
	}
}

func TestChaosNodeLossTriggersRemediation(t *testing.T) {
	t.Parallel()

	dbPath := filepath.Join(t.TempDir(), "badger")
	st, err := store.NewBadgerStateStore(dbPath)
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })

	orch := service.NewOrchestrator(st, scheduler.NewPlanner("least-loaded"), 2*time.Second)
	httpServer := api.NewHTTPServer(orch, auth.NewVerifier("static", "dev-token", "", "", "", ""))
	ts := httptest.NewServer(httpServer.Handler())
	t.Cleanup(ts.Close)

	client := agent.NewClient(ts.URL, "dev-token")
	runner1 := worker.NewRunner("worker-a", client, fakeRuntime{}, 3, 50*time.Millisecond)
	runner2 := worker.NewRunner("worker-b", client, fakeRuntime{}, 3, 50*time.Millisecond)

	if err := orch.RegisterNode(context.Background(), model.Node{ID: "worker-a", Address: "worker-a", Capacity: model.Resource{MilliCPU: 4000, MemoryMB: 8192}, Allocatable: model.Resource{MilliCPU: 3500, MemoryMB: 7168}}); err != nil {
		t.Fatalf("register worker-a: %v", err)
	}
	if err := orch.RegisterNode(context.Background(), model.Node{ID: "worker-b", Address: "worker-b", Capacity: model.Resource{MilliCPU: 4000, MemoryMB: 8192}, Allocatable: model.Resource{MilliCPU: 3500, MemoryMB: 7168}}); err != nil {
		t.Fatalf("register worker-b: %v", err)
	}
	_ = runner1.Heartbeat()
	_ = runner2.Heartbeat()

	reqBody := map[string]any{"spec": map[string]any{"name": "chaos-web", "image": "nginx:latest", "replicas": 2, "resources": map[string]any{"milliCPU": 250, "memoryMB": 128}}}
	status, _, err := doJSONRequest(ts.URL+"/v1/deployments", "dev-token", http.MethodPost, reqBody)
	if err != nil || status != http.StatusCreated {
		t.Fatalf("create deployment status=%d err=%v", status, err)
	}

	for i := 0; i < 3; i++ {
		_, _ = runner1.ProcessAssignments(context.Background(), 10)
		_, _ = runner2.ProcessAssignments(context.Background(), 10)
	}

	nodeA, err := st.GetNode("worker-a")
	if err != nil {
		t.Fatalf("get node a: %v", err)
	}
	nodeA.LastSeen = time.Now().UTC().Add(-10 * time.Second)
	if err := st.UpsertNode(nodeA); err != nil {
		t.Fatalf("mark node a stale: %v", err)
	}

	if err := orch.ReconcileAll(context.Background()); err != nil {
		t.Fatalf("reconcile after node loss: %v", err)
	}

	assignA, err := orch.PollAssignments(context.Background(), "worker-a", 20)
	if err != nil {
		t.Fatalf("poll worker-a assignments: %v", err)
	}
	foundDelete := false
	for _, a := range assignA {
		if a.Action == "delete" {
			foundDelete = true
			break
		}
	}
	if !foundDelete {
		t.Fatal("expected delete assignment for lost node remediation")
	}
}

func TestChaosControllerRestartPreservesState(t *testing.T) {
	t.Parallel()

	dbPath := filepath.Join(t.TempDir(), "badger")
	st, err := store.NewBadgerStateStore(dbPath)
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })

	orch := service.NewOrchestrator(st, scheduler.NewPlanner("least-loaded"), 20*time.Second)
	if err := orch.RegisterNode(context.Background(), model.Node{ID: "worker-r", Address: "worker-r", Capacity: model.Resource{MilliCPU: 4000, MemoryMB: 8192}, Allocatable: model.Resource{MilliCPU: 3500, MemoryMB: 7168}}); err != nil {
		t.Fatalf("register node: %v", err)
	}
	dep, err := orch.CreateDeployment(context.Background(), model.DeploymentSpec{Name: "restart-test", Image: "nginx:latest", Replicas: 1, Resources: model.Resource{MilliCPU: 250, MemoryMB: 128}})
	if err != nil {
		t.Fatalf("create deployment: %v", err)
	}

	orchAfterRestart := service.NewOrchestrator(st, scheduler.NewPlanner("least-loaded"), 20*time.Second)
	if err := orchAfterRestart.ReconcileAll(context.Background()); err != nil {
		t.Fatalf("reconcile after restart: %v", err)
	}

	got, err := st.GetDeployment(dep.ID)
	if err != nil {
		t.Fatalf("get deployment after restart reconcile: %v", err)
	}
	if got.ID != dep.ID {
		t.Fatalf("expected deployment %s to persist after restart, got %s", dep.ID, got.ID)
	}
}

func doJSONRequest(url, token, method string, payload any) (int, []byte, error) {
	var bodyBytes []byte
	if payload != nil {
		var err error
		bodyBytes, err = json.Marshal(payload)
		if err != nil {
			return 0, nil, err
		}
	}

	req, err := http.NewRequest(method, url, bytes.NewReader(bodyBytes))
	if err != nil {
		return 0, nil, err
	}
	if payload != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if token != "" {
		req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", token))
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()

	blob := make([]byte, 0)
	buf := bytes.NewBuffer(blob)
	if _, err := buf.ReadFrom(resp.Body); err != nil {
		return 0, nil, err
	}
	return resp.StatusCode, buf.Bytes(), nil
}
