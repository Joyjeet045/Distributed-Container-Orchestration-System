package api

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"

	"minikube-orchestrator/internal/auth"
	"minikube-orchestrator/internal/model"
	"minikube-orchestrator/internal/scheduler"
	"minikube-orchestrator/internal/service"
	"minikube-orchestrator/internal/store"
)

func TestAPIVersionAliasRoute(t *testing.T) {
	ts := testHTTPServer(t, auth.NewVerifier("static", "dev-token", "", "", "", ""))

	req, err := http.NewRequest(http.MethodGet, ts.URL+"/api/v1/cluster", nil)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Authorization", "Bearer dev-token")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do request: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
	if got := resp.Header.Get("X-API-Version"); got != "v1" {
		t.Fatalf("expected X-API-Version=v1, got %q", got)
	}
}

func TestUnsupportedVersionRejected(t *testing.T) {
	ts := testHTTPServer(t, auth.NewVerifier("static", "dev-token", "", "", "", ""))

	req, err := http.NewRequest(http.MethodGet, ts.URL+"/v1/cluster", nil)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Authorization", "Bearer dev-token")
	req.Header.Set("X-API-Version", "v2")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do request: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", resp.StatusCode)
	}
}

func TestPublicMetricsEndpointDoesNotRequireAuth(t *testing.T) {
	ts := testHTTPServer(t, auth.NewVerifier("static", "dev-token", "", "", "", ""))

	req, err := http.NewRequest(http.MethodGet, ts.URL+"/metrics", nil)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do request: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	if !strings.Contains(string(body), "orch_nodes_total") {
		t.Fatalf("expected prometheus payload, got: %s", string(body))
	}
}

func TestShouldAuditRequest(t *testing.T) {
	cases := []struct {
		name   string
		method string
		path   string
		want   bool
	}{
		{name: "health excluded", method: http.MethodGet, path: "/healthz", want: false},
		{name: "public metrics excluded", method: http.MethodGet, path: "/metrics", want: false},
		{name: "internal metrics excluded", method: http.MethodGet, path: "/v1/metrics/prometheus", want: false},
		{name: "deployment create audited", method: http.MethodPost, path: "/v1/deployments", want: true},
		{name: "deployment update audited", method: http.MethodPut, path: "/v1/deployments/dep-1", want: true},
		{name: "deployment delete audited", method: http.MethodDelete, path: "/v1/deployments/dep-1", want: true},
		{name: "secrets read audited", method: http.MethodGet, path: "/v1/secrets", want: true},
		{name: "cluster read not audited", method: http.MethodGet, path: "/v1/cluster", want: false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := shouldAuditRequest(tc.method, tc.path); got != tc.want {
				t.Fatalf("shouldAuditRequest(%s, %s) = %v, want %v", tc.method, tc.path, got, tc.want)
			}
		})
	}
}

func TestJWTAuthorizationBlocksDeleteForViewer(t *testing.T) {
	secret := "test-secret"
	ts := testHTTPServer(t, auth.NewVerifier("jwt", "", secret, "", "", ""))

	token := signedToken(t, secret, []string{"viewer"})
	req, err := http.NewRequest(http.MethodDelete, ts.URL+"/v1/deployments/dep-1", nil)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do request: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("expected 403, got %d", resp.StatusCode)
	}
}

func TestValidationErrorShape(t *testing.T) {
	ts := testHTTPServer(t, auth.NewVerifier("static", "dev-token", "", "", "", ""))

	payload := map[string]any{
		"spec": map[string]any{
			"name":  "",
			"image": "",
		},
	}
	blob, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}

	req, err := http.NewRequest(http.MethodPost, ts.URL+"/v1/deployments", bytes.NewReader(blob))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Authorization", "Bearer dev-token")
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do request: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", resp.StatusCode)
	}

	var apiErr APIError
	if err := json.NewDecoder(resp.Body).Decode(&apiErr); err != nil {
		t.Fatalf("decode error response: %v", err)
	}
	if apiErr.Code != "validation_failed" {
		t.Fatalf("expected validation_failed code, got %q", apiErr.Code)
	}
}

func TestWorkerDesiredWorkloadsEndpoint(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "badger")
	st, err := store.NewBadgerStateStore(dbPath)
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })

	orch := service.NewOrchestrator(st, scheduler.NewPlanner("least-loaded"), 20*time.Second)
	if err := orch.RegisterNode(context.Background(), model.Node{
		ID:          "worker-1",
		Address:     "worker-1",
		Allocatable: model.Resource{MilliCPU: 1000, MemoryMB: 1000},
		Capacity:    model.Resource{MilliCPU: 1000, MemoryMB: 1000},
	}); err != nil {
		t.Fatalf("register node: %v", err)
	}
	if _, err := orch.CreateDeployment(context.Background(), model.DeploymentSpec{
		Name:      "api-workloads",
		Image:     "nginx:latest",
		Replicas:  1,
		Resources: model.Resource{MilliCPU: 100, MemoryMB: 100},
	}); err != nil {
		t.Fatalf("create deployment: %v", err)
	}
	if err := orch.ReconcileAll(context.Background()); err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	srv := NewHTTPServer(orch, auth.NewVerifier("static", "dev-token", "", "", "", ""))
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(ts.Close)

	req, err := http.NewRequest(http.MethodGet, ts.URL+"/v1/workers/worker-1/workloads", nil)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Authorization", "Bearer dev-token")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do request: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
	var payload struct {
		Workloads []model.Workload `json:"workloads"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		t.Fatalf("decode payload: %v", err)
	}
	if len(payload.Workloads) != 1 {
		t.Fatalf("expected 1 desired workload, got %d", len(payload.Workloads))
	}
}

func TestServiceDiscoveryEndpoints(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "badger")
	st, err := store.NewBadgerStateStore(dbPath)
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })

	orch := service.NewOrchestrator(st, scheduler.NewPlanner("least-loaded"), 20*time.Second)
	if err := orch.RegisterNode(context.Background(), model.Node{
		ID:          "worker-1",
		Address:     "10.1.0.8",
		Allocatable: model.Resource{MilliCPU: 1000, MemoryMB: 1000},
		Capacity:    model.Resource{MilliCPU: 1000, MemoryMB: 1000},
	}); err != nil {
		t.Fatalf("register node: %v", err)
	}
	if _, err := orch.CreateDeployment(context.Background(), model.DeploymentSpec{
		Name:      "svc-api",
		Image:     "nginx:latest",
		Replicas:  1,
		Resources: model.Resource{MilliCPU: 100, MemoryMB: 100},
		Labels:    map[string]string{"app": "api"},
	}); err != nil {
		t.Fatalf("create deployment: %v", err)
	}
	assignments, err := orch.PollAssignments(context.Background(), "worker-1", 10)
	if err != nil {
		t.Fatalf("poll assignments: %v", err)
	}
	for _, a := range assignments {
		if err := orch.ReportWorkloadStatus(context.Background(), a.WorkloadID, model.WorkloadRunning, "ctr-"+a.WorkloadID); err != nil {
			t.Fatalf("report status: %v", err)
		}
	}

	srv := NewHTTPServer(orch, auth.NewVerifier("static", "dev-token", "", "", "", ""))
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(ts.Close)

	body := map[string]any{
		"spec": map[string]any{
			"name":     "api-service",
			"selector": map[string]any{"app": "api"},
			"ports": []map[string]any{{
				"name":       "http",
				"port":       80,
				"targetPort": 8080,
				"protocol":   "TCP",
			}},
		},
	}
	blob, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	req, err := http.NewRequest(http.MethodPost, ts.URL+"/v1/services", bytes.NewReader(blob))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Authorization", "Bearer dev-token")
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("create service request: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("expected 201, got %d", resp.StatusCode)
	}
	var created model.Service
	if err := json.NewDecoder(resp.Body).Decode(&created); err != nil {
		t.Fatalf("decode created service: %v", err)
	}

	if err := orch.ReconcileAll(context.Background()); err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	req, _ = http.NewRequest(http.MethodGet, ts.URL+"/v1/services/"+created.ID+"/endpoints", nil)
	req.Header.Set("Authorization", "Bearer dev-token")
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("endpoints request: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected endpoints 200, got %d", resp.StatusCode)
	}
	var endpointPayload struct {
		Endpoints []model.ServiceEndpoint `json:"endpoints"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&endpointPayload); err != nil {
		t.Fatalf("decode endpoints: %v", err)
	}
	if len(endpointPayload.Endpoints) != 1 {
		t.Fatalf("expected 1 endpoint, got %d", len(endpointPayload.Endpoints))
	}

	req, _ = http.NewRequest(http.MethodGet, ts.URL+"/v1/dns/resolve?name=api-service", nil)
	req.Header.Set("Authorization", "Bearer dev-token")
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("dns resolve request: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected resolve 200, got %d", resp.StatusCode)
	}

	req, _ = http.NewRequest(http.MethodGet, ts.URL+"/v1/services/"+created.ID+"/proxy-target?strategy=least-connections", nil)
	req.Header.Set("Authorization", "Bearer dev-token")
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("proxy target request: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected proxy target 200, got %d", resp.StatusCode)
	}
}

func TestServiceNetworkConnectRouteWithNodePort(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "badger")
	st, err := store.NewBadgerStateStore(dbPath)
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })

	orch := service.NewOrchestrator(st, scheduler.NewPlanner("least-loaded"), 20*time.Second)
	if err := orch.RegisterNode(context.Background(), model.Node{
		ID:          "worker-1",
		Address:     "10.1.0.8",
		Allocatable: model.Resource{MilliCPU: 1000, MemoryMB: 1000},
		Capacity:    model.Resource{MilliCPU: 1000, MemoryMB: 1000},
	}); err != nil {
		t.Fatalf("register node: %v", err)
	}
	if _, err := orch.CreateDeployment(context.Background(), model.DeploymentSpec{
		Name:      "connect-api",
		Image:     "nginx:latest",
		Replicas:  1,
		Resources: model.Resource{MilliCPU: 100, MemoryMB: 100},
		Labels:    map[string]string{"app": "connect-api"},
	}); err != nil {
		t.Fatalf("create deployment: %v", err)
	}
	assignments, err := orch.PollAssignments(context.Background(), "worker-1", 10)
	if err != nil {
		t.Fatalf("poll assignments: %v", err)
	}
	for _, a := range assignments {
		if err := orch.ReportWorkloadStatus(context.Background(), a.WorkloadID, model.WorkloadRunning, "ctr-"+a.WorkloadID); err != nil {
			t.Fatalf("report status: %v", err)
		}
	}

	srv := NewHTTPServer(orch, auth.NewVerifier("static", "dev-token", "", "", "", ""))
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(ts.Close)

	body := map[string]any{
		"spec": map[string]any{
			"name":     "connect-service",
			"type":     "NodePort",
			"selector": map[string]any{"app": "connect-api"},
			"ports": []map[string]any{{
				"name":       "http",
				"port":       80,
				"targetPort": 8080,
				"nodePort":   30080,
				"protocol":   "TCP",
			}},
		},
	}
	blob, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal body: %v", err)
	}
	req, err := http.NewRequest(http.MethodPost, ts.URL+"/v1/services", bytes.NewReader(blob))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Authorization", "Bearer dev-token")
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("create service request: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("expected 201, got %d", resp.StatusCode)
	}

	if err := orch.ReconcileAll(context.Background()); err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	req, err = http.NewRequest(http.MethodGet, ts.URL+"/v1/network/services/connect-service/connect?strategy=least-connections", nil)
	if err != nil {
		t.Fatalf("new connect request: %v", err)
	}
	req.Header.Set("Authorization", "Bearer dev-token")
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("connect request: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected connect 200, got %d", resp.StatusCode)
	}
	var payload struct {
		RoutedHost string `json:"routedHost"`
		RoutedPort int    `json:"routedPort"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		t.Fatalf("decode connect payload: %v", err)
	}
	if payload.RoutedHost != "10.1.0.8" {
		t.Fatalf("expected routed host 10.1.0.8, got %s", payload.RoutedHost)
	}
	if payload.RoutedPort != 30080 {
		t.Fatalf("expected routed port 30080, got %d", payload.RoutedPort)
	}
}

func TestDeploymentUpdateAndRollbackEndpoints(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "badger")
	st, err := store.NewBadgerStateStore(dbPath)
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })

	orch := service.NewOrchestrator(st, scheduler.NewPlanner("least-loaded"), 20*time.Second)
	if err := orch.RegisterNode(context.Background(), model.Node{ID: "worker-1", Address: "10.1.0.8", Allocatable: model.Resource{MilliCPU: 1000, MemoryMB: 1000}, Capacity: model.Resource{MilliCPU: 1000, MemoryMB: 1000}}); err != nil {
		t.Fatalf("register node: %v", err)
	}

	srv := NewHTTPServer(orch, auth.NewVerifier("static", "dev-token", "", "", "", ""))
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(ts.Close)

	createBody, _ := json.Marshal(map[string]any{"spec": map[string]any{"name": "rollout-api", "image": "nginx:v1", "replicas": 1, "resources": map[string]any{"milliCPU": 100, "memoryMB": 100}}})
	req, _ := http.NewRequest(http.MethodPost, ts.URL+"/v1/deployments", bytes.NewReader(createBody))
	req.Header.Set("Authorization", "Bearer dev-token")
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("create deployment request: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("expected 201, got %d", resp.StatusCode)
	}
	var created model.Deployment
	if err := json.NewDecoder(resp.Body).Decode(&created); err != nil {
		t.Fatalf("decode created deployment: %v", err)
	}

	updateBody, _ := json.Marshal(map[string]any{"spec": map[string]any{"name": "rollout-api", "image": "nginx:v2", "replicas": 1, "resources": map[string]any{"milliCPU": 100, "memoryMB": 100}, "rollout": map[string]any{"strategy": "RollingUpdate", "rollingUpdate": map[string]any{"maxSurge": 1, "maxUnavailable": 0}}}})
	req, _ = http.NewRequest(http.MethodPut, ts.URL+"/v1/deployments/"+created.ID, bytes.NewReader(updateBody))
	req.Header.Set("Authorization", "Bearer dev-token")
	req.Header.Set("Content-Type", "application/json")
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("update deployment request: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 on update, got %d", resp.StatusCode)
	}

	rollbackBody, _ := json.Marshal(map[string]any{"revision": 1})
	req, _ = http.NewRequest(http.MethodPost, ts.URL+"/v1/deployments/"+created.ID+"/rollback", bytes.NewReader(rollbackBody))
	req.Header.Set("Authorization", "Bearer dev-token")
	req.Header.Set("Content-Type", "application/json")
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("rollback deployment request: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 on rollback, got %d", resp.StatusCode)
	}
	var rolled model.Deployment
	if err := json.NewDecoder(resp.Body).Decode(&rolled); err != nil {
		t.Fatalf("decode rolled deployment: %v", err)
	}
	if rolled.Spec.Image != "nginx:v1" {
		t.Fatalf("expected rollback to nginx:v1, got %s", rolled.Spec.Image)
	}
}

func TestDNSSidecarResolveEndpoint(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "badger")
	st, err := store.NewBadgerStateStore(dbPath)
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })

	orch := service.NewOrchestrator(st, scheduler.NewPlanner("least-loaded"), 20*time.Second)
	if err := orch.RegisterNode(context.Background(), model.Node{
		ID:          "worker-1",
		Address:     "10.1.0.8",
		Allocatable: model.Resource{MilliCPU: 1000, MemoryMB: 1000},
		Capacity:    model.Resource{MilliCPU: 1000, MemoryMB: 1000},
	}); err != nil {
		t.Fatalf("register node: %v", err)
	}
	if _, err := orch.CreateDeployment(context.Background(), model.DeploymentSpec{
		Name:      "sidecar-api",
		Image:     "nginx:latest",
		Replicas:  1,
		Resources: model.Resource{MilliCPU: 100, MemoryMB: 100},
		Labels:    map[string]string{"app": "sidecar-api"},
	}); err != nil {
		t.Fatalf("create deployment: %v", err)
	}
	assignments, err := orch.PollAssignments(context.Background(), "worker-1", 10)
	if err != nil {
		t.Fatalf("poll assignments: %v", err)
	}
	for _, a := range assignments {
		if err := orch.ReportWorkloadStatus(context.Background(), a.WorkloadID, model.WorkloadRunning, "ctr-"+a.WorkloadID); err != nil {
			t.Fatalf("report status: %v", err)
		}
	}
	if _, err := orch.CreateService(context.Background(), model.ServiceSpec{
		Name:     "sidecar-service",
		Selector: map[string]string{"app": "sidecar-api"},
		Ports: []model.ServicePort{{
			Name:       "http",
			Port:       80,
			TargetPort: 8080,
			Protocol:   model.ServiceProtocolTCP,
		}},
	}); err != nil {
		t.Fatalf("create service: %v", err)
	}
	if err := orch.ReconcileAll(context.Background()); err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	srv := NewHTTPServer(orch, auth.NewVerifier("static", "dev-token", "", "", "", ""))
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(ts.Close)

	req, err := http.NewRequest(http.MethodGet, ts.URL+"/v1/dns/sidecar/resolve?name=sidecar-service", nil)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Authorization", "Bearer dev-token")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("sidecar resolve request: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
	var payload struct {
		Name       string                  `json:"name"`
		TTLSeconds int                     `json:"ttlSeconds"`
		Endpoints  []model.ServiceEndpoint `json:"endpoints"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		t.Fatalf("decode payload: %v", err)
	}
	if payload.Name != "sidecar-service.default.svc.cluster.local" {
		t.Fatalf("unexpected name: %s", payload.Name)
	}
	if payload.TTLSeconds <= 0 {
		t.Fatalf("expected positive ttl, got %d", payload.TTLSeconds)
	}
	if len(payload.Endpoints) == 0 {
		t.Fatal("expected at least one endpoint")
	}
}

func TestAutoscalerAndMetricsEndpoints(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "badger")
	st, err := store.NewBadgerStateStore(dbPath)
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })

	orch := service.NewOrchestrator(st, scheduler.NewPlanner("least-loaded"), 20*time.Second)
	if err := orch.RegisterNode(context.Background(), model.Node{ID: "worker-1", Address: "10.1.0.8", Allocatable: model.Resource{MilliCPU: 1000, MemoryMB: 1000}, Capacity: model.Resource{MilliCPU: 1000, MemoryMB: 1000}}); err != nil {
		t.Fatalf("register node: %v", err)
	}
	dep, err := orch.CreateDeployment(context.Background(), model.DeploymentSpec{Name: "autoscaler-api", Image: "nginx:latest", Replicas: 1, Resources: model.Resource{MilliCPU: 100, MemoryMB: 100}})
	if err != nil {
		t.Fatalf("create deployment: %v", err)
	}

	srv := NewHTTPServer(orch, auth.NewVerifier("static", "dev-token", "", "", "", ""))
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(ts.Close)

	policyBody, _ := json.Marshal(map[string]any{"policy": map[string]any{"deploymentId": dep.ID, "minReplicas": 1, "maxReplicas": 3, "targetCPUUtilization": 0.5, "stabilizationWindowSec": 10, "scaleUpCooldownSec": 30, "scaleDownCooldownSec": 30, "predictiveScalingEnabled": true, "predictiveLookbackSamples": 4, "predictiveScaleFactor": 2}})
	req, _ := http.NewRequest(http.MethodPost, ts.URL+"/v1/autoscalers", bytes.NewReader(policyBody))
	req.Header.Set("Authorization", "Bearer dev-token")
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("create autoscaler request: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("expected 201, got %d", resp.StatusCode)
	}

	req, _ = http.NewRequest(http.MethodGet, ts.URL+"/v1/autoscalers", nil)
	req.Header.Set("Authorization", "Bearer dev-token")
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("list autoscalers request: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
	var listed struct {
		Policies []model.AutoscalerPolicy `json:"policies"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&listed); err != nil {
		t.Fatalf("decode policies: %v", err)
	}
	if len(listed.Policies) != 1 {
		t.Fatalf("expected 1 autoscaler policy, got %d", len(listed.Policies))
	}

	metricBody, _ := json.Marshal(map[string]any{"metric": map[string]any{"deploymentId": dep.ID, "cpuUsage": 0.8, "memoryUsage": 0.5, "custom": map[string]any{"rps": 120}}})
	req, _ = http.NewRequest(http.MethodPost, ts.URL+"/v1/metrics/deployments", bytes.NewReader(metricBody))
	req.Header.Set("Authorization", "Bearer dev-token")
	req.Header.Set("Content-Type", "application/json")
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("ingest metric request: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("expected 202, got %d", resp.StatusCode)
	}

	if err := orch.HeartbeatNode(context.Background(), "worker-1", model.Resource{MilliCPU: 920, MemoryMB: 930}); err != nil {
		t.Fatalf("heartbeat node: %v", err)
	}
	if err := orch.ReconcileAll(context.Background()); err != nil {
		t.Fatalf("reconcile for node health samples: %v", err)
	}

	req, _ = http.NewRequest(http.MethodGet, ts.URL+"/v1/health/nodes/trends?window=10", nil)
	req.Header.Set("Authorization", "Bearer dev-token")
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("node health trends request: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 from trends endpoint, got %d", resp.StatusCode)
	}
	var trendsPayload struct {
		Trends []model.NodeHealthTrend `json:"trends"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&trendsPayload); err != nil {
		t.Fatalf("decode trends payload: %v", err)
	}
	if len(trendsPayload.Trends) == 0 {
		t.Fatal("expected at least one node health trend")
	}

	req, _ = http.NewRequest(http.MethodGet, ts.URL+"/v1/metrics/prometheus", nil)
	req.Header.Set("Authorization", "Bearer dev-token")
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("prometheus metrics request: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 from prometheus metrics endpoint, got %d", resp.StatusCode)
	}
	metricsBody, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read metrics body: %v", err)
	}
	if !strings.Contains(string(metricsBody), "orch_deployments_total") {
		t.Fatalf("expected prometheus output to include orch_deployments_total, got: %s", string(metricsBody))
	}
}

func TestNetworkServiceProxyRoute(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		_, _ = w.Write([]byte("proxied-ok"))
	}))
	defer backend.Close()

	backendURL, err := url.Parse(backend.URL)
	if err != nil {
		t.Fatalf("parse backend url: %v", err)
	}
	hostParts := strings.Split(backendURL.Host, ":")
	if len(hostParts) != 2 {
		t.Fatalf("unexpected backend host format: %s", backendURL.Host)
	}
	backendHost := hostParts[0]
	backendPort, err := strconv.Atoi(hostParts[1])
	if err != nil {
		t.Fatalf("parse backend port: %v", err)
	}

	dbPath := filepath.Join(t.TempDir(), "badger")
	st, err := store.NewBadgerStateStore(dbPath)
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })

	orch := service.NewOrchestrator(st, scheduler.NewPlanner("least-loaded"), 20*time.Second)
	if err := orch.RegisterNode(context.Background(), model.Node{
		ID:          "worker-1",
		Address:     backendHost,
		Allocatable: model.Resource{MilliCPU: 1000, MemoryMB: 1000},
		Capacity:    model.Resource{MilliCPU: 1000, MemoryMB: 1000},
	}); err != nil {
		t.Fatalf("register node: %v", err)
	}
	if _, err := orch.CreateDeployment(context.Background(), model.DeploymentSpec{
		Name:      "proxy-api",
		Image:     "nginx:latest",
		Replicas:  1,
		Resources: model.Resource{MilliCPU: 100, MemoryMB: 100},
		Labels:    map[string]string{"app": "proxy-api"},
	}); err != nil {
		t.Fatalf("create deployment: %v", err)
	}
	assignments, err := orch.PollAssignments(context.Background(), "worker-1", 10)
	if err != nil {
		t.Fatalf("poll assignments: %v", err)
	}
	for _, a := range assignments {
		if err := orch.ReportWorkloadStatus(context.Background(), a.WorkloadID, model.WorkloadRunning, "ctr-"+a.WorkloadID); err != nil {
			t.Fatalf("report status: %v", err)
		}
	}
	if _, err := orch.CreateService(context.Background(), model.ServiceSpec{
		Name:     "proxy-service",
		Selector: map[string]string{"app": "proxy-api"},
		Ports: []model.ServicePort{{
			Name:       "http",
			Port:       80,
			TargetPort: backendPort,
			Protocol:   model.ServiceProtocolTCP,
		}},
	}); err != nil {
		t.Fatalf("create service: %v", err)
	}
	if err := orch.ReconcileAll(context.Background()); err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	srv := NewHTTPServer(orch, auth.NewVerifier("static", "dev-token", "", "", "", ""))
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(ts.Close)

	req, err := http.NewRequest(http.MethodGet, ts.URL+"/v1/network/services/proxy-service/proxy", nil)
	if err != nil {
		t.Fatalf("new proxy request: %v", err)
	}
	req.Header.Set("Authorization", "Bearer dev-token")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("proxy request: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected proxy status 200, got %d", resp.StatusCode)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read proxy body: %v", err)
	}
	if string(body) != "proxied-ok" {
		t.Fatalf("unexpected proxy body: %s", string(body))
	}
	if got := resp.Header.Get("X-Orch-Proxied-Service"); got != "proxy-service" {
		t.Fatalf("unexpected proxied service header: %s", got)
	}
}

func TestNamespaceQuotaAndSecretsEndpoints(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "badger")
	st, err := store.NewBadgerStateStore(dbPath)
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })

	orch := service.NewOrchestrator(st, scheduler.NewPlanner("least-loaded"), 20*time.Second)
	if err := orch.RegisterNode(context.Background(), model.Node{ID: "worker-1", Address: "10.1.0.8", Allocatable: model.Resource{MilliCPU: 1000, MemoryMB: 1000}, Capacity: model.Resource{MilliCPU: 1000, MemoryMB: 1000}}); err != nil {
		t.Fatalf("register node: %v", err)
	}

	srv := NewHTTPServer(orch, auth.NewVerifier("static", "dev-token", "", "", "", ""))
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(ts.Close)

	nsBody, _ := json.Marshal(map[string]any{"namespace": map[string]any{"name": "team-a", "tenant": "tenant-1"}})
	req, _ := http.NewRequest(http.MethodPost, ts.URL+"/v1/namespaces", bytes.NewReader(nsBody))
	req.Header.Set("Authorization", "Bearer dev-token")
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("create namespace request: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("expected namespace create 201, got %d", resp.StatusCode)
	}

	quotaBody, _ := json.Marshal(map[string]any{"quota": map[string]any{"namespace": "team-a", "maxDeployments": 1, "maxMilliCPU": 500, "maxMemoryMB": 512}})
	req, _ = http.NewRequest(http.MethodPost, ts.URL+"/v1/quotas", bytes.NewReader(quotaBody))
	req.Header.Set("Authorization", "Bearer dev-token")
	req.Header.Set("Content-Type", "application/json")
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("upsert quota request: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("expected quota create 201, got %d", resp.StatusCode)
	}

	dep1, _ := json.Marshal(map[string]any{"spec": map[string]any{"namespace": "team-a", "name": "q1", "image": "nginx", "replicas": 1, "resources": map[string]any{"milliCPU": 200, "memoryMB": 128}}})
	req, _ = http.NewRequest(http.MethodPost, ts.URL+"/v1/deployments", bytes.NewReader(dep1))
	req.Header.Set("Authorization", "Bearer dev-token")
	req.Header.Set("Content-Type", "application/json")
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("create deployment one: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("expected first deployment create 201, got %d", resp.StatusCode)
	}

	dep2, _ := json.Marshal(map[string]any{"spec": map[string]any{"namespace": "team-a", "name": "q2", "image": "nginx", "replicas": 1, "resources": map[string]any{"milliCPU": 200, "memoryMB": 128}}})
	req, _ = http.NewRequest(http.MethodPost, ts.URL+"/v1/deployments", bytes.NewReader(dep2))
	req.Header.Set("Authorization", "Bearer dev-token")
	req.Header.Set("Content-Type", "application/json")
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("create deployment two: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected quota rejection 400, got %d", resp.StatusCode)
	}

	secretBody, _ := json.Marshal(map[string]any{"secret": map[string]any{"namespace": "team-a", "name": "registry", "data": map[string]any{"username": "u", "password": "p"}}})
	req, _ = http.NewRequest(http.MethodPost, ts.URL+"/v1/secrets", bytes.NewReader(secretBody))
	req.Header.Set("Authorization", "Bearer dev-token")
	req.Header.Set("Content-Type", "application/json")
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("create secret: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("expected secret create 201, got %d", resp.StatusCode)
	}

	req, _ = http.NewRequest(http.MethodGet, ts.URL+"/v1/secrets?namespace=team-a", nil)
	req.Header.Set("Authorization", "Bearer dev-token")
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("list secrets: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected secret list 200, got %d", resp.StatusCode)
	}
}

func testHTTPServer(t *testing.T, verifier *auth.Verifier) *httptest.Server {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "badger")
	st, err := store.NewBadgerStateStore(dbPath)
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })

	orch := service.NewOrchestrator(st, scheduler.NewPlanner("least-loaded"), 20*time.Second)
	srv := NewHTTPServer(orch, verifier)
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(ts.Close)
	return ts
}

func signedToken(t *testing.T, secret string, roles []string) string {
	t.Helper()
	tok := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{
		"sub":   "u-1",
		"roles": roles,
	})
	s, err := tok.SignedString([]byte(secret))
	if err != nil {
		t.Fatalf("sign jwt: %v", err)
	}
	return s
}
