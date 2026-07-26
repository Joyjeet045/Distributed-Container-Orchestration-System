package api

import (
	"context"
	"net"
	"path/filepath"
	"testing"
	"time"

	grpcpb "minikube-orchestrator/api/proto"
	"minikube-orchestrator/internal/model"
	"minikube-orchestrator/internal/scheduler"
	"minikube-orchestrator/internal/service"
	"minikube-orchestrator/internal/store"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

func TestGRPCCreateAndListDeployments(t *testing.T) {
	t.Parallel()

	dbPath := filepath.Join(t.TempDir(), "badger")
	st, err := store.NewBadgerStateStore(dbPath)
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })

	orch := service.NewOrchestrator(st, scheduler.NewPlanner("least-loaded"), 20*time.Second)
	if err := orch.RegisterNode(context.Background(), model.Node{
		ID:          "grpc-worker",
		Address:     "grpc-worker",
		Capacity:    model.Resource{MilliCPU: 2000, MemoryMB: 2048},
		Allocatable: model.Resource{MilliCPU: 2000, MemoryMB: 2048},
		Labels:      map[string]string{"zone": "a"},
	}); err != nil {
		t.Fatalf("register node: %v", err)
	}

	srv := NewGRPCServer(orch)
	listener := bufconn.Listen(1024 * 1024)
	grpcSrv := grpc.NewServer()
	grpcpb.RegisterOrchestratorServiceServer(grpcSrv, srv)
	go func() {
		_ = grpcSrv.Serve(listener)
	}()
	t.Cleanup(grpcSrv.Stop)

	dialer := func(context.Context, string) (net.Conn, error) {
		return listener.Dial()
	}

	conn, err := grpc.DialContext(
		context.Background(),
		"bufnet",
		grpc.WithContextDialer(dialer),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("dial grpc: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	client := grpcpb.NewOrchestratorServiceClient(conn)

	createReq := &grpcpb.CreateDeploymentRequest{
		Spec: &grpcpb.DeploymentSpec{
			Name:     "grpc-app",
			Image:    "nginx:latest",
			Replicas: 1,
			Resources: &grpcpb.ResourceRequest{
				MilliCpu: 250,
				MemoryMb: 128,
			},
		},
	}
	createResp, err := client.CreateDeployment(context.Background(), createReq)
	if err != nil {
		t.Fatalf("create deployment invoke: %v", err)
	}
	if createResp.GetDeployment().GetId() == "" {
		t.Fatal("expected deployment id")
	}

	listResp, err := client.ListDeployments(context.Background(), &grpcpb.ListDeploymentsRequest{})
	if err != nil {
		t.Fatalf("list deployments invoke: %v", err)
	}
	if len(listResp.GetDeployments()) != 1 {
		t.Fatalf("expected 1 deployment, got %d", len(listResp.GetDeployments()))
	}

	stateResp, err := client.GetClusterState(context.Background(), &grpcpb.ClusterStateRequest{})
	if err != nil {
		t.Fatalf("cluster state invoke: %v", err)
	}
	if len(stateResp.GetNodes()) != 1 {
		t.Fatalf("expected 1 node in state, got %d", len(stateResp.GetNodes()))
	}
	if stateResp.GetNodes()[0].GetId() != "grpc-worker" {
		t.Fatalf("unexpected node id: %s", stateResp.GetNodes()[0].GetId())
	}
}
