package service

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"minikube-orchestrator/internal/model"
	"minikube-orchestrator/internal/scheduler"
	"minikube-orchestrator/internal/store"
)

func TestPriorityPreemptionAndEvents(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "badger")
	st, err := store.NewBadgerStateStore(dbPath)
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })

	orch := NewOrchestrator(st, scheduler.NewPlanner("least-loaded"), 20*time.Second)
	if err := orch.RegisterNode(context.Background(), model.Node{
		ID:          "node-1",
		Address:     "node-1",
		Allocatable: model.Resource{MilliCPU: 1000, MemoryMB: 1000},
		Capacity:    model.Resource{MilliCPU: 1000, MemoryMB: 1000},
	}); err != nil {
		t.Fatalf("register node: %v", err)
	}

	low, err := orch.CreateDeployment(context.Background(), model.DeploymentSpec{
		Name:      "low",
		Image:     "nginx:latest",
		Replicas:  1,
		Priority:  1,
		Resources: model.Resource{MilliCPU: 800, MemoryMB: 100},
	})
	if err != nil {
		t.Fatalf("create low priority deployment: %v", err)
	}

	high, err := orch.CreateDeployment(context.Background(), model.DeploymentSpec{
		Name:      "high",
		Image:     "nginx:latest",
		Replicas:  1,
		Priority:  10,
		Resources: model.Resource{MilliCPU: 600, MemoryMB: 100},
	})
	if err != nil {
		t.Fatalf("create high priority deployment: %v", err)
	}

	lowWorkloads, err := st.ListWorkloadsByDeployment(low.ID)
	if err != nil {
		t.Fatalf("list low workloads: %v", err)
	}
	highWorkloads, err := st.ListWorkloadsByDeployment(high.ID)
	if err != nil {
		t.Fatalf("list high workloads: %v", err)
	}
	if len(lowWorkloads) != 0 {
		t.Fatalf("expected low priority workload preempted, got %d", len(lowWorkloads))
	}
	if len(highWorkloads) != 1 {
		t.Fatalf("expected high priority workload present, got %d", len(highWorkloads))
	}

	events, err := orch.ListEvents(context.Background(), 200)
	if err != nil {
		t.Fatalf("list events: %v", err)
	}
	foundPreempt := false
	foundScheduler := false
	for _, evt := range events {
		if evt.Reason == "PreemptedLowerPriorityWorkload" {
			foundPreempt = true
		}
		if evt.Reason == "NodeSelected" {
			foundScheduler = true
		}
	}
	if !foundPreempt {
		t.Fatal("expected preemption event")
	}
	if !foundScheduler {
		t.Fatal("expected scheduler selection event")
	}
}

func TestScaleDownReconcileAndConditions(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "badger")
	st, err := store.NewBadgerStateStore(dbPath)
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })

	orch := NewOrchestrator(st, scheduler.NewPlanner("least-loaded"), 20*time.Second)
	if err := orch.RegisterNode(context.Background(), model.Node{
		ID:          "node-1",
		Address:     "node-1",
		Allocatable: model.Resource{MilliCPU: 2000, MemoryMB: 2000},
		Capacity:    model.Resource{MilliCPU: 2000, MemoryMB: 2000},
	}); err != nil {
		t.Fatalf("register node: %v", err)
	}

	dep, err := orch.CreateDeployment(context.Background(), model.DeploymentSpec{
		Name:      "scale-test",
		Image:     "nginx:latest",
		Replicas:  3,
		Priority:  5,
		Resources: model.Resource{MilliCPU: 100, MemoryMB: 100},
	})
	if err != nil {
		t.Fatalf("create deployment: %v", err)
	}

	storedDep, err := st.GetDeployment(dep.ID)
	if err != nil {
		t.Fatalf("get deployment: %v", err)
	}
	storedDep.Spec.Replicas = 1
	if err := st.UpdateDeployment(storedDep); err != nil {
		t.Fatalf("update deployment replicas: %v", err)
	}

	if err := orch.ReconcileAll(context.Background()); err != nil {
		t.Fatalf("reconcile all: %v", err)
	}

	for {
		assignments, err := orch.PollAssignments(context.Background(), "node-1", 10)
		if err != nil {
			t.Fatalf("poll assignments: %v", err)
		}
		if len(assignments) == 0 {
			break
		}
		for _, a := range assignments {
			if a.Action == "delete" {
				if err := orch.ReportWorkloadStatus(context.Background(), a.WorkloadID, model.WorkloadTerminated, ""); err != nil {
					t.Fatalf("report terminated: %v", err)
				}
			}
		}
	}

	workloads, err := st.ListWorkloadsByDeployment(dep.ID)
	if err != nil {
		t.Fatalf("list workloads: %v", err)
	}
	if len(workloads) != 1 {
		t.Fatalf("expected scale down to 1 workload, got %d", len(workloads))
	}

	updatedDep, err := st.GetDeployment(dep.ID)
	if err != nil {
		t.Fatalf("get updated deployment: %v", err)
	}
	if !hasCondition(updatedDep.Conditions, model.ConditionAvailable, true) {
		t.Fatal("expected Available=true condition")
	}
}

func TestDeletionFinalizerLifecycle(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "badger")
	st, err := store.NewBadgerStateStore(dbPath)
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })

	orch := NewOrchestrator(st, scheduler.NewPlanner("least-loaded"), 20*time.Second)
	if err := orch.RegisterNode(context.Background(), model.Node{
		ID:          "node-1",
		Address:     "node-1",
		Allocatable: model.Resource{MilliCPU: 1000, MemoryMB: 1000},
		Capacity:    model.Resource{MilliCPU: 1000, MemoryMB: 1000},
	}); err != nil {
		t.Fatalf("register node: %v", err)
	}

	dep, err := orch.CreateDeployment(context.Background(), model.DeploymentSpec{
		Name:      "delete-me",
		Image:     "nginx:latest",
		Replicas:  1,
		Resources: model.Resource{MilliCPU: 100, MemoryMB: 100},
	})
	if err != nil {
		t.Fatalf("create deployment: %v", err)
	}
	if err := orch.DeleteDeployment(context.Background(), dep.ID); err != nil {
		t.Fatalf("mark deployment for delete: %v", err)
	}
	if err := orch.ReconcileAll(context.Background()); err != nil {
		t.Fatalf("reconcile deletion: %v", err)
	}

	assignments, err := orch.PollAssignments(context.Background(), "node-1", 10)
	if err != nil {
		t.Fatalf("poll assignments: %v", err)
	}
	for _, a := range assignments {
		if a.Action == "delete" {
			if err := orch.ReportWorkloadStatus(context.Background(), a.WorkloadID, model.WorkloadTerminated, ""); err != nil {
				t.Fatalf("report terminated: %v", err)
			}
		}
	}
	if err := orch.ReconcileAll(context.Background()); err != nil {
		t.Fatalf("reconcile deletion completion: %v", err)
	}

	if _, err := st.GetDeployment(dep.ID); err == nil {
		t.Fatal("expected deployment deleted after finalizer completion")
	}
}

func TestEventSubscriptionStream(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "badger")
	st, err := store.NewBadgerStateStore(dbPath)
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })

	orch := NewOrchestrator(st, scheduler.NewPlanner("least-loaded"), 20*time.Second)
	if err := orch.RegisterNode(context.Background(), model.Node{
		ID:          "node-1",
		Address:     "node-1",
		Allocatable: model.Resource{MilliCPU: 1000, MemoryMB: 1000},
		Capacity:    model.Resource{MilliCPU: 1000, MemoryMB: 1000},
	}); err != nil {
		t.Fatalf("register node: %v", err)
	}

	ch, unsub := orch.SubscribeEvents(8)
	defer unsub()

	if _, err := orch.CreateDeployment(context.Background(), model.DeploymentSpec{
		Name:      "evt",
		Image:     "nginx:latest",
		Replicas:  1,
		Resources: model.Resource{MilliCPU: 100, MemoryMB: 100},
	}); err != nil {
		t.Fatalf("create deployment: %v", err)
	}

	select {
	case <-time.After(2 * time.Second):
		t.Fatal("expected streamed event but timed out")
	case evt := <-ch:
		if evt.Type == "" {
			t.Fatal("expected non-empty event type")
		}
	}
}

func TestServiceEndpointReconcileAndResolve(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "badger")
	st, err := store.NewBadgerStateStore(dbPath)
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })

	orch := NewOrchestrator(st, scheduler.NewPlanner("least-loaded"), 20*time.Second)
	if err := orch.RegisterNode(context.Background(), model.Node{
		ID:          "node-1",
		Address:     "10.0.0.1",
		Allocatable: model.Resource{MilliCPU: 2000, MemoryMB: 2000},
		Capacity:    model.Resource{MilliCPU: 2000, MemoryMB: 2000},
	}); err != nil {
		t.Fatalf("register node: %v", err)
	}

	dep, err := orch.CreateDeployment(context.Background(), model.DeploymentSpec{
		Name:      "svc-web",
		Image:     "nginx:latest",
		Replicas:  1,
		Resources: model.Resource{MilliCPU: 100, MemoryMB: 100},
		Labels:    map[string]string{"app": "web"},
	})
	if err != nil {
		t.Fatalf("create deployment: %v", err)
	}

	assignments, err := orch.PollAssignments(context.Background(), "node-1", 10)
	if err != nil {
		t.Fatalf("poll assignments: %v", err)
	}
	if len(assignments) == 0 {
		t.Fatal("expected assignment")
	}
	for _, a := range assignments {
		if err := orch.ReportWorkloadStatus(context.Background(), a.WorkloadID, model.WorkloadRunning, "ctr-"+a.WorkloadID); err != nil {
			t.Fatalf("report running: %v", err)
		}
	}

	svc, err := orch.CreateService(context.Background(), model.ServiceSpec{
		Name:     "web",
		Selector: map[string]string{"app": "web"},
		Ports: []model.ServicePort{
			{Name: "http", Port: 80, TargetPort: 8080, Protocol: model.ServiceProtocolTCP},
		},
	})
	if err != nil {
		t.Fatalf("create service: %v", err)
	}
	if err := orch.ReconcileAll(context.Background()); err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	endpoints, err := orch.ListServiceEndpoints(context.Background(), svc.ID)
	if err != nil {
		t.Fatalf("list endpoints: %v", err)
	}
	if len(endpoints) != 1 {
		t.Fatalf("expected 1 endpoint, got %d", len(endpoints))
	}
	if endpoints[0].Address != "10.0.0.1" {
		t.Fatalf("expected endpoint address 10.0.0.1, got %s", endpoints[0].Address)
	}
	if !endpoints[0].Ready {
		t.Fatal("expected endpoint ready")
	}

	dnsName, resolved, err := orch.ResolveServiceName(context.Background(), "web")
	if err != nil {
		t.Fatalf("resolve service: %v", err)
	}
	if dnsName != "web.default.svc.cluster.local" {
		t.Fatalf("unexpected dns name: %s", dnsName)
	}
	if len(resolved) != 1 {
		t.Fatalf("expected 1 resolved endpoint, got %d", len(resolved))
	}

	target, err := orch.SelectServiceEndpoint(context.Background(), svc.ID, "round-robin")
	if err != nil {
		t.Fatalf("select target: %v", err)
	}
	if target.ServiceID != svc.ID {
		t.Fatalf("unexpected target service id: %s", target.ServiceID)
	}
	orch.ReleaseServiceEndpoint(context.Background(), target.ID)

	depState, err := st.GetDeployment(dep.ID)
	if err != nil {
		t.Fatalf("get deployment: %v", err)
	}
	if depState.Status != model.DeploymentRunning {
		t.Fatalf("unexpected deployment status: %s", depState.Status)
	}
}

func TestServiceEndpointSelectionSkipsUnhealthyNodes(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "badger")
	st, err := store.NewBadgerStateStore(dbPath)
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })

	orch := NewOrchestrator(st, scheduler.NewPlanner("least-loaded"), 20*time.Second)
	if err := orch.RegisterNode(context.Background(), model.Node{
		ID:          "node-1",
		Address:     "10.0.0.1",
		Allocatable: model.Resource{MilliCPU: 2000, MemoryMB: 2000},
		Capacity:    model.Resource{MilliCPU: 2000, MemoryMB: 2000},
	}); err != nil {
		t.Fatalf("register node-1: %v", err)
	}
	if err := orch.RegisterNode(context.Background(), model.Node{
		ID:          "node-2",
		Address:     "10.0.0.2",
		Allocatable: model.Resource{MilliCPU: 2000, MemoryMB: 2000},
		Capacity:    model.Resource{MilliCPU: 2000, MemoryMB: 2000},
	}); err != nil {
		t.Fatalf("register node-2: %v", err)
	}

	dep, err := orch.CreateDeployment(context.Background(), model.DeploymentSpec{
		Name:      "svc-multi",
		Image:     "nginx:latest",
		Replicas:  2,
		Resources: model.Resource{MilliCPU: 100, MemoryMB: 100},
		Labels:    map[string]string{"app": "multi"},
	})
	if err != nil {
		t.Fatalf("create deployment: %v", err)
	}
	assignments1, err := orch.PollAssignments(context.Background(), "node-1", 10)
	if err != nil {
		t.Fatalf("poll node-1 assignments: %v", err)
	}
	assignments2, err := orch.PollAssignments(context.Background(), "node-2", 10)
	if err != nil {
		t.Fatalf("poll node-2 assignments: %v", err)
	}
	allAssignments := append(assignments1, assignments2...)
	if len(allAssignments) != 2 {
		t.Fatalf("expected 2 assignments, got %d", len(allAssignments))
	}
	for _, a := range allAssignments {
		if err := orch.ReportWorkloadStatus(context.Background(), a.WorkloadID, model.WorkloadRunning, "ctr-"+a.WorkloadID); err != nil {
			t.Fatalf("report running: %v", err)
		}
	}

	svc, err := orch.CreateService(context.Background(), model.ServiceSpec{
		Name:     "multi",
		Selector: map[string]string{"app": "multi"},
		Ports: []model.ServicePort{
			{Name: "http", Port: 80, TargetPort: 8080, Protocol: model.ServiceProtocolTCP},
		},
	})
	if err != nil {
		t.Fatalf("create service: %v", err)
	}
	if err := orch.ReconcileAll(context.Background()); err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	workloads, err := st.ListWorkloadsByDeployment(dep.ID)
	if err != nil {
		t.Fatalf("list workloads: %v", err)
	}
	nodeToKeep := workloads[0].NodeID
	nodeToMarkDown := workloads[1].NodeID
	node, err := st.GetNode(nodeToMarkDown)
	if err != nil {
		t.Fatalf("get node to mark down: %v", err)
	}
	node.Status = model.NodeDown
	if err := st.UpsertNode(node); err != nil {
		t.Fatalf("update node status: %v", err)
	}

	target, err := orch.SelectServiceEndpoint(context.Background(), svc.ID, "round-robin")
	if err != nil {
		t.Fatalf("select endpoint: %v", err)
	}
	if target.NodeID != nodeToKeep {
		t.Fatalf("expected endpoint on healthy node %s, got %s", nodeToKeep, target.NodeID)
	}
}

func TestProbeDrivenEndpointReadinessAndLiveness(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "badger")
	st, err := store.NewBadgerStateStore(dbPath)
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })

	orch := NewOrchestrator(st, scheduler.NewPlanner("least-loaded"), 20*time.Second)
	if err := orch.RegisterNode(context.Background(), model.Node{
		ID:          "node-1",
		Address:     "10.0.0.1",
		Allocatable: model.Resource{MilliCPU: 2000, MemoryMB: 2000},
		Capacity:    model.Resource{MilliCPU: 2000, MemoryMB: 2000},
	}); err != nil {
		t.Fatalf("register node: %v", err)
	}

	dep, err := orch.CreateDeployment(context.Background(), model.DeploymentSpec{
		Name:      "probe-dep",
		Image:     "nginx:latest",
		Replicas:  1,
		Resources: model.Resource{MilliCPU: 100, MemoryMB: 100},
		Labels:    map[string]string{"app": "probe"},
		LivenessProbe: model.ProbeSpec{
			Enabled:             true,
			InitialDelaySeconds: 0,
		},
		ReadinessProbe: model.ProbeSpec{
			Enabled:             true,
			InitialDelaySeconds: 3600,
		},
	})
	if err != nil {
		t.Fatalf("create deployment: %v", err)
	}

	assignments, err := orch.PollAssignments(context.Background(), "node-1", 10)
	if err != nil {
		t.Fatalf("poll assignments: %v", err)
	}
	if len(assignments) != 1 {
		t.Fatalf("expected 1 assignment, got %d", len(assignments))
	}
	if err := orch.ReportWorkloadStatus(context.Background(), assignments[0].WorkloadID, model.WorkloadRunning, ""); err != nil {
		t.Fatalf("report running with empty container id: %v", err)
	}

	svc, err := orch.CreateService(context.Background(), model.ServiceSpec{
		Name:     "probe-svc",
		Selector: map[string]string{"app": "probe"},
		Ports: []model.ServicePort{
			{Name: "http", Port: 80, TargetPort: 8080, Protocol: model.ServiceProtocolTCP},
		},
	})
	if err != nil {
		t.Fatalf("create service: %v", err)
	}

	if err := orch.ReconcileAll(context.Background()); err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	workloads, err := st.ListWorkloadsByDeployment(dep.ID)
	if err != nil {
		t.Fatalf("list workloads: %v", err)
	}
	if len(workloads) != 1 {
		t.Fatalf("expected 1 workload, got %d", len(workloads))
	}
	if workloads[0].Status != model.WorkloadFailed {
		t.Fatalf("expected liveness failure -> workload failed, got %s", workloads[0].Status)
	}

	endpoints, err := orch.ListServiceEndpoints(context.Background(), svc.ID)
	if err != nil {
		t.Fatalf("list endpoints: %v", err)
	}
	if len(endpoints) != 0 {
		t.Fatalf("expected no endpoints from failed workload, got %d", len(endpoints))
	}
}

func TestNodeCriticalHealthTriggersDrainRemediation(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "badger")
	st, err := store.NewBadgerStateStore(dbPath)
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })

	orch := NewOrchestrator(st, scheduler.NewPlanner("least-loaded"), 5*time.Second)
	if err := orch.RegisterNode(context.Background(), model.Node{
		ID:          "node-1",
		Address:     "10.0.0.1",
		Allocatable: model.Resource{MilliCPU: 2000, MemoryMB: 2000},
		Capacity:    model.Resource{MilliCPU: 2000, MemoryMB: 2000},
	}); err != nil {
		t.Fatalf("register node: %v", err)
	}

	dep, err := orch.CreateDeployment(context.Background(), model.DeploymentSpec{
		Name:      "remediate-dep",
		Image:     "nginx:latest",
		Replicas:  1,
		Resources: model.Resource{MilliCPU: 100, MemoryMB: 100},
	})
	if err != nil {
		t.Fatalf("create deployment: %v", err)
	}
	assignments, err := orch.PollAssignments(context.Background(), "node-1", 10)
	if err != nil {
		t.Fatalf("poll assignments: %v", err)
	}
	if len(assignments) != 1 {
		t.Fatalf("expected one assignment, got %d", len(assignments))
	}
	if err := orch.ReportWorkloadStatus(context.Background(), assignments[0].WorkloadID, model.WorkloadRunning, "ctr-1"); err != nil {
		t.Fatalf("report running: %v", err)
	}

	node, err := st.GetNode("node-1")
	if err != nil {
		t.Fatalf("get node: %v", err)
	}
	node.LastSeen = time.Now().UTC().Add(-20 * time.Second)
	if err := st.UpsertNode(node); err != nil {
		t.Fatalf("update node last seen: %v", err)
	}

	if err := orch.ReconcileAll(context.Background()); err != nil {
		t.Fatalf("reconcile all: %v", err)
	}

	updatedNode, err := st.GetNode("node-1")
	if err != nil {
		t.Fatalf("get updated node: %v", err)
	}
	if updatedNode.Status != model.NodeDown {
		t.Fatalf("expected node down, got %s", updatedNode.Status)
	}
	if updatedNode.Health.FailureClass != model.NodeFailureCritical {
		t.Fatalf("expected critical failure class, got %s", updatedNode.Health.FailureClass)
	}
	if !updatedNode.Health.Isolated || !updatedNode.Health.Draining {
		t.Fatalf("expected isolated+draining true, got isolated=%v draining=%v", updatedNode.Health.Isolated, updatedNode.Health.Draining)
	}

	workloads, err := st.ListWorkloadsByDeployment(dep.ID)
	if err != nil {
		t.Fatalf("list workloads: %v", err)
	}
	if len(workloads) != 1 {
		t.Fatalf("expected one workload, got %d", len(workloads))
	}
	if workloads[0].Status != model.WorkloadTerminating {
		t.Fatalf("expected workload terminating, got %s", workloads[0].Status)
	}

	deleteAssignments, err := orch.PollAssignments(context.Background(), "node-1", 10)
	if err != nil {
		t.Fatalf("poll delete assignments: %v", err)
	}
	foundDelete := false
	for _, a := range deleteAssignments {
		if a.Action == "delete" && a.WorkloadID == workloads[0].ID {
			foundDelete = true
			break
		}
	}
	if !foundDelete {
		t.Fatal("expected delete assignment from remediation")
	}
}

func hasCondition(conditions []model.DeploymentCondition, typ model.DeploymentConditionType, status bool) bool {
	for _, c := range conditions {
		if c.Type == typ && c.Status == status {
			return true
		}
	}
	return false
}

func TestRollingUpdateHistoryAndProgress(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "badger")
	st, err := store.NewBadgerStateStore(dbPath)
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })

	orch := NewOrchestrator(st, scheduler.NewPlanner("least-loaded"), 20*time.Second)
	if err := orch.RegisterNode(context.Background(), model.Node{ID: "node-1", Address: "10.0.0.1", Allocatable: model.Resource{MilliCPU: 4000, MemoryMB: 4000}, Capacity: model.Resource{MilliCPU: 4000, MemoryMB: 4000}}); err != nil {
		t.Fatalf("register node: %v", err)
	}

	dep, err := orch.CreateDeployment(context.Background(), model.DeploymentSpec{
		Name:      "roll",
		Image:     "nginx:v1",
		Replicas:  2,
		Resources: model.Resource{MilliCPU: 100, MemoryMB: 100},
		Rollout:   model.RolloutSpec{Strategy: model.RolloutStrategyRollingUpdate, RollingUpdate: model.RollingUpdateSpec{MaxSurge: 1, MaxUnavailable: 0}},
	})
	if err != nil {
		t.Fatalf("create deployment: %v", err)
	}
	initial, _ := orch.PollAssignments(context.Background(), "node-1", 10)
	for _, a := range initial {
		_ = orch.ReportWorkloadStatus(context.Background(), a.WorkloadID, model.WorkloadRunning, "ctr-"+a.WorkloadID)
	}

	updated, err := orch.UpdateDeployment(context.Background(), dep.ID, model.DeploymentSpec{
		Name:        dep.Spec.Name,
		Image:       "nginx:v2",
		Replicas:    2,
		Resources:   dep.Spec.Resources,
		Labels:      dep.Spec.Labels,
		Tolerations: dep.Spec.Tolerations,
		Rollout:     model.RolloutSpec{Strategy: model.RolloutStrategyRollingUpdate, RollingUpdate: model.RollingUpdateSpec{MaxSurge: 1, MaxUnavailable: 0}},
	})
	if err != nil {
		t.Fatalf("update deployment: %v", err)
	}
	if updated.CurrentRevision != 2 {
		t.Fatalf("expected current revision 2, got %d", updated.CurrentRevision)
	}

	for i := 0; i < 6; i++ {
		if err := orch.ReconcileAll(context.Background()); err != nil {
			t.Fatalf("reconcile: %v", err)
		}
		assignments, _ := orch.PollAssignments(context.Background(), "node-1", 20)
		for _, a := range assignments {
			if a.Action == "create" {
				_ = orch.ReportWorkloadStatus(context.Background(), a.WorkloadID, model.WorkloadRunning, "ctr-"+a.WorkloadID)
			}
			if a.Action == "delete" {
				_ = orch.ReportWorkloadStatus(context.Background(), a.WorkloadID, model.WorkloadTerminated, "")
			}
		}
	}

	finalDep, err := st.GetDeployment(dep.ID)
	if err != nil {
		t.Fatalf("get deployment: %v", err)
	}
	if finalDep.RolloutStatus.Phase != "Stable" {
		t.Fatalf("expected stable rollout phase, got %s", finalDep.RolloutStatus.Phase)
	}
	if len(finalDep.RevisionHistory) < 2 {
		t.Fatalf("expected revision history >=2, got %d", len(finalDep.RevisionHistory))
	}
	workloads, _ := st.ListWorkloadsByDeployment(dep.ID)
	for _, w := range workloads {
		if w.Version != finalDep.CurrentRevision {
			t.Fatalf("expected only current revision workloads, got version=%d current=%d", w.Version, finalDep.CurrentRevision)
		}
	}
}

func TestRollbackAndCanaryStrategy(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "badger")
	st, err := store.NewBadgerStateStore(dbPath)
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })

	orch := NewOrchestrator(st, scheduler.NewPlanner("least-loaded"), 20*time.Second)
	if err := orch.RegisterNode(context.Background(), model.Node{ID: "node-1", Address: "10.0.0.1", Allocatable: model.Resource{MilliCPU: 4000, MemoryMB: 4000}, Capacity: model.Resource{MilliCPU: 4000, MemoryMB: 4000}}); err != nil {
		t.Fatalf("register node: %v", err)
	}

	dep, err := orch.CreateDeployment(context.Background(), model.DeploymentSpec{Name: "canary", Image: "nginx:v1", Replicas: 4, Resources: model.Resource{MilliCPU: 100, MemoryMB: 100}})
	if err != nil {
		t.Fatalf("create deployment: %v", err)
	}
	initial, _ := orch.PollAssignments(context.Background(), "node-1", 20)
	for _, a := range initial {
		_ = orch.ReportWorkloadStatus(context.Background(), a.WorkloadID, model.WorkloadRunning, "ctr-"+a.WorkloadID)
	}

	_, err = orch.UpdateDeployment(context.Background(), dep.ID, model.DeploymentSpec{
		Name:      dep.Spec.Name,
		Image:     "nginx:v2",
		Replicas:  4,
		Resources: dep.Spec.Resources,
		Rollout: model.RolloutSpec{
			Strategy:      model.RolloutStrategyCanary,
			Canary:        model.CanarySpec{Enabled: true, Percentage: 25},
			RollingUpdate: model.RollingUpdateSpec{MaxSurge: 1, MaxUnavailable: 0},
		},
	})
	if err != nil {
		t.Fatalf("update canary deployment: %v", err)
	}
	if err := orch.ReconcileAll(context.Background()); err != nil {
		t.Fatalf("reconcile canary: %v", err)
	}
	midDep, _ := st.GetDeployment(dep.ID)
	if midDep.RolloutStatus.Phase != "Canary" && midDep.RolloutStatus.Phase != "CanaryPromoting" {
		t.Fatalf("expected canary phase, got %s", midDep.RolloutStatus.Phase)
	}

	rolledBack, err := orch.RollbackDeployment(context.Background(), dep.ID, 1)
	if err != nil {
		t.Fatalf("rollback deployment: %v", err)
	}
	if rolledBack.Spec.Image != "nginx:v1" {
		t.Fatalf("expected rollback image nginx:v1, got %s", rolledBack.Spec.Image)
	}
}

func TestAutoscalingScaleUpAndCooldown(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "badger")
	st, err := store.NewBadgerStateStore(dbPath)
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })

	orch := NewOrchestrator(st, scheduler.NewPlanner("least-loaded"), 20*time.Second)
	if err := orch.RegisterNode(context.Background(), model.Node{ID: "node-1", Address: "10.0.0.1", Allocatable: model.Resource{MilliCPU: 4000, MemoryMB: 4000}, Capacity: model.Resource{MilliCPU: 4000, MemoryMB: 4000}}); err != nil {
		t.Fatalf("register node: %v", err)
	}

	dep, err := orch.CreateDeployment(context.Background(), model.DeploymentSpec{Name: "autoscale", Image: "nginx:v1", Replicas: 1, Resources: model.Resource{MilliCPU: 100, MemoryMB: 100}})
	if err != nil {
		t.Fatalf("create deployment: %v", err)
	}

	_, err = orch.UpsertAutoscalerPolicy(context.Background(), model.AutoscalerPolicy{
		DeploymentID:           dep.ID,
		MinReplicas:            1,
		MaxReplicas:            4,
		TargetCPUUtilization:   0.5,
		StabilizationWindowSec: 10,
		ScaleUpCooldownSec:     60,
		ScaleDownCooldownSec:   60,
	})
	if err != nil {
		t.Fatalf("upsert autoscaler policy: %v", err)
	}

	if err := orch.IngestDeploymentMetric(context.Background(), model.DeploymentMetricSample{DeploymentID: dep.ID, CPUUsage: 0.9, MemoryUsage: 0.3}); err != nil {
		t.Fatalf("ingest metric: %v", err)
	}
	if err := orch.ReconcileAll(context.Background()); err != nil {
		t.Fatalf("reconcile all: %v", err)
	}

	afterScaleUp, err := st.GetDeployment(dep.ID)
	if err != nil {
		t.Fatalf("get deployment after scale up: %v", err)
	}
	if afterScaleUp.Spec.Replicas <= 1 {
		t.Fatalf("expected autoscaler to increase replicas, got %d", afterScaleUp.Spec.Replicas)
	}

	if err := orch.IngestDeploymentMetric(context.Background(), model.DeploymentMetricSample{DeploymentID: dep.ID, CPUUsage: 0.95, MemoryUsage: 0.3}); err != nil {
		t.Fatalf("ingest second metric: %v", err)
	}
	if err := orch.ReconcileAll(context.Background()); err != nil {
		t.Fatalf("second reconcile all: %v", err)
	}

	afterCooldown, err := st.GetDeployment(dep.ID)
	if err != nil {
		t.Fatalf("get deployment after cooldown check: %v", err)
	}
	if afterCooldown.Spec.Replicas != afterScaleUp.Spec.Replicas {
		t.Fatalf("expected cooldown to block immediate second scaling, got %d -> %d", afterScaleUp.Spec.Replicas, afterCooldown.Spec.Replicas)
	}
}

func TestPredictiveAutoscalingScalesFromTrend(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "badger")
	st, err := store.NewBadgerStateStore(dbPath)
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })

	orch := NewOrchestrator(st, scheduler.NewPlanner("least-loaded"), 20*time.Second)
	if err := orch.RegisterNode(context.Background(), model.Node{ID: "node-1", Address: "10.0.0.1", Allocatable: model.Resource{MilliCPU: 4000, MemoryMB: 4000}, Capacity: model.Resource{MilliCPU: 4000, MemoryMB: 4000}}); err != nil {
		t.Fatalf("register node: %v", err)
	}
	dep, err := orch.CreateDeployment(context.Background(), model.DeploymentSpec{Name: "predictive", Image: "nginx:v1", Replicas: 1, Resources: model.Resource{MilliCPU: 100, MemoryMB: 100}})
	if err != nil {
		t.Fatalf("create deployment: %v", err)
	}
	_, err = orch.UpsertAutoscalerPolicy(context.Background(), model.AutoscalerPolicy{
		DeploymentID:              dep.ID,
		MinReplicas:               1,
		MaxReplicas:               4,
		TargetCPUUtilization:      0.7,
		PredictiveScalingEnabled:  true,
		PredictiveLookbackSamples: 3,
		PredictiveScaleFactor:     8,
		ScaleUpCooldownSec:        1,
		ScaleDownCooldownSec:      1,
	})
	if err != nil {
		t.Fatalf("upsert autoscaler policy: %v", err)
	}
	_ = orch.IngestDeploymentMetric(context.Background(), model.DeploymentMetricSample{DeploymentID: dep.ID, CPUUsage: 0.10, MemoryUsage: 0.30})
	_ = orch.IngestDeploymentMetric(context.Background(), model.DeploymentMetricSample{DeploymentID: dep.ID, CPUUsage: 0.30, MemoryUsage: 0.30})
	_ = orch.IngestDeploymentMetric(context.Background(), model.DeploymentMetricSample{DeploymentID: dep.ID, CPUUsage: 0.50, MemoryUsage: 0.30})

	if err := orch.ReconcileAll(context.Background()); err != nil {
		t.Fatalf("reconcile all: %v", err)
	}
	updated, err := st.GetDeployment(dep.ID)
	if err != nil {
		t.Fatalf("get updated deployment: %v", err)
	}
	if updated.Spec.Replicas <= 1 {
		t.Fatalf("expected predictive autoscaling to increase replicas, got %d", updated.Spec.Replicas)
	}
}

func TestAutoRollbackTriggeredByLivenessFailures(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "badger")
	st, err := store.NewBadgerStateStore(dbPath)
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })

	orch := NewOrchestrator(st, scheduler.NewPlanner("least-loaded"), 20*time.Second)
	if err := orch.RegisterNode(context.Background(), model.Node{ID: "node-1", Address: "10.0.0.1", Allocatable: model.Resource{MilliCPU: 4000, MemoryMB: 4000}, Capacity: model.Resource{MilliCPU: 4000, MemoryMB: 4000}}); err != nil {
		t.Fatalf("register node: %v", err)
	}
	dep, err := orch.CreateDeployment(context.Background(), model.DeploymentSpec{Name: "rollback-fail", Image: "nginx:v1", Replicas: 1, Resources: model.Resource{MilliCPU: 100, MemoryMB: 100}})
	if err != nil {
		t.Fatalf("create deployment: %v", err)
	}
	assignments, _ := orch.PollAssignments(context.Background(), "node-1", 20)
	for _, a := range assignments {
		_ = orch.ReportWorkloadStatus(context.Background(), a.WorkloadID, model.WorkloadRunning, "ctr-"+a.WorkloadID)
	}

	_, err = orch.UpdateDeployment(context.Background(), dep.ID, model.DeploymentSpec{
		Name:      dep.Spec.Name,
		Image:     "nginx:v2",
		Replicas:  1,
		Resources: dep.Spec.Resources,
		Rollout: model.RolloutSpec{
			Strategy:              model.RolloutStrategyRollingUpdate,
			AutoRollbackOnFailure: true,
			MaxFailedWorkloads:    1,
			RollingUpdate:         model.RollingUpdateSpec{MaxSurge: 1, MaxUnavailable: 0},
		},
		LivenessProbe: model.ProbeSpec{Enabled: true, InitialDelaySeconds: 0},
	})
	if err != nil {
		t.Fatalf("update deployment: %v", err)
	}
	if err := orch.ReconcileAll(context.Background()); err != nil {
		t.Fatalf("reconcile rollout: %v", err)
	}

	newAssignments, _ := orch.PollAssignments(context.Background(), "node-1", 20)
	for _, a := range newAssignments {
		if a.Action == "create" {
			_ = orch.ReportWorkloadStatus(context.Background(), a.WorkloadID, model.WorkloadRunning, "")
		}
	}
	if err := orch.ReconcileAll(context.Background()); err != nil {
		t.Fatalf("reconcile for probe failure and auto rollback: %v", err)
	}

	after, err := st.GetDeployment(dep.ID)
	if err != nil {
		t.Fatalf("get deployment: %v", err)
	}
	if after.Spec.Image != "nginx:v1" {
		t.Fatalf("expected auto rollback to previous image nginx:v1, got %s", after.Spec.Image)
	}
}

func TestRolloutProgressDeadlineMarksDeploymentFailed(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "badger")
	st, err := store.NewBadgerStateStore(dbPath)
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })

	orch := NewOrchestrator(st, scheduler.NewPlanner("least-loaded"), 20*time.Second)
	if err := orch.RegisterNode(context.Background(), model.Node{ID: "node-1", Address: "10.0.0.1", Allocatable: model.Resource{MilliCPU: 4000, MemoryMB: 4000}, Capacity: model.Resource{MilliCPU: 4000, MemoryMB: 4000}}); err != nil {
		t.Fatalf("register node: %v", err)
	}
	dep, err := orch.CreateDeployment(context.Background(), model.DeploymentSpec{Name: "deadline", Image: "nginx:v1", Replicas: 1, Resources: model.Resource{MilliCPU: 100, MemoryMB: 100}})
	if err != nil {
		t.Fatalf("create deployment: %v", err)
	}
	_, err = orch.UpdateDeployment(context.Background(), dep.ID, model.DeploymentSpec{
		Name:      dep.Spec.Name,
		Image:     "nginx:v2",
		Replicas:  1,
		Resources: dep.Spec.Resources,
		Rollout: model.RolloutSpec{
			Strategy:                model.RolloutStrategyRollingUpdate,
			ProgressDeadlineSeconds: 1,
		},
	})
	if err != nil {
		t.Fatalf("update deployment: %v", err)
	}
	updated, err := st.GetDeployment(dep.ID)
	if err != nil {
		t.Fatalf("get updated deployment: %v", err)
	}
	updated.RolloutStatus.StartedAt = time.Now().UTC().Add(-5 * time.Second)
	updated.RolloutStatus.Phase = "Progressing"
	if err := st.UpdateDeployment(updated); err != nil {
		t.Fatalf("set startedAt in past: %v", err)
	}

	if err := orch.ReconcileAll(context.Background()); err != nil {
		t.Fatalf("reconcile all: %v", err)
	}
	final, err := st.GetDeployment(dep.ID)
	if err != nil {
		t.Fatalf("get final deployment: %v", err)
	}
	if final.Status != model.DeploymentFailed {
		t.Fatalf("expected deployment failed due to rollout deadline, got %s", final.Status)
	}
	if final.RolloutStatus.Phase != "Failed" {
		t.Fatalf("expected rollout phase Failed, got %s", final.RolloutStatus.Phase)
	}
}
