package scheduler

import (
	"testing"

	"minikube-orchestrator/internal/model"
)

type fixedScorePlugin struct {
	scores map[string]float64
}

func (p fixedScorePlugin) Score(node model.Node, _ model.Resource) float64 {
	return p.scores[node.ID]
}

func TestPolicyBestFitPrefersTighterNode(t *testing.T) {
	nodes := []model.Node{
		{
			ID:          "n1",
			Status:      model.NodeReady,
			Allocatable: model.Resource{MilliCPU: 1000, MemoryMB: 1000},
			Used:        model.Resource{MilliCPU: 100, MemoryMB: 100},
		},
		{
			ID:          "n2",
			Status:      model.NodeReady,
			Allocatable: model.Resource{MilliCPU: 1000, MemoryMB: 1000},
			Used:        model.Resource{MilliCPU: 700, MemoryMB: 700},
		},
	}
	req := model.Resource{MilliCPU: 200, MemoryMB: 200}

	least := NewPlanner("least-loaded")
	leastNode, err := least.SelectNode(nodes, req, nil, nil)
	if err != nil {
		t.Fatalf("least-loaded select: %v", err)
	}
	if leastNode.ID != "n1" {
		t.Fatalf("expected n1 for least-loaded, got %s", leastNode.ID)
	}

	best := NewPlanner("best-fit")
	bestNode, err := best.SelectNode(nodes, req, nil, nil)
	if err != nil {
		t.Fatalf("best-fit select: %v", err)
	}
	if bestNode.ID != "n2" {
		t.Fatalf("expected n2 for best-fit, got %s", bestNode.ID)
	}
}

func TestTaintsRequireMatchingToleration(t *testing.T) {
	nodes := []model.Node{
		{
			ID:          "tainted",
			Status:      model.NodeReady,
			Allocatable: model.Resource{MilliCPU: 1000, MemoryMB: 1000},
			Used:        model.Resource{},
			Taints: []model.Taint{
				{Key: "dedicated", Value: "db", Effect: "NoSchedule"},
			},
		},
	}
	req := model.Resource{MilliCPU: 100, MemoryMB: 100}
	planner := NewPlanner("least-loaded")

	if _, err := planner.SelectNode(nodes, req, nil, nil); err == nil {
		t.Fatal("expected scheduling to fail without toleration")
	}

	tolerations := []model.Toleration{{Key: "dedicated", Value: "db", Effect: "NoSchedule"}}
	node, err := planner.SelectNode(nodes, req, nil, tolerations)
	if err != nil {
		t.Fatalf("expected scheduling with toleration, got error: %v", err)
	}
	if node.ID != "tainted" {
		t.Fatalf("expected tainted node selected, got %s", node.ID)
	}
}

func TestCustomPolicyPluginSelectsExpectedNode(t *testing.T) {
	nodes := []model.Node{
		{ID: "n1", Status: model.NodeReady, Allocatable: model.Resource{MilliCPU: 1000, MemoryMB: 1000}},
		{ID: "n2", Status: model.NodeReady, Allocatable: model.Resource{MilliCPU: 1000, MemoryMB: 1000}},
	}
	planner := NewPlanner("least-loaded")
	planner.RegisterPlugin(Policy("custom"), fixedScorePlugin{scores: map[string]float64{"n1": 10, "n2": 1}})
	planner.policy = Policy("custom")

	node, err := planner.SelectNode(nodes, model.Resource{MilliCPU: 100, MemoryMB: 100}, nil, nil)
	if err != nil {
		t.Fatalf("select node: %v", err)
	}
	if node.ID != "n2" {
		t.Fatalf("expected n2 with custom plugin, got %s", node.ID)
	}
}

func TestExternalPluginFailureFallsBackToBuiltInPolicy(t *testing.T) {
	nodes := []model.Node{
		{
			ID:          "n1",
			Status:      model.NodeReady,
			Allocatable: model.Resource{MilliCPU: 1000, MemoryMB: 1000},
			Used:        model.Resource{MilliCPU: 100, MemoryMB: 100},
		},
		{
			ID:          "n2",
			Status:      model.NodeReady,
			Allocatable: model.Resource{MilliCPU: 1000, MemoryMB: 1000},
			Used:        model.Resource{MilliCPU: 700, MemoryMB: 700},
		},
	}
	planner := NewPlanner("external:nonexistent-scheduler-plugin")
	node, err := planner.SelectNode(nodes, model.Resource{MilliCPU: 100, MemoryMB: 100}, nil, nil)
	if err != nil {
		t.Fatalf("select node: %v", err)
	}
	if node.ID != "n1" {
		t.Fatalf("expected fallback to least-loaded policy selecting n1, got %s", node.ID)
	}
}
