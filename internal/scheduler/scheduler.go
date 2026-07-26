package scheduler

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"math"
	"os/exec"
	"sort"
	"strings"
	"time"

	"minikube-orchestrator/internal/model"
)

var ErrNoSchedulableNode = errors.New("no schedulable node")

type Policy string

const (
	PolicyLeastLoaded Policy = "least-loaded"
	PolicyBestFit     Policy = "best-fit"
)

type Planner struct {
	policy            Policy
	plugins           map[Policy]ScorePlugin
	externalPluginCmd string
	externalTimeout   time.Duration
}

type ScorePlugin interface {
	Score(node model.Node, req model.Resource) float64
}

type scorePluginFunc func(node model.Node, req model.Resource) float64

func (f scorePluginFunc) Score(node model.Node, req model.Resource) float64 {
	return f(node, req)
}

func NewPlanner(policy string) *Planner {
	raw := strings.TrimSpace(policy)
	pluginCmd := ""
	if strings.HasPrefix(strings.ToLower(raw), "external:") {
		pluginCmd = strings.TrimSpace(strings.TrimPrefix(raw, "external:"))
		raw = "external"
	}
	p := Policy(strings.TrimSpace(strings.ToLower(raw)))
	if p != PolicyBestFit {
		p = PolicyLeastLoaded
	}
	planner := &Planner{policy: p, plugins: map[Policy]ScorePlugin{}, externalPluginCmd: pluginCmd, externalTimeout: 250 * time.Millisecond}
	planner.RegisterPlugin(PolicyLeastLoaded, scorePluginFunc(leastLoadedScore))
	planner.RegisterPlugin(PolicyBestFit, scorePluginFunc(bestFitScore))
	return planner
}

func (p *Planner) RegisterPlugin(policy Policy, plugin ScorePlugin) {
	if p.plugins == nil {
		p.plugins = map[Policy]ScorePlugin{}
	}
	p.plugins[policy] = plugin
}

type candidate struct {
	node  model.Node
	score float64
}

func (p *Planner) SelectNode(nodes []model.Node, req model.Resource, labels map[string]string, tolerations []model.Toleration) (model.Node, error) {
	candidates := make([]candidate, 0, len(nodes))
	for _, n := range nodes {
		if n.Status != model.NodeReady {
			continue
		}
		if !fits(n, req) {
			continue
		}
		if !toleratesNodeTaints(n, tolerations) {
			continue
		}
		if !matchesAffinity(n, labels) {
			continue
		}
		candidates = append(candidates, candidate{node: n, score: p.scoreNode(n, req)})
	}

	if len(candidates) == 0 {
		return model.Node{}, ErrNoSchedulableNode
	}

	sort.SliceStable(candidates, func(i, j int) bool {
		return candidates[i].score < candidates[j].score
	})
	return candidates[0].node, nil
}

func fits(n model.Node, req model.Resource) bool {
	availCPU := n.Allocatable.MilliCPU - n.Used.MilliCPU
	availMem := n.Allocatable.MemoryMB - n.Used.MemoryMB
	return req.MilliCPU <= availCPU && req.MemoryMB <= availMem
}

func (p *Planner) scoreNode(n model.Node, req model.Resource) float64 {
	if p.externalPluginCmd != "" {
		if score, err := p.scoreNodeWithExternalPlugin(n, req); err == nil {
			return score
		}
	}
	plugin := p.plugins[p.policy]
	if plugin == nil {
		plugin = p.plugins[PolicyLeastLoaded]
	}
	if plugin == nil {
		return leastLoadedScore(n, req)
	}
	return plugin.Score(n, req)
}

func (p *Planner) scoreNodeWithExternalPlugin(n model.Node, req model.Resource) (float64, error) {
	ctx, cancel := context.WithTimeout(context.Background(), p.externalTimeout)
	defer cancel()

	parts := strings.Fields(p.externalPluginCmd)
	if len(parts) == 0 {
		return 0, errors.New("external plugin command is empty")
	}
	cmd := exec.CommandContext(ctx, parts[0], parts[1:]...)
	input := map[string]any{"node": n, "resource": req}
	blob, _ := json.Marshal(input)
	cmd.Stdin = bytes.NewReader(blob)
	out, err := cmd.Output()
	if err != nil {
		return 0, err
	}
	var payload struct {
		Score float64 `json:"score"`
	}
	if err := json.Unmarshal(out, &payload); err != nil {
		return 0, err
	}
	return payload.Score, nil
}

func leastLoadedScore(n model.Node, req model.Resource) float64 {
	allocCPU := math.Max(1, float64(n.Allocatable.MilliCPU))
	allocMem := math.Max(1, float64(n.Allocatable.MemoryMB))

	cpuAfter := float64(n.Used.MilliCPU+req.MilliCPU) / allocCPU
	memAfter := float64(n.Used.MemoryMB+req.MemoryMB) / allocMem

	spreadPenalty := math.Abs(cpuAfter - memAfter)
	return (cpuAfter+memAfter)*0.5 + spreadPenalty*0.15
}

func bestFitScore(n model.Node, req model.Resource) float64 {
	allocCPU := math.Max(1, float64(n.Allocatable.MilliCPU))
	allocMem := math.Max(1, float64(n.Allocatable.MemoryMB))

	leftCPU := float64((n.Allocatable.MilliCPU - n.Used.MilliCPU) - req.MilliCPU)
	leftMem := float64((n.Allocatable.MemoryMB - n.Used.MemoryMB) - req.MemoryMB)

	leftCPU = math.Max(0, leftCPU) / allocCPU
	leftMem = math.Max(0, leftMem) / allocMem

	leftoverScore := (leftCPU + leftMem) * 0.5
	balancePenalty := math.Abs(leftCPU-leftMem) * 0.1
	return leftoverScore + balancePenalty
}

func toleratesNodeTaints(n model.Node, tolerations []model.Toleration) bool {
	if len(n.Taints) == 0 {
		return true
	}
	for _, taint := range n.Taints {
		matched := false
		for _, tol := range tolerations {
			if strings.TrimSpace(tol.Key) == "" {
				continue
			}
			if tol.Key != taint.Key {
				continue
			}
			if strings.TrimSpace(tol.Effect) != "" && tol.Effect != taint.Effect {
				continue
			}
			op := strings.ToLower(strings.TrimSpace(tol.Operator))
			if op == "exists" {
				matched = true
				break
			}
			if strings.TrimSpace(tol.Value) == "" || tol.Value == taint.Value {
				matched = true
				break
			}
		}
		if !matched {
			return false
		}
	}
	return true
}

func matchesAffinity(n model.Node, labels map[string]string) bool {
	if len(labels) == 0 {
		return true
	}

	for k, v := range labels {
		if !strings.HasPrefix(k, "affinity.") {
			continue
		}
		nodeKey := strings.TrimPrefix(k, "affinity.")
		if n.Labels[nodeKey] != v {
			return false
		}
	}

	for k, v := range labels {
		if !strings.HasPrefix(k, "anti-affinity.") {
			continue
		}
		nodeKey := strings.TrimPrefix(k, "anti-affinity.")
		if n.Labels[nodeKey] == v {
			return false
		}
	}

	return true
}
