package plugin

import "minikube-orchestrator/internal/model"

// SchedulerPlugin is the contract for external schedulers.
type SchedulerPlugin interface {
	Name() string
	Score(node model.Node, req model.Resource) float64
}

// ControllerPlugin is the contract for custom controller extensions.
type ControllerPlugin interface {
	Name() string
	Kind() string
	Reconcile() error
}

// RuntimeIsolationPolicy models isolation boundaries for plugin execution.
type RuntimeIsolationPolicy struct {
	ExecTimeoutSeconds int  `json:"execTimeoutSeconds"`
	ReadOnlyFS         bool `json:"readOnlyFS"`
	DropNetwork        bool `json:"dropNetwork"`
}
