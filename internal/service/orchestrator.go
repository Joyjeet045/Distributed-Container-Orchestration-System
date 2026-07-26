package service

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math"
	"math/rand"
	"reflect"
	"sort"
	"strings"
	"sync"
	"time"

	"minikube-orchestrator/internal/loadbalancer"
	"minikube-orchestrator/internal/model"
	"minikube-orchestrator/internal/scheduler"
	"minikube-orchestrator/internal/store"
)

type Orchestrator struct {
	store     store.StateStore
	scheduler *scheduler.Planner
	balance   *loadbalancer.Balancer

	nodeTTL time.Duration
	mu      sync.Mutex

	subsMu      sync.RWMutex
	subscribers map[chan model.Event]struct{}

	healthMu      sync.Mutex
	healthSamples map[string][]model.NodeHealthSample
}

func NewOrchestrator(st store.StateStore, sch *scheduler.Planner, nodeTTL time.Duration) *Orchestrator {
	return &Orchestrator{store: st, scheduler: sch, balance: loadbalancer.NewBalancer(), nodeTTL: nodeTTL, subscribers: map[chan model.Event]struct{}{}, healthSamples: map[string][]model.NodeHealthSample{}}
}

func (o *Orchestrator) RegisterNode(_ context.Context, node model.Node) error {
	if strings.TrimSpace(node.ID) == "" {
		return errors.New("node id is required")
	}
	if node.Capacity.MilliCPU == 0 {
		node.Capacity.MilliCPU = 2000
	}
	if node.Capacity.MemoryMB == 0 {
		node.Capacity.MemoryMB = 4096
	}
	if node.Allocatable.MilliCPU == 0 {
		node.Allocatable.MilliCPU = node.Capacity.MilliCPU
	}
	if node.Allocatable.MemoryMB == 0 {
		node.Allocatable.MemoryMB = node.Capacity.MemoryMB
	}
	if node.Labels == nil {
		node.Labels = map[string]string{}
	}
	node.Status = model.NodeReady
	node.Health = model.NodeHealth{
		FailureClass:    model.NodeFailureHealthy,
		LastEvaluatedAt: time.Now().UTC(),
	}
	node.LastSeen = time.Now().UTC()
	if err := o.store.UpsertNode(node); err != nil {
		return err
	}
	_ = o.rebalanceWorkloads()
	return nil
}

func (o *Orchestrator) HeartbeatNode(_ context.Context, nodeID string, used model.Resource) error {
	node, err := o.store.GetNode(nodeID)
	if err != nil {
		return err
	}
	node.Used = used
	node.Status = model.NodeReady
	node.LastSeen = time.Now().UTC()
	node.Health.LastHeartbeatAgeSec = 0
	node.Health.ConsecutiveMissedTTL = 0
	node.Health.Isolated = false
	node.Health.Draining = false
	node.Health.FailureClass = model.NodeFailureHealthy
	node.Health.Reason = ""
	node.Health.LastEvaluatedAt = time.Now().UTC()
	return o.store.UpsertNode(node)
}

func (o *Orchestrator) CreateDeployment(_ context.Context, spec model.DeploymentSpec) (model.Deployment, error) {
	if strings.TrimSpace(spec.Name) == "" {
		return model.Deployment{}, errors.New("name is required")
	}
	if strings.TrimSpace(spec.Image) == "" {
		return model.Deployment{}, errors.New("image is required")
	}
	if spec.Replicas <= 0 {
		spec.Replicas = 1
	}
	if spec.Resources.MilliCPU == 0 {
		spec.Resources.MilliCPU = 250
	}
	if spec.Resources.MemoryMB == 0 {
		spec.Resources.MemoryMB = 256
	}
	if strings.TrimSpace(spec.Namespace) == "" {
		spec.Namespace = "default"
	}
	if err := o.runAdmissionCheck("Deployment", spec); err != nil {
		return model.Deployment{}, err
	}
	if err := o.ensureNamespaceExists(spec.Namespace); err != nil {
		return model.Deployment{}, err
	}
	for _, claim := range spec.VolumeClaims {
		pvc, err := o.store.GetPersistentVolumeClaim(spec.Namespace, claim)
		if err != nil {
			return model.Deployment{}, fmt.Errorf("volume claim %s lookup failed: %w", claim, err)
		}
		if pvc.Phase != model.PersistentVolumeClaimBound {
			return model.Deployment{}, fmt.Errorf("volume claim %s is not bound", claim)
		}
	}
	if err := o.enforceNamespaceQuota(spec); err != nil {
		return model.Deployment{}, err
	}
	if spec.Labels == nil {
		spec.Labels = map[string]string{}
	}
	if spec.Tolerations == nil {
		spec.Tolerations = []model.Toleration{}
	}
	spec.Rollout = normalizeRolloutSpec(spec.Rollout)

	now := time.Now().UTC()
	dep := model.Deployment{
		ID:                 fmt.Sprintf("dep-%d-%04d", now.UnixNano(), rand.Intn(9999)),
		Spec:               spec,
		Generation:         1,
		ObservedGeneration: 1,
		CurrentRevision:    1,
		RevisionHistory: []model.DeploymentRevision{{
			Revision:  1,
			Spec:      spec,
			CreatedAt: now,
		}},
		RolloutStatus: model.DeploymentRolloutStatus{Phase: "Stable", LastUpdatedAt: now},
		Owner:         "orchestrator/controller-manager",
		Finalizers:    []string{"workloads.cleanup"},
		Status:        model.DeploymentPending,
		Conditions: []model.DeploymentCondition{
			{
				Type:               model.ConditionProgressing,
				Status:             true,
				Reason:             "DeploymentAccepted",
				Message:            "Deployment accepted and awaiting reconciliation",
				LastTransitionTime: now,
			},
		},
		CreatedAt: now,
		UpdatedAt: now,
	}

	if err := o.store.CreateDeployment(dep); err != nil {
		return model.Deployment{}, err
	}

	if err := o.reconcileDeployment(dep.ID); err != nil {
		return model.Deployment{}, err
	}
	_ = o.emitEvent(model.Event{
		Level:        model.EventInfo,
		Type:         "reconcile",
		Reason:       "DeploymentCreated",
		Message:      "Deployment reconciled after creation",
		DeploymentID: dep.ID,
	})
	dep.Status = model.DeploymentRunning
	dep.Conditions = setCondition(dep.Conditions, model.DeploymentCondition{
		Type:               model.ConditionAvailable,
		Status:             true,
		Reason:             "ReplicasScheduled",
		Message:            "Deployment has scheduled replicas",
		LastTransitionTime: time.Now().UTC(),
	})
	dep.Conditions = setCondition(dep.Conditions, model.DeploymentCondition{
		Type:               model.ConditionProgressing,
		Status:             false,
		Reason:             "ReconciliationComplete",
		Message:            "Initial reconciliation completed",
		LastTransitionTime: time.Now().UTC(),
	})
	dep.UpdatedAt = time.Now().UTC()
	_ = o.store.UpdateDeployment(dep)

	return dep, nil
}

func (o *Orchestrator) DeleteDeployment(_ context.Context, deploymentID string) error {
	o.mu.Lock()
	defer o.mu.Unlock()

	dep, err := o.store.GetDeployment(deploymentID)
	if err != nil {
		return err
	}
	now := time.Now().UTC()
	dep.DeletionTimestamp = &now
	dep.Status = model.DeploymentPending
	dep.Conditions = setCondition(dep.Conditions, model.DeploymentCondition{
		Type:               model.ConditionProgressing,
		Status:             true,
		Reason:             "DeletionInProgress",
		Message:            "Deployment marked for deletion",
		LastTransitionTime: now,
	})
	if err := o.store.UpdateDeployment(dep); err != nil {
		return err
	}
	_ = o.emitEvent(model.Event{Level: model.EventInfo, Type: "reconcile", Reason: "DeploymentMarkedForDeletion", Message: "Deployment marked for deletion", DeploymentID: dep.ID})
	return nil
}

func (o *Orchestrator) ListDeployments(_ context.Context) ([]model.Deployment, error) {
	return o.store.ListDeployments()
}

func (o *Orchestrator) UpsertAutoscalerPolicy(_ context.Context, policy model.AutoscalerPolicy) (model.AutoscalerPolicy, error) {
	if strings.TrimSpace(policy.DeploymentID) == "" {
		return model.AutoscalerPolicy{}, errors.New("deployment id is required")
	}
	if _, err := o.store.GetDeployment(policy.DeploymentID); err != nil {
		return model.AutoscalerPolicy{}, err
	}
	if policy.MinReplicas <= 0 {
		policy.MinReplicas = 1
	}
	if policy.MaxReplicas <= 0 || policy.MaxReplicas < policy.MinReplicas {
		policy.MaxReplicas = policy.MinReplicas
	}
	if policy.StabilizationWindowSec <= 0 {
		policy.StabilizationWindowSec = 30
	}
	if policy.ScaleUpCooldownSec <= 0 {
		policy.ScaleUpCooldownSec = 20
	}
	if policy.ScaleDownCooldownSec <= 0 {
		policy.ScaleDownCooldownSec = 40
	}
	if policy.PredictiveScalingEnabled {
		if policy.PredictiveLookbackSamples <= 1 {
			policy.PredictiveLookbackSamples = 6
		}
		if policy.PredictiveScaleFactor <= 0 {
			policy.PredictiveScaleFactor = 2
		}
	}
	if strings.TrimSpace(policy.ID) == "" {
		policy.ID = fmt.Sprintf("as-%d-%04d", time.Now().UnixNano(), rand.Intn(9999))
	}
	if policy.CreatedAt.IsZero() {
		policy.CreatedAt = time.Now().UTC()
	}
	policy.UpdatedAt = time.Now().UTC()
	if err := o.store.UpsertAutoscalerPolicy(policy); err != nil {
		return model.AutoscalerPolicy{}, err
	}
	_ = o.emitEvent(model.Event{Level: model.EventInfo, Type: "autoscaler", Reason: "AutoscalerPolicyUpserted", Message: "Autoscaler policy configured", DeploymentID: policy.DeploymentID})
	return policy, nil
}

func (o *Orchestrator) ListAutoscalerPolicies(_ context.Context) ([]model.AutoscalerPolicy, error) {
	return o.store.ListAutoscalerPolicies()
}

func (o *Orchestrator) DeleteAutoscalerPolicy(_ context.Context, policyID string) error {
	return o.store.DeleteAutoscalerPolicy(policyID)
}

func (o *Orchestrator) IngestDeploymentMetric(_ context.Context, sample model.DeploymentMetricSample) error {
	if strings.TrimSpace(sample.DeploymentID) == "" {
		return errors.New("deployment id is required")
	}
	if _, err := o.store.GetDeployment(sample.DeploymentID); err != nil {
		return err
	}
	if sample.Custom == nil {
		sample.Custom = map[string]float64{}
	}
	if sample.Timestamp.IsZero() {
		sample.Timestamp = time.Now().UTC()
	}
	if sample.ID == "" {
		sample.ID = fmt.Sprintf("ms-%d-%04d", sample.Timestamp.UnixNano(), rand.Intn(9999))
	}
	return o.store.AppendDeploymentMetric(sample)
}

func (o *Orchestrator) UpdateDeployment(_ context.Context, deploymentID string, nextSpec model.DeploymentSpec) (model.Deployment, error) {
	o.mu.Lock()
	defer o.mu.Unlock()

	dep, err := o.store.GetDeployment(deploymentID)
	if err != nil {
		return model.Deployment{}, err
	}
	nextSpec.Rollout = normalizeRolloutSpec(nextSpec.Rollout)
	if nextSpec.Replicas <= 0 {
		nextSpec.Replicas = dep.Spec.Replicas
	}
	if nextSpec.Resources.MilliCPU == 0 {
		nextSpec.Resources.MilliCPU = dep.Spec.Resources.MilliCPU
	}
	if nextSpec.Resources.MemoryMB == 0 {
		nextSpec.Resources.MemoryMB = dep.Spec.Resources.MemoryMB
	}
	if nextSpec.Labels == nil {
		nextSpec.Labels = dep.Spec.Labels
	}
	if nextSpec.Tolerations == nil {
		nextSpec.Tolerations = dep.Spec.Tolerations
	}

	specChanged := !reflect.DeepEqual(dep.Spec, nextSpec)
	dep.Spec = nextSpec
	dep.Generation++
	dep.Status = model.DeploymentPending
	dep.RolloutStatus.Phase = "Progressing"
	dep.RolloutStatus.Message = "Deployment spec updated"
	dep.RolloutStatus.StartedAt = time.Now().UTC()
	dep.RolloutStatus.LastUpdatedAt = time.Now().UTC()
	dep.Conditions = setCondition(dep.Conditions, model.DeploymentCondition{
		Type:               model.ConditionProgressing,
		Status:             true,
		Reason:             "RolloutStarted",
		Message:            "Deployment rollout started",
		LastTransitionTime: time.Now().UTC(),
	})
	if specChanged {
		dep.CurrentRevision++
		dep.RevisionHistory = append(dep.RevisionHistory, model.DeploymentRevision{
			Revision:  dep.CurrentRevision,
			Spec:      dep.Spec,
			CreatedAt: time.Now().UTC(),
		})
	}
	dep.UpdatedAt = time.Now().UTC()
	if err := o.store.UpdateDeployment(dep); err != nil {
		return model.Deployment{}, err
	}
	_ = o.emitEvent(model.Event{Level: model.EventInfo, Type: "rollout", Reason: "DeploymentUpdated", Message: "Deployment spec updated for rollout", DeploymentID: dep.ID})
	return dep, nil
}

func (o *Orchestrator) RollbackDeployment(_ context.Context, deploymentID string, targetRevision int) (model.Deployment, error) {
	o.mu.Lock()
	defer o.mu.Unlock()

	dep, err := o.store.GetDeployment(deploymentID)
	if err != nil {
		return model.Deployment{}, err
	}
	if len(dep.RevisionHistory) == 0 {
		return model.Deployment{}, errors.New("no revision history available")
	}
	if targetRevision <= 0 {
		targetRevision = dep.CurrentRevision - 1
	}
	var target *model.DeploymentRevision
	for i := range dep.RevisionHistory {
		if dep.RevisionHistory[i].Revision == targetRevision {
			target = &dep.RevisionHistory[i]
			break
		}
	}
	if target == nil {
		return model.Deployment{}, fmt.Errorf("target revision %d not found", targetRevision)
	}

	dep.Spec = target.Spec
	dep.Spec.Rollout = normalizeRolloutSpec(dep.Spec.Rollout)
	dep.Generation++
	dep.CurrentRevision++
	dep.RevisionHistory = append(dep.RevisionHistory, model.DeploymentRevision{
		Revision:  dep.CurrentRevision,
		Spec:      dep.Spec,
		CreatedAt: time.Now().UTC(),
	})
	dep.Status = model.DeploymentPending
	dep.RolloutStatus.Phase = "Progressing"
	dep.RolloutStatus.Message = fmt.Sprintf("Rollback to revision %d started", targetRevision)
	dep.RolloutStatus.StartedAt = time.Now().UTC()
	dep.RolloutStatus.LastUpdatedAt = time.Now().UTC()
	dep.Conditions = setCondition(dep.Conditions, model.DeploymentCondition{
		Type:               model.ConditionProgressing,
		Status:             true,
		Reason:             "RollbackStarted",
		Message:            fmt.Sprintf("Rollback started from revision %d", targetRevision),
		LastTransitionTime: time.Now().UTC(),
	})
	dep.UpdatedAt = time.Now().UTC()
	if err := o.store.UpdateDeployment(dep); err != nil {
		return model.Deployment{}, err
	}
	_ = o.emitEvent(model.Event{Level: model.EventWarn, Type: "rollout", Reason: "RollbackStarted", Message: fmt.Sprintf("Rollback to revision %d started", targetRevision), DeploymentID: dep.ID})
	return dep, nil
}

func (o *Orchestrator) CreateService(_ context.Context, spec model.ServiceSpec) (model.Service, error) {
	if strings.TrimSpace(spec.Name) == "" {
		return model.Service{}, errors.New("service name is required")
	}
	if len(spec.Selector) == 0 {
		return model.Service{}, errors.New("service selector is required")
	}
	if len(spec.Ports) == 0 {
		return model.Service{}, errors.New("service ports are required")
	}
	if spec.Type == "" {
		spec.Type = model.ServiceTypeClusterIP
	}
	if strings.TrimSpace(spec.Namespace) == "" {
		spec.Namespace = "default"
	}
	if err := o.runAdmissionCheck("Service", spec); err != nil {
		return model.Service{}, err
	}
	if err := o.ensureNamespaceExists(spec.Namespace); err != nil {
		return model.Service{}, err
	}
	now := time.Now().UTC()
	svc := model.Service{
		ID:                  fmt.Sprintf("svc-%d-%04d", now.UnixNano(), rand.Intn(9999)),
		Spec:                spec,
		ClusterIP:           allocateClusterIP(now.UnixNano()),
		DNSRecordTTLSeconds: 30,
		CreatedAt:           now,
		UpdatedAt:           now,
	}
	if err := o.store.CreateService(svc); err != nil {
		return model.Service{}, err
	}
	if err := o.reconcileServiceEndpoints(svc.ID); err != nil {
		return model.Service{}, err
	}
	_ = o.emitEvent(model.Event{Level: model.EventInfo, Type: "service", Reason: "ServiceCreated", Message: "Service created", DeploymentID: svc.ID})
	return svc, nil
}

func (o *Orchestrator) CreateNamespace(_ context.Context, ns model.Namespace) (model.Namespace, error) {
	if strings.TrimSpace(ns.Name) == "" {
		return model.Namespace{}, errors.New("namespace name is required")
	}
	ns.Name = strings.TrimSpace(ns.Name)
	if strings.TrimSpace(ns.Tenant) == "" {
		ns.Tenant = "default"
	}
	now := time.Now().UTC()
	if ns.CreatedAt.IsZero() {
		ns.CreatedAt = now
	}
	ns.UpdatedAt = now
	if err := o.store.CreateNamespace(ns); err != nil {
		return model.Namespace{}, err
	}
	_ = o.emitEvent(model.Event{Level: model.EventInfo, Type: "namespace", Reason: "NamespaceCreated", Message: "Namespace created", DeploymentID: ns.Name})
	return ns, nil
}

func (o *Orchestrator) ListNamespaces(_ context.Context) ([]model.Namespace, error) {
	return o.store.ListNamespaces()
}

func (o *Orchestrator) UpsertNamespaceQuota(_ context.Context, quota model.NamespaceQuota) (model.NamespaceQuota, error) {
	if strings.TrimSpace(quota.Namespace) == "" {
		return model.NamespaceQuota{}, errors.New("quota namespace is required")
	}
	if err := o.ensureNamespaceExists(quota.Namespace); err != nil {
		return model.NamespaceQuota{}, err
	}
	if quota.MaxDeployments < 0 || quota.MaxMilliCPU < 0 || quota.MaxMemoryMB < 0 {
		return model.NamespaceQuota{}, errors.New("quota values cannot be negative")
	}
	if err := o.store.UpsertNamespaceQuota(quota); err != nil {
		return model.NamespaceQuota{}, err
	}
	return o.store.GetNamespaceQuota(quota.Namespace)
}

func (o *Orchestrator) ListNamespaceQuotas(_ context.Context) ([]model.NamespaceQuota, error) {
	return o.store.ListNamespaceQuotas()
}

func (o *Orchestrator) UpsertSecret(_ context.Context, secret model.Secret) (model.Secret, error) {
	if strings.TrimSpace(secret.Namespace) == "" {
		secret.Namespace = "default"
	}
	if strings.TrimSpace(secret.Name) == "" {
		return model.Secret{}, errors.New("secret name is required")
	}
	if err := o.ensureNamespaceExists(secret.Namespace); err != nil {
		return model.Secret{}, err
	}
	if secret.Data == nil {
		secret.Data = map[string]string{}
	}
	enc, err := o.encryptSecretData(secret.Data)
	if err != nil {
		return model.Secret{}, err
	}
	secret.Data = enc
	if err := o.store.UpsertSecret(secret); err != nil {
		return model.Secret{}, err
	}
	out, err := o.store.GetSecret(secret.Namespace, secret.Name)
	if err != nil {
		return model.Secret{}, err
	}
	out.Data = o.decryptSecretData(out.Data)
	return out, nil
}

func (o *Orchestrator) ListSecrets(_ context.Context, namespace string) ([]model.Secret, error) {
	if strings.TrimSpace(namespace) == "" {
		namespace = "default"
	}
	items, err := o.store.ListSecrets(namespace)
	if err != nil {
		return nil, err
	}
	for i := range items {
		items[i].Data = o.decryptSecretData(items[i].Data)
	}
	return items, nil
}

func (o *Orchestrator) ListServices(_ context.Context) ([]model.Service, error) {
	return o.store.ListServices()
}

func (o *Orchestrator) ListServiceEndpoints(_ context.Context, serviceID string) ([]model.ServiceEndpoint, error) {
	if _, err := o.store.GetService(serviceID); err != nil {
		return nil, err
	}
	return o.store.ListServiceEndpoints(serviceID)
}

func (o *Orchestrator) ResolveServiceName(_ context.Context, name string) (string, []model.ServiceEndpoint, error) {
	dnsName, endpoints, _, err := o.ResolveServiceDNS(context.Background(), name)
	if err != nil {
		return "", nil, err
	}
	return dnsName, endpoints, nil
}

func (o *Orchestrator) ResolveServiceDNS(_ context.Context, name string) (string, []model.ServiceEndpoint, int, error) {
	services, err := o.store.ListServices()
	if err != nil {
		return "", nil, 0, err
	}
	for _, svc := range services {
		if strings.EqualFold(strings.TrimSpace(svc.Spec.Name), strings.TrimSpace(name)) {
			endpoints, err := o.store.ListServiceEndpoints(svc.ID)
			if err != nil {
				return "", nil, 0, err
			}
			dnsName := fmt.Sprintf("%s.default.svc.cluster.local", svc.Spec.Name)
			ttl := svc.DNSRecordTTLSeconds
			if ttl <= 0 {
				ttl = 30
			}
			return dnsName, endpoints, ttl, nil
		}
	}
	return "", nil, 0, store.ErrNotFound
}

func (o *Orchestrator) SelectServiceEndpoint(_ context.Context, serviceID, strategy string) (model.ServiceEndpoint, error) {
	if _, err := o.store.GetService(serviceID); err != nil {
		return model.ServiceEndpoint{}, err
	}
	endpoints, err := o.store.ListServiceEndpoints(serviceID)
	if err != nil {
		return model.ServiceEndpoint{}, err
	}
	nodes, err := o.store.ListNodes()
	if err != nil {
		return model.ServiceEndpoint{}, err
	}
	healthyNodes := map[string]bool{}
	for _, n := range nodes {
		healthyNodes[n.ID] = n.Status == model.NodeReady
	}
	filtered := make([]model.ServiceEndpoint, 0, len(endpoints))
	for _, ep := range endpoints {
		if healthyNodes[ep.NodeID] {
			filtered = append(filtered, ep)
		}
	}
	return o.balance.Select(serviceID, strategy, filtered)
}

func (o *Orchestrator) ReleaseServiceEndpoint(_ context.Context, endpointID string) {
	o.balance.Release(endpointID)
}

func (o *Orchestrator) ClusterState(_ context.Context) (model.ClusterState, error) {
	return o.store.Snapshot()
}

func (o *Orchestrator) PollAssignments(_ context.Context, nodeID string, max int) ([]model.Assignment, error) {
	if max <= 0 {
		max = 10
	}
	return o.store.PopAssignments(nodeID, max)
}

func (o *Orchestrator) ReportWorkloadStatus(_ context.Context, workloadID string, status model.WorkloadStatus, containerID string) error {
	w, err := o.store.GetWorkload(workloadID)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil
		}
		return err
	}
	if status == model.WorkloadTerminated {
		node, nodeErr := o.store.GetNode(w.NodeID)
		if nodeErr == nil {
			node.Used.MilliCPU -= w.Resources.MilliCPU
			node.Used.MemoryMB -= w.Resources.MemoryMB
			if node.Used.MilliCPU < 0 {
				node.Used.MilliCPU = 0
			}
			if node.Used.MemoryMB < 0 {
				node.Used.MemoryMB = 0
			}
			_ = o.store.UpsertNode(node)
		}
		if err := o.store.DeleteWorkload(w.ID); err != nil {
			return err
		}
		dep, depErr := o.store.GetDeployment(w.DeploymentID)
		if depErr == nil {
			workloads, lerr := o.store.ListWorkloadsByDeployment(dep.ID)
			if lerr == nil {
				ready := runningCount(workloads)
				active := runningOrPendingCount(workloads)
				if active <= dep.Spec.Replicas {
					dep.Status = model.DeploymentRunning
					dep.RolloutStatus.Phase = "Stable"
					dep.RolloutStatus.Message = "rollout complete"
					dep.RolloutStatus.ReadyReplicas = ready
					dep.RolloutStatus.UpdatedReplicas = active
					dep.RolloutStatus.UnavailableReplicas = maxInt(0, dep.Spec.Replicas-ready)
					dep.RolloutStatus.LastUpdatedAt = time.Now().UTC()
					dep.Conditions = setCondition(dep.Conditions, model.DeploymentCondition{Type: model.ConditionAvailable, Status: true, Reason: "DesiredReplicasPlaced", Message: "Desired replica count has been placed", LastTransitionTime: time.Now().UTC()})
					dep.Conditions = setCondition(dep.Conditions, model.DeploymentCondition{Type: model.ConditionProgressing, Status: false, Reason: "ReconciliationStable", Message: "Reconciliation reached stable state", LastTransitionTime: time.Now().UTC()})
					dep.Conditions = setCondition(dep.Conditions, model.DeploymentCondition{Type: model.ConditionDegraded, Status: false, Reason: "Healthy", Message: "Deployment is healthy", LastTransitionTime: time.Now().UTC()})
					dep.ObservedGeneration = dep.Generation
					dep.UpdatedAt = time.Now().UTC()
					_ = o.store.UpdateDeployment(dep)
				}
			}
		}
		_ = o.emitEvent(model.Event{Level: model.EventInfo, Type: "workload", Reason: "WorkloadTerminated", Message: "Workload terminated and resources released", DeploymentID: w.DeploymentID, WorkloadID: w.ID, NodeID: w.NodeID})
		return nil
	}
	w.Status = status
	w.ContainerID = containerID
	if status == model.WorkloadFailed {
		w.RestartCount++
	}
	return o.store.UpsertWorkload(w)
}

func (o *Orchestrator) ReconcileAll(ctx context.Context) error {
	deps, err := o.store.ListDeployments()
	if err != nil {
		_ = o.emitEvent(model.Event{Level: model.EventError, Type: "reconcile", Reason: "ListDeploymentsFailed", Message: err.Error()})
		return err
	}
	for _, dep := range deps {
		if dep.DeletionTimestamp != nil {
			if err := o.reconcileDeletion(dep.ID); err != nil {
				_ = o.emitEvent(model.Event{Level: model.EventError, Type: "reconcile", Reason: "ReconcileDeletionFailed", Message: err.Error(), DeploymentID: dep.ID})
			}
			continue
		}
		if err := o.reconcileDeployment(dep.ID); err != nil {
			_ = o.emitEvent(model.Event{Level: model.EventError, Type: "reconcile", Reason: "ReconcileFailed", Message: err.Error(), DeploymentID: dep.ID})
			continue
		}
	}
	if err := o.reconcileAutoscaling(); err != nil {
		_ = o.emitEvent(model.Event{Level: model.EventError, Type: "autoscaler", Reason: "AutoscalerReconcileFailed", Message: err.Error()})
	}
	if err := o.evaluateWorkloadHealth(); err != nil {
		_ = o.emitEvent(model.Event{Level: model.EventError, Type: "health", Reason: "WorkloadHealthCheckFailed", Message: err.Error()})
	}
	if err := o.reconcileAllServiceEndpoints(); err != nil {
		_ = o.emitEvent(model.Event{Level: model.EventError, Type: "service", Reason: "ServiceEndpointReconcileFailed", Message: err.Error()})
	}
	if err := o.reconcilePVCBindings(); err != nil {
		_ = o.emitEvent(model.Event{Level: model.EventError, Type: "storage", Reason: "PVCBindingReconcileFailed", Message: err.Error()})
	}
	if err := o.reconcileJobsAndCron(ctx); err != nil {
		_ = o.emitEvent(model.Event{Level: model.EventError, Type: "job", Reason: "JobReconcileFailed", Message: err.Error()})
	}
	return o.updateNodeHealth(ctx)
}

func (o *Orchestrator) RouteServiceTraffic(_ context.Context, serviceName, strategy string) (model.Service, model.ServiceEndpoint, string, int, error) {
	svc, err := o.findServiceByName(serviceName)
	if err != nil {
		return model.Service{}, model.ServiceEndpoint{}, "", 0, err
	}
	target, err := o.SelectServiceEndpoint(context.Background(), svc.ID, strategy)
	if err != nil {
		return model.Service{}, model.ServiceEndpoint{}, "", 0, err
	}
	if len(svc.Spec.Ports) == 0 {
		return model.Service{}, model.ServiceEndpoint{}, "", 0, errors.New("service has no ports")
	}
	servicePort := svc.Spec.Ports[0]
	if svc.Spec.Type == model.ServiceTypeNodePort {
		if servicePort.NodePort == 0 {
			return model.Service{}, model.ServiceEndpoint{}, "", 0, errors.New("nodeport service missing nodePort")
		}
		return svc, target, target.Address, servicePort.NodePort, nil
	}
	return svc, target, svc.ClusterIP, servicePort.Port, nil
}

func (o *Orchestrator) ListNodeWorkloads(_ context.Context, nodeID string) ([]model.Workload, error) {
	all, err := o.store.ListWorkloads()
	if err != nil {
		return nil, err
	}
	out := make([]model.Workload, 0)
	for _, w := range all {
		if w.NodeID != nodeID {
			continue
		}
		if w.Status == model.WorkloadPending || w.Status == model.WorkloadRunning || w.Status == model.WorkloadTerminating {
			out = append(out, w)
		}
	}
	return out, nil
}

func (o *Orchestrator) ListEvents(_ context.Context, limit int) ([]model.Event, error) {
	return o.store.ListEvents(limit)
}

func (o *Orchestrator) ListNodeHealthTrends(_ context.Context, window int) ([]model.NodeHealthTrend, error) {
	if window <= 0 {
		window = 20
	}
	nodes, err := o.store.ListNodes()
	if err != nil {
		return nil, err
	}

	o.healthMu.Lock()
	defer o.healthMu.Unlock()

	trends := make([]model.NodeHealthTrend, 0, len(nodes))
	for _, n := range nodes {
		history := o.healthSamples[n.ID]
		if len(history) > window {
			history = history[len(history)-window:]
		}
		trend := model.NodeHealthTrend{NodeID: n.ID, CurrentFailureClass: n.Health.FailureClass, LastEvaluatedAt: n.Health.LastEvaluatedAt}
		if len(history) == 0 {
			trends = append(trends, trend)
			continue
		}
		trend.SampleCount = len(history)
		var cpuSum, memSum float64
		for _, s := range history {
			cpuSum += s.CPUUtilization
			memSum += s.MemoryUtilization
			if s.CPUUtilization > trend.MaxCPUUtilization {
				trend.MaxCPUUtilization = s.CPUUtilization
			}
			if s.MemoryUtilization > trend.MaxMemoryUtilization {
				trend.MaxMemoryUtilization = s.MemoryUtilization
			}
		}
		trend.AvgCPUUtilization = cpuSum / float64(len(history))
		trend.AvgMemoryUtilization = memSum / float64(len(history))

		latest := history[len(history)-1]
		if n.Health.FailureClass == model.NodeFailureCritical {
			trend.AnomalyDetected = true
			trend.AnomalyReason = "critical node health state"
		} else if (latest.CPUUtilization > 0.90 && latest.CPUUtilization > trend.AvgCPUUtilization*1.35) || (latest.MemoryUtilization > 0.90 && latest.MemoryUtilization > trend.AvgMemoryUtilization*1.35) {
			trend.AnomalyDetected = true
			trend.AnomalyReason = "resource utilization spike detected"
		}
		trends = append(trends, trend)
	}
	return trends, nil
}

func (o *Orchestrator) PrometheusMetrics(_ context.Context) (string, error) {
	state, err := o.store.Snapshot()
	if err != nil {
		return "", err
	}
	policies, err := o.store.ListAutoscalerPolicies()
	if err != nil {
		return "", err
	}
	readyNodes := 0
	for _, n := range state.Nodes {
		if n.Status == model.NodeReady {
			readyNodes++
		}
	}
	running := 0
	pending := 0
	failed := 0
	for _, w := range state.Workloads {
		switch w.Status {
		case model.WorkloadRunning:
			running++
		case model.WorkloadPending:
			pending++
		case model.WorkloadFailed:
			failed++
		}
	}
	var b strings.Builder
	b.WriteString("# HELP orch_nodes_total Total number of registered nodes\n")
	b.WriteString("# TYPE orch_nodes_total gauge\n")
	b.WriteString(fmt.Sprintf("orch_nodes_total %d\n", len(state.Nodes)))
	b.WriteString("# HELP orch_nodes_ready Number of nodes in Ready state\n")
	b.WriteString("# TYPE orch_nodes_ready gauge\n")
	b.WriteString(fmt.Sprintf("orch_nodes_ready %d\n", readyNodes))
	b.WriteString("# HELP orch_deployments_total Total number of deployments\n")
	b.WriteString("# TYPE orch_deployments_total gauge\n")
	b.WriteString(fmt.Sprintf("orch_deployments_total %d\n", len(state.Deployments)))
	b.WriteString("# HELP orch_workloads_running Number of running workloads\n")
	b.WriteString("# TYPE orch_workloads_running gauge\n")
	b.WriteString(fmt.Sprintf("orch_workloads_running %d\n", running))
	b.WriteString("# HELP orch_workloads_pending Number of pending workloads\n")
	b.WriteString("# TYPE orch_workloads_pending gauge\n")
	b.WriteString(fmt.Sprintf("orch_workloads_pending %d\n", pending))
	b.WriteString("# HELP orch_workloads_failed Number of failed workloads\n")
	b.WriteString("# TYPE orch_workloads_failed gauge\n")
	b.WriteString(fmt.Sprintf("orch_workloads_failed %d\n", failed))
	b.WriteString("# HELP orch_autoscaler_policies_total Total number of autoscaler policies\n")
	b.WriteString("# TYPE orch_autoscaler_policies_total gauge\n")
	b.WriteString(fmt.Sprintf("orch_autoscaler_policies_total %d\n", len(policies)))
	return b.String(), nil
}

func (o *Orchestrator) SubscribeEvents(buffer int) (<-chan model.Event, func()) {
	if buffer <= 0 {
		buffer = 32
	}
	ch := make(chan model.Event, buffer)
	o.subsMu.Lock()
	o.subscribers[ch] = struct{}{}
	o.subsMu.Unlock()

	cleanup := func() {
		o.subsMu.Lock()
		if _, ok := o.subscribers[ch]; ok {
			delete(o.subscribers, ch)
			close(ch)
		}
		o.subsMu.Unlock()
	}
	return ch, cleanup
}

func (o *Orchestrator) reconcileDeployment(deploymentID string) error {
	o.mu.Lock()
	defer o.mu.Unlock()

	dep, err := o.store.GetDeployment(deploymentID)
	if err != nil {
		return err
	}
	dep.Spec.Rollout = normalizeRolloutSpec(dep.Spec.Rollout)
	if dep.CurrentRevision <= 0 {
		dep.CurrentRevision = 1
	}
	if isRolloutInProgress(dep.RolloutStatus.Phase) && dep.Spec.Rollout.ProgressDeadlineSeconds > 0 && !dep.RolloutStatus.StartedAt.IsZero() {
		deadline := dep.RolloutStatus.StartedAt.Add(time.Duration(dep.Spec.Rollout.ProgressDeadlineSeconds) * time.Second)
		if time.Now().UTC().After(deadline) {
			dep.Status = model.DeploymentFailed
			dep.RolloutStatus.Phase = "Failed"
			dep.RolloutStatus.Message = "rollout progress deadline exceeded"
			dep.RolloutStatus.LastUpdatedAt = time.Now().UTC()
			dep.Conditions = setCondition(dep.Conditions, model.DeploymentCondition{Type: model.ConditionProgressing, Status: false, Reason: "ProgressDeadlineExceeded", Message: dep.RolloutStatus.Message, LastTransitionTime: time.Now().UTC()})
			dep.Conditions = setCondition(dep.Conditions, model.DeploymentCondition{Type: model.ConditionDegraded, Status: true, Reason: "ProgressDeadlineExceeded", Message: dep.RolloutStatus.Message, LastTransitionTime: time.Now().UTC()})
			if dep.Spec.Rollout.AutoRollbackOnFailure && dep.CurrentRevision > 1 {
				if o.rollbackDeploymentLocked(&dep, dep.CurrentRevision-1, "Automatic rollback after progress deadline exceeded") {
					_ = o.emitEvent(model.Event{Level: model.EventWarn, Type: "rollout", Reason: "AutoRollbackTriggered", Message: "Automatic rollback triggered by progress deadline", DeploymentID: dep.ID})
				}
			}
			dep.UpdatedAt = time.Now().UTC()
			_ = o.emitEvent(model.Event{Level: model.EventWarn, Type: "rollout", Reason: "ProgressDeadlineExceeded", Message: dep.RolloutStatus.Message, DeploymentID: dep.ID})
			return o.store.UpdateDeployment(dep)
		}
	}

	workloads, err := o.store.ListWorkloadsByDeployment(deploymentID)
	if err != nil {
		return err
	}

	active := activeWorkloads(workloads)
	newActive := activeForRevision(active, dep.CurrentRevision)
	oldActive := len(active) - len(newActive)

	targetNewReplicas := dep.Spec.Replicas
	rolloutPhase := "Progressing"
	if dep.Spec.Rollout.Strategy == model.RolloutStrategyCanary && dep.Spec.Rollout.Canary.Enabled && oldActive > 0 {
		canaryTarget := (dep.Spec.Replicas * dep.Spec.Rollout.Canary.Percentage) / 100
		if canaryTarget <= 0 {
			canaryTarget = 1
		}
		if canaryTarget > dep.Spec.Replicas {
			canaryTarget = dep.Spec.Replicas
		}
		targetNewReplicas = canaryTarget
		rolloutPhase = "Canary"
		if runningCount(activeForRevision(workloads, dep.CurrentRevision)) >= canaryTarget {
			targetNewReplicas = dep.Spec.Replicas
			rolloutPhase = "CanaryPromoting"
		}
	}

	maxSurge := dep.Spec.Rollout.RollingUpdate.MaxSurge
	maxUnavailable := dep.Spec.Rollout.RollingUpdate.MaxUnavailable
	maxTotal := dep.Spec.Replicas + maxSurge
	if maxTotal < dep.Spec.Replicas {
		maxTotal = dep.Spec.Replicas
	}
	minAvailable := dep.Spec.Replicas - maxUnavailable
	if minAvailable < 0 {
		minAvailable = 0
	}

	totalActive := len(active)
	newCount := len(newActive)
	createNeeded := targetNewReplicas - newCount
	if createNeeded < 0 {
		createNeeded = 0
	}
	createBudget := maxTotal - totalActive
	if createBudget < 0 {
		createBudget = 0
	}
	toCreate := minInt(createNeeded, createBudget)

	for i := 0; i < toCreate; i++ {
		if err := o.createWorkloadForDeployment(dep); err != nil {
			dep.Status = model.DeploymentPending
			dep.Conditions = setCondition(dep.Conditions, model.DeploymentCondition{Type: model.ConditionProgressing, Status: true, Reason: "NoSchedulableNode", Message: err.Error(), LastTransitionTime: time.Now().UTC()})
			dep.Conditions = setCondition(dep.Conditions, model.DeploymentCondition{Type: model.ConditionDegraded, Status: true, Reason: "SchedulingFailed", Message: "Scheduler could not place workload", LastTransitionTime: time.Now().UTC()})
			dep.RolloutStatus.Phase = rolloutPhase
			dep.RolloutStatus.Message = "waiting for schedulable nodes"
			dep.RolloutStatus.LastUpdatedAt = time.Now().UTC()
			dep.UpdatedAt = time.Now().UTC()
			_ = o.store.UpdateDeployment(dep)
			_ = o.emitEvent(model.Event{Level: model.EventWarn, Type: "scheduler", Reason: "NoSchedulableNode", Message: "Scheduler could not place workload", DeploymentID: dep.ID})
			return nil
		}
	}

	workloads, err = o.store.ListWorkloadsByDeployment(deploymentID)
	if err != nil {
		return err
	}
	active = activeWorkloads(workloads)
	totalActive = len(active)
	oldCandidates := oldRevisionWorkloads(active, dep.CurrentRevision)

	if rolloutPhase != "Canary" {
		deleteBudget := totalActive - minAvailable
		if deleteBudget > 0 {
			toDelete := minInt(len(oldCandidates), deleteBudget)
			for _, victim := range pickScaleDownVictims(oldCandidates, toDelete) {
				if err := o.enqueueDeleteForWorkload(dep, victim); err != nil {
					return err
				}
			}
		}

		workloads, err = o.store.ListWorkloadsByDeployment(deploymentID)
		if err != nil {
			return err
		}
		activeNow := activeWorkloads(workloads)
		excess := len(activeNow) - dep.Spec.Replicas
		if excess > 0 {
			for _, victim := range pickScaleDownVictims(activeNow, excess) {
				if err := o.enqueueDeleteForWorkload(dep, victim); err != nil {
					return err
				}
			}
		}
	}

	active = activeWorkloadsAfterRefresh(o.store, dep.ID)
	updatedReplicas := len(activeForRevision(active, dep.CurrentRevision))
	readyReplicas := runningCountFromSlice(active)
	unavailable := dep.Spec.Replicas - readyReplicas
	if unavailable < 0 {
		unavailable = 0
	}
	remainingOld := len(oldRevisionWorkloads(active, dep.CurrentRevision))
	stable := remainingOld == 0 && updatedReplicas >= dep.Spec.Replicas && unavailable == 0

	if stable {
		dep.Status = model.DeploymentRunning
		dep.RolloutStatus.Phase = "Stable"
		dep.RolloutStatus.Message = "rollout complete"
		dep.Conditions = setCondition(dep.Conditions, model.DeploymentCondition{Type: model.ConditionAvailable, Status: true, Reason: "DesiredReplicasPlaced", Message: "Desired replica count has been placed", LastTransitionTime: time.Now().UTC()})
		dep.Conditions = setCondition(dep.Conditions, model.DeploymentCondition{Type: model.ConditionProgressing, Status: false, Reason: "ReconciliationStable", Message: "Reconciliation reached stable state", LastTransitionTime: time.Now().UTC()})
		dep.Conditions = setCondition(dep.Conditions, model.DeploymentCondition{Type: model.ConditionDegraded, Status: false, Reason: "Healthy", Message: "Deployment is healthy", LastTransitionTime: time.Now().UTC()})
	} else {
		dep.Status = model.DeploymentPending
		dep.RolloutStatus.Phase = rolloutPhase
		dep.RolloutStatus.Message = fmt.Sprintf("rollout in progress: updated=%d ready=%d unavailable=%d old=%d", updatedReplicas, readyReplicas, unavailable, remainingOld)
		dep.Conditions = setCondition(dep.Conditions, model.DeploymentCondition{Type: model.ConditionProgressing, Status: true, Reason: "RolloutInProgress", Message: dep.RolloutStatus.Message, LastTransitionTime: time.Now().UTC()})
		availableNow := readyReplicas >= dep.Spec.Replicas && remainingOld == 0
		dep.Conditions = setCondition(dep.Conditions, model.DeploymentCondition{Type: model.ConditionAvailable, Status: availableNow, Reason: "ReplicaReadiness", Message: "Availability evaluated", LastTransitionTime: time.Now().UTC()})
		if availableNow {
			dep.Status = model.DeploymentRunning
			dep.RolloutStatus.Phase = "Stable"
			dep.RolloutStatus.Message = "rollout complete"
			dep.Conditions = setCondition(dep.Conditions, model.DeploymentCondition{Type: model.ConditionProgressing, Status: false, Reason: "ReconciliationStable", Message: "Reconciliation reached stable state", LastTransitionTime: time.Now().UTC()})
			dep.Conditions = setCondition(dep.Conditions, model.DeploymentCondition{Type: model.ConditionDegraded, Status: false, Reason: "Healthy", Message: "Deployment is healthy", LastTransitionTime: time.Now().UTC()})
		}
	}
	dep.ObservedGeneration = dep.Generation
	dep.RolloutStatus.UpdatedReplicas = updatedReplicas
	dep.RolloutStatus.ReadyReplicas = readyReplicas
	dep.RolloutStatus.UnavailableReplicas = unavailable
	dep.RolloutStatus.LastUpdatedAt = time.Now().UTC()
	dep.UpdatedAt = time.Now().UTC()
	_ = o.emitEvent(model.Event{Level: model.EventInfo, Type: "rollout", Reason: "RolloutProgress", Message: dep.RolloutStatus.Message, DeploymentID: dep.ID})
	return o.store.UpdateDeployment(dep)
}

func (o *Orchestrator) reconcileDeletion(deploymentID string) error {
	o.mu.Lock()
	defer o.mu.Unlock()

	dep, err := o.store.GetDeployment(deploymentID)
	if err != nil {
		return err
	}
	workloads, err := o.store.ListWorkloadsByDeployment(deploymentID)
	if err != nil {
		return err
	}

	for _, w := range workloads {
		if w.Status == model.WorkloadTerminating || w.Status == model.WorkloadTerminated {
			continue
		}
		w.Status = model.WorkloadTerminating
		if err := o.store.UpsertWorkload(w); err != nil {
			return err
		}
		assignment := model.Assignment{
			Action:      "delete",
			NodeID:      w.NodeID,
			WorkloadID:  w.ID,
			ContainerID: w.ContainerID,
			Resources:   w.Resources,
		}
		if err := o.store.EnqueueAssignment(assignment); err != nil {
			return err
		}
		_ = o.emitEvent(model.Event{Level: model.EventInfo, Type: "reconcile", Reason: "CleanupAssignmentEnqueued", Message: "Enqueued workload cleanup assignment", DeploymentID: dep.ID, WorkloadID: w.ID, NodeID: w.NodeID})
	}

	remaining := 0
	for _, w := range workloads {
		if w.Status != model.WorkloadTerminated {
			remaining++
		}
	}
	if remaining > 0 {
		return nil
	}

	dep.Finalizers = removeFinalizer(dep.Finalizers, "workloads.cleanup")
	if len(dep.Finalizers) > 0 {
		if err := o.store.UpdateDeployment(dep); err != nil {
			return err
		}
		return nil
	}

	if err := o.store.DeleteDeployment(dep.ID); err != nil {
		return err
	}
	_ = o.emitEvent(model.Event{Level: model.EventInfo, Type: "reconcile", Reason: "DeploymentDeleted", Message: "Deployment fully deleted after finalizers completed", DeploymentID: dep.ID})
	return nil
}

func (o *Orchestrator) updateNodeHealth(_ context.Context) error {
	nodes, err := o.store.ListNodes()
	if err != nil {
		return err
	}
	now := time.Now().UTC()
	for _, n := range nodes {
		age := now.Sub(n.LastSeen)
		missed := int(age / o.nodeTTL)
		cpuUtil := safeUtilization(n.Used.MilliCPU, n.Allocatable.MilliCPU)
		memUtil := safeUtilization(n.Used.MemoryMB, n.Allocatable.MemoryMB)

		n.Health.CPUUtilization = cpuUtil
		n.Health.MemoryUtilization = memUtil
		n.Health.LastHeartbeatAgeSec = int64(age.Seconds())
		n.Health.ConsecutiveMissedTTL = missed
		n.Health.LastEvaluatedAt = now

		prevClass := n.Health.FailureClass
		switch {
		case missed >= 2:
			n.Health.FailureClass = model.NodeFailureCritical
			n.Health.Reason = "heartbeat missed for multiple TTL intervals"
			n.Health.Isolated = true
			n.Health.Draining = true
			n.Status = model.NodeDown
		case missed >= 1 || cpuUtil > 0.90 || memUtil > 0.90:
			n.Health.FailureClass = model.NodeFailureWarning
			if missed >= 1 {
				n.Health.Reason = "heartbeat delay exceeds TTL"
				n.Status = model.NodeUnknown
			} else {
				n.Health.Reason = "node resource pressure above threshold"
				n.Status = model.NodeReady
			}
		default:
			n.Health.FailureClass = model.NodeFailureHealthy
			n.Health.Reason = ""
			n.Health.Isolated = false
			n.Health.Draining = false
			n.Status = model.NodeReady
		}

		if err := o.store.UpsertNode(n); err != nil {
			return err
		}
		o.recordNodeHealthSample(n)

		if n.Health.FailureClass == model.NodeFailureCritical && prevClass != model.NodeFailureCritical {
			if err := o.remediateUnhealthyNode(n.ID, n.Health.Reason); err != nil {
				return err
			}
			_ = o.emitEvent(model.Event{Level: model.EventWarn, Type: "node", Reason: "NodeIsolatedAndDraining", Message: "Node isolated and draining started", NodeID: n.ID})
		} else if n.Health.FailureClass == model.NodeFailureWarning {
			_ = o.emitEvent(model.Event{Level: model.EventWarn, Type: "node", Reason: "NodeWarning", Message: n.Health.Reason, NodeID: n.ID})
		}
	}
	return nil
}

func (o *Orchestrator) remediateUnhealthyNode(nodeID, reason string) error {
	workloads, err := o.store.ListWorkloads()
	if err != nil {
		return err
	}
	for _, w := range workloads {
		if w.NodeID != nodeID {
			continue
		}
		if w.Status != model.WorkloadRunning && w.Status != model.WorkloadPending {
			continue
		}
		w.Status = model.WorkloadTerminating
		if err := o.store.UpsertWorkload(w); err != nil {
			return err
		}
		assignment := model.Assignment{
			Action:      "delete",
			NodeID:      w.NodeID,
			WorkloadID:  w.ID,
			ContainerID: w.ContainerID,
			Resources:   w.Resources,
		}
		if err := o.store.EnqueueAssignment(assignment); err != nil {
			return err
		}
		_ = o.emitEvent(model.Event{Level: model.EventWarn, Type: "health", Reason: "NodeRemediationTriggered", Message: "Workload marked terminating due to node remediation: " + reason, DeploymentID: w.DeploymentID, WorkloadID: w.ID, NodeID: w.NodeID})
	}
	return nil
}

func (o *Orchestrator) reconcileAutoscaling() error {
	policies, err := o.store.ListAutoscalerPolicies()
	if err != nil {
		return err
	}
	now := time.Now().UTC()
	for _, p := range policies {
		dep, err := o.store.GetDeployment(p.DeploymentID)
		if err != nil {
			continue
		}
		samples, err := o.store.ListDeploymentMetrics(dep.ID, 200)
		if err != nil || len(samples) == 0 {
			continue
		}
		windowStart := now.Add(-time.Duration(p.StabilizationWindowSec) * time.Second)
		windowSamples := make([]model.DeploymentMetricSample, 0, len(samples))
		for _, s := range samples {
			if s.Timestamp.After(windowStart) || s.Timestamp.Equal(windowStart) {
				windowSamples = append(windowSamples, s)
			}
		}
		if len(windowSamples) == 0 {
			windowSamples = samples
		}

		avgCPU, avgMem, avgCustom := aggregateMetrics(windowSamples, p.CustomMetricName)
		if p.PredictiveScalingEnabled {
			predictiveCPU, predictiveMem, predictiveCustom := predictMetrics(windowSamples, p.CustomMetricName, p.PredictiveLookbackSamples, p.PredictiveScaleFactor)
			if predictiveCPU > avgCPU {
				avgCPU = predictiveCPU
			}
			if predictiveMem > avgMem {
				avgMem = predictiveMem
			}
			if predictiveCustom > avgCustom {
				avgCustom = predictiveCustom
			}
		}
		current := dep.Spec.Replicas
		recommended := current

		if p.TargetCPUUtilization > 0 && avgCPU > 0 {
			candidate := int(math.Ceil(float64(current) * (avgCPU / p.TargetCPUUtilization)))
			recommended = maxInt(recommended, candidate)
			if avgCPU < p.TargetCPUUtilization*0.65 {
				recommended = minInt(recommended, current-1)
			}
		}
		if p.TargetMemoryUtilization > 0 && avgMem > 0 {
			candidate := int(math.Ceil(float64(current) * (avgMem / p.TargetMemoryUtilization)))
			recommended = maxInt(recommended, candidate)
			if avgMem < p.TargetMemoryUtilization*0.65 {
				recommended = minInt(recommended, current-1)
			}
		}
		if strings.TrimSpace(p.CustomMetricName) != "" && p.TargetCustomMetricValue > 0 && avgCustom > 0 {
			candidate := int(math.Ceil(float64(current) * (avgCustom / p.TargetCustomMetricValue)))
			recommended = maxInt(recommended, candidate)
		}

		recommended = maxInt(p.MinReplicas, minInt(p.MaxReplicas, recommended))
		if recommended == current {
			continue
		}

		direction := "down"
		cooldown := time.Duration(p.ScaleDownCooldownSec) * time.Second
		if recommended > current {
			direction = "up"
			cooldown = time.Duration(p.ScaleUpCooldownSec) * time.Second
		}
		if !p.LastScaleAt.IsZero() && now.Sub(p.LastScaleAt) < cooldown {
			continue
		}

		dep.Spec.Replicas = recommended
		dep.Generation++
		dep.Status = model.DeploymentPending
		dep.RolloutStatus.Phase = "Autoscaling"
		dep.RolloutStatus.Message = fmt.Sprintf("autoscaler adjusted replicas from %d to %d", current, recommended)
		if dep.RolloutStatus.StartedAt.IsZero() {
			dep.RolloutStatus.StartedAt = now
		}
		dep.RolloutStatus.LastUpdatedAt = now
		dep.Conditions = setCondition(dep.Conditions, model.DeploymentCondition{
			Type:               model.ConditionProgressing,
			Status:             true,
			Reason:             "AutoscalerAdjusting",
			Message:            dep.RolloutStatus.Message,
			LastTransitionTime: now,
		})
		dep.UpdatedAt = now
		if err := o.store.UpdateDeployment(dep); err != nil {
			return err
		}

		p.LastScaleAt = now
		p.LastScaleDirection = direction
		p.UpdatedAt = now
		if err := o.store.UpsertAutoscalerPolicy(p); err != nil {
			return err
		}

		reason := "AutoscalerScaleDown"
		if direction == "up" {
			reason = "AutoscalerScaleUp"
		}
		_ = o.emitEvent(model.Event{Level: model.EventInfo, Type: "autoscaler", Reason: reason, Message: fmt.Sprintf("autoscaler adjusted replicas %d -> %d (cpu=%.2f mem=%.2f custom=%.2f)", current, recommended, avgCPU, avgMem, avgCustom), DeploymentID: dep.ID})
	}
	return nil
}

func aggregateMetrics(samples []model.DeploymentMetricSample, customMetric string) (float64, float64, float64) {
	if len(samples) == 0 {
		return 0, 0, 0
	}
	var cpu, mem, custom float64
	for _, s := range samples {
		cpu += s.CPUUsage
		mem += s.MemoryUsage
		if customMetric != "" {
			custom += s.Custom[customMetric]
		}
	}
	count := float64(len(samples))
	return cpu / count, mem / count, custom / count
}

func (o *Orchestrator) reconcileAllServiceEndpoints() error {
	services, err := o.store.ListServices()
	if err != nil {
		return err
	}
	for _, svc := range services {
		if err := o.reconcileServiceEndpoints(svc.ID); err != nil {
			return err
		}
	}
	return nil
}

func (o *Orchestrator) reconcileServiceEndpoints(serviceID string) error {
	svc, err := o.store.GetService(serviceID)
	if err != nil {
		return err
	}
	deployments, err := o.store.ListDeployments()
	if err != nil {
		return err
	}
	workloads, err := o.store.ListWorkloads()
	if err != nil {
		return err
	}
	nodes, err := o.store.ListNodes()
	if err != nil {
		return err
	}
	nodeAddress := map[string]string{}
	for _, n := range nodes {
		nodeAddress[n.ID] = n.Address
	}
	depByID := map[string]model.Deployment{}
	for _, d := range deployments {
		depByID[d.ID] = d
	}
	endpoints := []model.ServiceEndpoint{}
	now := time.Now().UTC()
	for _, w := range workloads {
		if w.Status != model.WorkloadRunning && w.Status != model.WorkloadPending {
			continue
		}
		dep, ok := depByID[w.DeploymentID]
		if !ok {
			continue
		}
		if strings.TrimSpace(dep.Spec.Namespace) != strings.TrimSpace(svc.Spec.Namespace) {
			continue
		}
		if !labelsMatchSelector(dep.Spec.Labels, svc.Spec.Selector) {
			continue
		}
		for _, p := range svc.Spec.Ports {
			addr := nodeAddress[w.NodeID]
			if strings.TrimSpace(addr) == "" {
				addr = w.NodeID
			}
			ready := w.Status == model.WorkloadRunning
			if dep.Spec.ReadinessProbe.Enabled {
				delay := time.Duration(dep.Spec.ReadinessProbe.InitialDelaySeconds) * time.Second
				ready = ready && time.Since(w.CreatedAt) >= delay
			}
			endpoints = append(endpoints, model.ServiceEndpoint{
				ID:         fmt.Sprintf("%s-%s-%d", svc.ID, w.ID, p.TargetPort),
				ServiceID:  svc.ID,
				WorkloadID: w.ID,
				NodeID:     w.NodeID,
				Address:    addr,
				Port:       p.TargetPort,
				Protocol:   p.Protocol,
				Ready:      ready,
				UpdatedAt:  now,
			})
		}
	}
	if err := o.store.ReplaceServiceEndpoints(svc.ID, endpoints); err != nil {
		return err
	}
	_ = o.emitEvent(model.Event{Level: model.EventInfo, Type: "service", Reason: "EndpointsUpdated", Message: "Service endpoints updated", WorkloadID: svc.ID})
	return nil
}

func labelsMatchSelector(labels map[string]string, selector map[string]string) bool {
	if len(selector) == 0 {
		return false
	}
	for key, value := range selector {
		if labels[key] != value {
			return false
		}
	}
	return true
}

func (o *Orchestrator) findServiceByName(name string) (model.Service, error) {
	services, err := o.store.ListServices()
	if err != nil {
		return model.Service{}, err
	}
	for _, svc := range services {
		if strings.EqualFold(strings.TrimSpace(svc.Spec.Name), strings.TrimSpace(name)) {
			return svc, nil
		}
	}
	return model.Service{}, store.ErrNotFound
}

func (o *Orchestrator) evaluateWorkloadHealth() error {
	workloads, err := o.store.ListWorkloads()
	if err != nil {
		return err
	}
	deployments, err := o.store.ListDeployments()
	if err != nil {
		return err
	}
	depByID := map[string]model.Deployment{}
	for _, d := range deployments {
		depByID[d.ID] = d
	}
	now := time.Now().UTC()
	for _, w := range workloads {
		dep, ok := depByID[w.DeploymentID]
		if !ok {
			continue
		}
		if dep.Spec.LivenessProbe.Enabled && w.Status == model.WorkloadRunning {
			delay := time.Duration(dep.Spec.LivenessProbe.InitialDelaySeconds) * time.Second
			if now.Sub(w.CreatedAt) >= delay && strings.TrimSpace(w.ContainerID) == "" {
				w.Status = model.WorkloadFailed
				w.RestartCount++
				if err := o.store.UpsertWorkload(w); err != nil {
					return err
				}
				_ = o.emitEvent(model.Event{Level: model.EventWarn, Type: "health", Reason: "LivenessProbeFailed", Message: "Workload failed liveness probe", DeploymentID: w.DeploymentID, WorkloadID: w.ID, NodeID: w.NodeID})
				o.maybeAutoRollbackForFailures(depByID[w.DeploymentID])
			}
		}
	}
	return nil
}

func (o *Orchestrator) maybeAutoRollbackForFailures(dep model.Deployment) {
	if !dep.Spec.Rollout.AutoRollbackOnFailure || dep.CurrentRevision <= 1 {
		return
	}
	threshold := dep.Spec.Rollout.MaxFailedWorkloads
	if threshold <= 0 {
		threshold = 1
	}
	workloads, err := o.store.ListWorkloadsByDeployment(dep.ID)
	if err != nil {
		return
	}
	failed := 0
	for _, w := range workloads {
		if w.Version == dep.CurrentRevision && w.Status == model.WorkloadFailed {
			failed++
		}
	}
	if failed < threshold {
		return
	}
	if _, err := o.RollbackDeployment(context.Background(), dep.ID, dep.CurrentRevision-1); err == nil {
		_ = o.emitEvent(model.Event{Level: model.EventWarn, Type: "rollout", Reason: "AutoRollbackTriggered", Message: fmt.Sprintf("Automatic rollback triggered after %d failed workload(s)", failed), DeploymentID: dep.ID})
	}
}

func allocateClusterIP(seed int64) string {
	v := seed % 65534
	octet3 := (v / 254) % 254
	octet4 := (v % 254) + 1
	return fmt.Sprintf("10.96.%d.%d", octet3+1, octet4)
}

func safeUtilization(used, alloc int64) float64 {
	if alloc <= 0 {
		return 0
	}
	v := float64(used) / float64(alloc)
	if v < 0 {
		return 0
	}
	if v > 1 {
		return 1
	}
	return v
}

func normalizeRolloutSpec(in model.RolloutSpec) model.RolloutSpec {
	if in.Strategy == "" {
		in.Strategy = model.RolloutStrategyRollingUpdate
	}
	if in.ProgressDeadlineSeconds < 0 {
		in.ProgressDeadlineSeconds = 0
	}
	if in.MaxFailedWorkloads < 0 {
		in.MaxFailedWorkloads = 0
	}
	if in.SLOErrorRateThreshold < 0 {
		in.SLOErrorRateThreshold = 0
	}
	if in.SLOWindowSeconds < 0 {
		in.SLOWindowSeconds = 0
	}
	if in.RollingUpdate.MaxSurge < 0 {
		in.RollingUpdate.MaxSurge = 0
	}
	if in.RollingUpdate.MaxUnavailable < 0 {
		in.RollingUpdate.MaxUnavailable = 0
	}
	if in.Strategy == model.RolloutStrategyCanary {
		if in.Canary.Percentage <= 0 {
			in.Canary.Percentage = 20
		}
		if in.Canary.Percentage > 100 {
			in.Canary.Percentage = 100
		}
		in.Canary.Enabled = true
	}
	return in
}

func (o *Orchestrator) ensureNamespaceExists(namespace string) error {
	namespace = strings.TrimSpace(namespace)
	if namespace == "" {
		namespace = "default"
	}
	if _, err := o.store.GetNamespace(namespace); err == nil {
		return nil
	}
	ns := model.Namespace{Name: namespace, Tenant: "default", CreatedAt: time.Now().UTC(), UpdatedAt: time.Now().UTC()}
	if err := o.store.CreateNamespace(ns); err != nil {
		if _, getErr := o.store.GetNamespace(namespace); getErr == nil {
			return nil
		}
		return err
	}
	return nil
}

func (o *Orchestrator) enforceNamespaceQuota(spec model.DeploymentSpec) error {
	quota, err := o.store.GetNamespaceQuota(spec.Namespace)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil
		}
		return err
	}
	if quota.MaxDeployments == 0 && quota.MaxMilliCPU == 0 && quota.MaxMemoryMB == 0 {
		return nil
	}
	deps, err := o.store.ListDeployments()
	if err != nil {
		return err
	}
	count := 0
	var usedCPU int64
	var usedMem int64
	for _, d := range deps {
		if d.Spec.Namespace != spec.Namespace {
			continue
		}
		count++
		usedCPU += int64(d.Spec.Replicas) * d.Spec.Resources.MilliCPU
		usedMem += int64(d.Spec.Replicas) * d.Spec.Resources.MemoryMB
	}
	newCPU := usedCPU + int64(spec.Replicas)*spec.Resources.MilliCPU
	newMem := usedMem + int64(spec.Replicas)*spec.Resources.MemoryMB
	if quota.MaxDeployments > 0 && count+1 > quota.MaxDeployments {
		return fmt.Errorf("namespace quota exceeded: max deployments %d", quota.MaxDeployments)
	}
	if quota.MaxMilliCPU > 0 && newCPU > quota.MaxMilliCPU {
		return fmt.Errorf("namespace quota exceeded: max milliCPU %d", quota.MaxMilliCPU)
	}
	if quota.MaxMemoryMB > 0 && newMem > quota.MaxMemoryMB {
		return fmt.Errorf("namespace quota exceeded: max memoryMB %d", quota.MaxMemoryMB)
	}
	return nil
}

func isRolloutInProgress(phase string) bool {
	switch phase {
	case "Progressing", "Canary", "CanaryPromoting", "Autoscaling", "Rollback":
		return true
	default:
		return false
	}
}

func (o *Orchestrator) rollbackDeploymentLocked(dep *model.Deployment, targetRevision int, message string) bool {
	var target *model.DeploymentRevision
	for i := range dep.RevisionHistory {
		if dep.RevisionHistory[i].Revision == targetRevision {
			target = &dep.RevisionHistory[i]
			break
		}
	}
	if target == nil {
		return false
	}
	dep.Spec = target.Spec
	dep.Spec.Rollout = normalizeRolloutSpec(dep.Spec.Rollout)
	dep.Generation++
	dep.CurrentRevision++
	now := time.Now().UTC()
	dep.RevisionHistory = append(dep.RevisionHistory, model.DeploymentRevision{Revision: dep.CurrentRevision, Spec: dep.Spec, CreatedAt: now})
	dep.Status = model.DeploymentPending
	dep.RolloutStatus.Phase = "Rollback"
	dep.RolloutStatus.Message = message
	dep.RolloutStatus.StartedAt = now
	dep.RolloutStatus.LastUpdatedAt = now
	dep.Conditions = setCondition(dep.Conditions, model.DeploymentCondition{Type: model.ConditionProgressing, Status: true, Reason: "RollbackStarted", Message: message, LastTransitionTime: now})
	return true
}

func predictMetrics(samples []model.DeploymentMetricSample, customMetric string, lookback int, factor float64) (float64, float64, float64) {
	ordered := append([]model.DeploymentMetricSample(nil), samples...)
	sort.SliceStable(ordered, func(i, j int) bool {
		return ordered[i].Timestamp.Before(ordered[j].Timestamp)
	})
	samples = ordered
	if lookback <= 1 {
		lookback = 2
	}
	if factor <= 0 {
		factor = 1
	}
	if len(samples) > lookback {
		samples = samples[len(samples)-lookback:]
	}
	if len(samples) < 2 {
		return 0, 0, 0
	}
	first := samples[0]
	last := samples[len(samples)-1]
	steps := float64(len(samples) - 1)
	cpuSlope := (last.CPUUsage - first.CPUUsage) / steps
	memSlope := (last.MemoryUsage - first.MemoryUsage) / steps
	predCPU := last.CPUUsage + cpuSlope*factor
	predMem := last.MemoryUsage + memSlope*factor
	predCustom := 0.0
	if customMetric != "" {
		firstCustom := first.Custom[customMetric]
		lastCustom := last.Custom[customMetric]
		customSlope := (lastCustom - firstCustom) / steps
		predCustom = lastCustom + customSlope*factor
	}
	if predCPU < 0 {
		predCPU = 0
	}
	if predMem < 0 {
		predMem = 0
	}
	if predCustom < 0 {
		predCustom = 0
	}
	return predCPU, predMem, predCustom
}

func (o *Orchestrator) recordNodeHealthSample(node model.Node) {
	o.healthMu.Lock()
	defer o.healthMu.Unlock()
	samples := o.healthSamples[node.ID]
	samples = append(samples, model.NodeHealthSample{
		NodeID:            node.ID,
		Timestamp:         node.Health.LastEvaluatedAt,
		CPUUtilization:    node.Health.CPUUtilization,
		MemoryUtilization: node.Health.MemoryUtilization,
		FailureClass:      node.Health.FailureClass,
	})
	if len(samples) > 256 {
		samples = samples[len(samples)-256:]
	}
	o.healthSamples[node.ID] = samples
}

func activeWorkloads(in []model.Workload) []model.Workload {
	out := make([]model.Workload, 0, len(in))
	for _, w := range in {
		if w.Status == model.WorkloadRunning || w.Status == model.WorkloadPending {
			out = append(out, w)
		}
	}
	return out
}

func runningCount(in []model.Workload) int {
	c := 0
	for _, w := range in {
		if w.Status == model.WorkloadRunning {
			c++
		}
	}
	return c
}

func runningCountFromSlice(in []model.Workload) int {
	c := 0
	for _, w := range in {
		if w.Status == model.WorkloadRunning {
			c++
		}
	}
	return c
}

func activeForRevision(in []model.Workload, rev int) []model.Workload {
	out := make([]model.Workload, 0, len(in))
	for _, w := range in {
		if w.Version == rev {
			out = append(out, w)
		}
	}
	return out
}

func oldRevisionWorkloads(in []model.Workload, rev int) []model.Workload {
	out := make([]model.Workload, 0, len(in))
	for _, w := range in {
		if w.Version != rev {
			out = append(out, w)
		}
	}
	return out
}

func activeWorkloadsAfterRefresh(st store.StateStore, deploymentID string) []model.Workload {
	items, err := st.ListWorkloadsByDeployment(deploymentID)
	if err != nil {
		return nil
	}
	return activeWorkloads(items)
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func (o *Orchestrator) createWorkloadForDeployment(dep model.Deployment) error {
	nodes, err := o.store.ListNodes()
	if err != nil {
		return err
	}
	node, err := o.scheduler.SelectNode(nodes, dep.Spec.Resources, dep.Spec.Labels, dep.Spec.Tolerations)
	if err != nil {
		preempted := false
		if dep.Spec.Priority > 0 {
			preempted, _ = o.tryPreemptForPriority(dep, dep.Spec.Resources)
		}
		if preempted {
			nodes, err = o.store.ListNodes()
			if err != nil {
				return err
			}
			node, err = o.scheduler.SelectNode(nodes, dep.Spec.Resources, dep.Spec.Labels, dep.Spec.Tolerations)
		}
	}
	if err != nil {
		return err
	}
	_ = o.emitEvent(model.Event{Level: model.EventInfo, Type: "scheduler", Reason: "NodeSelected", Message: "Scheduler selected node for workload", DeploymentID: dep.ID, NodeID: node.ID})

	workload := model.Workload{
		ID:           fmt.Sprintf("wk-%d-%04d", time.Now().UnixNano(), rand.Intn(9999)),
		DeploymentID: dep.ID,
		Priority:     dep.Spec.Priority,
		NodeID:       node.ID,
		Image:        dep.Spec.Image,
		Resources:    dep.Spec.Resources,
		Status:       model.WorkloadPending,
		Version:      dep.CurrentRevision,
		CreatedAt:    time.Now().UTC(),
		UpdatedAt:    time.Now().UTC(),
	}
	if err := o.store.UpsertWorkload(workload); err != nil {
		return err
	}

	node.Used.MilliCPU += dep.Spec.Resources.MilliCPU
	node.Used.MemoryMB += dep.Spec.Resources.MemoryMB
	if err := o.store.UpsertNode(node); err != nil {
		return err
	}

	secretFiles, err := o.resolveSecretFilesForDeployment(dep)
	if err != nil {
		return err
	}
	volumeMounts := make([]model.VolumeMount, 0, len(dep.Spec.VolumeClaims))
	for _, claim := range dep.Spec.VolumeClaims {
		volumeMounts = append(volumeMounts, model.VolumeMount{ClaimName: claim, MountPath: "/mnt/pv/" + claim, ReadOnly: false})
	}

	assignment := model.Assignment{
		Action:          "create",
		NodeID:          node.ID,
		WorkloadID:      workload.ID,
		Image:           workload.Image,
		ImagePullSecret: dep.Spec.ImagePullSecret,
		SecretFiles:     secretFiles,
		VolumeMounts:    volumeMounts,
		Resources:       workload.Resources,
	}
	if err := o.store.EnqueueAssignment(assignment); err != nil {
		return err
	}
	_ = o.emitEvent(model.Event{Level: model.EventInfo, Type: "reconcile", Reason: "AssignmentEnqueued", Message: "Workload assignment enqueued", DeploymentID: dep.ID, WorkloadID: workload.ID, NodeID: node.ID})
	return nil
}

func (o *Orchestrator) enqueueDeleteForWorkload(dep model.Deployment, victim model.Workload) error {
	victim.Status = model.WorkloadTerminating
	if err := o.store.UpsertWorkload(victim); err != nil {
		return err
	}
	assignment := model.Assignment{
		Action:      "delete",
		NodeID:      victim.NodeID,
		WorkloadID:  victim.ID,
		ContainerID: victim.ContainerID,
		Resources:   victim.Resources,
	}
	if err := o.store.EnqueueAssignment(assignment); err != nil {
		return err
	}
	_ = o.emitEvent(model.Event{Level: model.EventInfo, Type: "rollout", Reason: "OldRevisionScaledDown", Message: "Scaled down old revision workload", DeploymentID: dep.ID, WorkloadID: victim.ID, NodeID: victim.NodeID})
	return nil
}

func runningOrPendingCount(workloads []model.Workload) int {
	count := 0
	for _, w := range workloads {
		if w.Status == model.WorkloadRunning || w.Status == model.WorkloadPending {
			count++
		}
	}
	return count
}

func pickScaleDownVictims(workloads []model.Workload, count int) []model.Workload {
	if count <= 0 {
		return nil
	}
	candidates := make([]model.Workload, 0, len(workloads))
	for _, w := range workloads {
		if w.Status == model.WorkloadPending || w.Status == model.WorkloadRunning {
			candidates = append(candidates, w)
		}
	}
	sort.SliceStable(candidates, func(i, j int) bool {
		if candidates[i].Status != candidates[j].Status {
			return candidates[i].Status == model.WorkloadPending
		}
		return candidates[i].CreatedAt.After(candidates[j].CreatedAt)
	})
	if len(candidates) > count {
		return candidates[:count]
	}
	return candidates
}

func (o *Orchestrator) tryPreemptForPriority(dep model.Deployment, req model.Resource) (bool, error) {
	workloads, err := o.store.ListWorkloads()
	if err != nil {
		return false, err
	}
	nodes, err := o.store.ListNodes()
	if err != nil {
		return false, err
	}
	nodeByID := map[string]model.Node{}
	for _, n := range nodes {
		nodeByID[n.ID] = n
	}

	candidates := make([]model.Workload, 0, len(workloads))
	for _, w := range workloads {
		if w.Priority < dep.Spec.Priority && w.DeploymentID != dep.ID {
			candidates = append(candidates, w)
		}
	}
	sort.SliceStable(candidates, func(i, j int) bool {
		if candidates[i].Priority != candidates[j].Priority {
			return candidates[i].Priority < candidates[j].Priority
		}
		return candidates[i].CreatedAt.After(candidates[j].CreatedAt)
	})

	for _, victim := range candidates {
		node, ok := nodeByID[victim.NodeID]
		if !ok || node.Status != model.NodeReady {
			continue
		}
		availCPU := node.Allocatable.MilliCPU - node.Used.MilliCPU + victim.Resources.MilliCPU
		availMem := node.Allocatable.MemoryMB - node.Used.MemoryMB + victim.Resources.MemoryMB
		if req.MilliCPU > availCPU || req.MemoryMB > availMem {
			continue
		}

		node.Used.MilliCPU -= victim.Resources.MilliCPU
		node.Used.MemoryMB -= victim.Resources.MemoryMB
		if node.Used.MilliCPU < 0 {
			node.Used.MilliCPU = 0
		}
		if node.Used.MemoryMB < 0 {
			node.Used.MemoryMB = 0
		}
		if err := o.store.UpsertNode(node); err != nil {
			return false, err
		}
		if err := o.store.DeleteWorkload(victim.ID); err != nil {
			return false, err
		}
		_ = o.emitEvent(model.Event{
			Level:        model.EventWarn,
			Type:         "scheduler",
			Reason:       "PreemptedLowerPriorityWorkload",
			Message:      "Preempted lower priority workload to free resources",
			DeploymentID: dep.ID,
			WorkloadID:   victim.ID,
			NodeID:       victim.NodeID,
		})
		return true, nil
	}

	return false, nil
}

func (o *Orchestrator) emitEvent(event model.Event) error {
	event.Timestamp = time.Now().UTC()
	event.ID = fmt.Sprintf("evt-%d-%04d", event.Timestamp.UnixNano(), rand.Intn(9999))
	if err := o.store.AppendEvent(event); err != nil {
		return err
	}
	log.Printf("event id=%s level=%s type=%s reason=%s deployment=%s workload=%s node=%s msg=%q", event.ID, event.Level, event.Type, event.Reason, event.DeploymentID, event.WorkloadID, event.NodeID, event.Message)

	o.subsMu.RLock()
	for ch := range o.subscribers {
		select {
		case ch <- event:
		default:
		}
	}
	o.subsMu.RUnlock()
	return nil
}

func setCondition(existing []model.DeploymentCondition, next model.DeploymentCondition) []model.DeploymentCondition {
	out := make([]model.DeploymentCondition, 0, len(existing)+1)
	updated := false
	for _, c := range existing {
		if c.Type == next.Type {
			out = append(out, next)
			updated = true
			continue
		}
		out = append(out, c)
	}
	if !updated {
		out = append(out, next)
	}
	return out
}

func removeFinalizer(finalizers []string, target string) []string {
	out := make([]string, 0, len(finalizers))
	for _, f := range finalizers {
		if f != target {
			out = append(out, f)
		}
	}
	return out
}
