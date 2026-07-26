package store

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/dgraph-io/badger/v4"

	"minikube-orchestrator/internal/model"
)

var ErrNotFound = errors.New("not found")

type StateStore interface {
	UpsertNode(node model.Node) error
	GetNode(id string) (model.Node, error)
	ListNodes() ([]model.Node, error)

	CreateDeployment(dep model.Deployment) error
	DeleteDeployment(id string) error
	GetDeployment(id string) (model.Deployment, error)
	ListDeployments() ([]model.Deployment, error)
	UpdateDeployment(dep model.Deployment) error

	UpsertWorkload(w model.Workload) error
	GetWorkload(id string) (model.Workload, error)
	ListWorkloads() ([]model.Workload, error)
	ListWorkloadsByDeployment(deploymentID string) ([]model.Workload, error)
	DeleteWorkload(id string) error

	CreateService(svc model.Service) error
	GetService(id string) (model.Service, error)
	ListServices() ([]model.Service, error)
	UpdateService(svc model.Service) error
	DeleteService(id string) error

	CreateNamespace(ns model.Namespace) error
	GetNamespace(name string) (model.Namespace, error)
	ListNamespaces() ([]model.Namespace, error)
	DeleteNamespace(name string) error

	UpsertNamespaceQuota(quota model.NamespaceQuota) error
	GetNamespaceQuota(namespace string) (model.NamespaceQuota, error)
	ListNamespaceQuotas() ([]model.NamespaceQuota, error)
	DeleteNamespaceQuota(namespace string) error

	UpsertSecret(secret model.Secret) error
	GetSecret(namespace, name string) (model.Secret, error)
	ListSecrets(namespace string) ([]model.Secret, error)
	DeleteSecret(namespace, name string) error

	UpsertPersistentVolume(pv model.PersistentVolume) error
	GetPersistentVolume(name string) (model.PersistentVolume, error)
	ListPersistentVolumes() ([]model.PersistentVolume, error)
	DeletePersistentVolume(name string) error

	UpsertPersistentVolumeClaim(pvc model.PersistentVolumeClaim) error
	GetPersistentVolumeClaim(namespace, name string) (model.PersistentVolumeClaim, error)
	ListPersistentVolumeClaims(namespace string) ([]model.PersistentVolumeClaim, error)
	DeletePersistentVolumeClaim(namespace, name string) error

	UpsertNetworkPolicy(policy model.NetworkPolicy) error
	GetNetworkPolicy(namespace, name string) (model.NetworkPolicy, error)
	ListNetworkPolicies(namespace string) ([]model.NetworkPolicy, error)
	DeleteNetworkPolicy(namespace, name string) error

	UpsertJob(job model.Job) error
	GetJob(id string) (model.Job, error)
	ListJobs(namespace string) ([]model.Job, error)
	DeleteJob(id string) error

	UpsertCronJob(cj model.CronJob) error
	GetCronJob(namespace, name string) (model.CronJob, error)
	ListCronJobs(namespace string) ([]model.CronJob, error)
	DeleteCronJob(namespace, name string) error

	UpsertRoleBinding(binding model.RoleBinding) error
	GetRoleBinding(namespace, name string) (model.RoleBinding, error)
	ListRoleBindings(namespace string) ([]model.RoleBinding, error)
	DeleteRoleBinding(namespace, name string) error

	UpsertControllerLease(lease model.ControllerLease) error
	GetControllerLease() (model.ControllerLease, error)
	DeleteControllerLease() error

	AppendControlPlaneTransition(t model.ControlPlaneTransition) error
	ListControlPlaneTransitions(limit int) ([]model.ControlPlaneTransition, error)

	ReplaceServiceEndpoints(serviceID string, endpoints []model.ServiceEndpoint) error
	ListServiceEndpoints(serviceID string) ([]model.ServiceEndpoint, error)

	UpsertAutoscalerPolicy(policy model.AutoscalerPolicy) error
	GetAutoscalerPolicy(id string) (model.AutoscalerPolicy, error)
	ListAutoscalerPolicies() ([]model.AutoscalerPolicy, error)
	DeleteAutoscalerPolicy(id string) error

	AppendDeploymentMetric(sample model.DeploymentMetricSample) error
	ListDeploymentMetrics(deploymentID string, limit int) ([]model.DeploymentMetricSample, error)

	EnqueueAssignment(a model.Assignment) error
	PopAssignments(nodeID string, max int) ([]model.Assignment, error)

	AppendEvent(e model.Event) error
	ListEvents(limit int) ([]model.Event, error)

	Snapshot() (model.ClusterState, error)
	Close() error
}

type BadgerStateStore struct {
	db *badger.DB
	mu sync.RWMutex
}

func NewBadgerStateStore(path string) (*BadgerStateStore, error) {
	opts := badger.DefaultOptions(path)
	opts.Logger = nil
	db, err := badger.Open(opts)
	if err != nil {
		return nil, fmt.Errorf("open badger: %w", err)
	}
	return &BadgerStateStore{db: db}, nil
}

func (s *BadgerStateStore) Close() error {
	return s.db.Close()
}

func (s *BadgerStateStore) UpsertNode(node model.Node) error {
	if node.LastSeen.IsZero() {
		node.LastSeen = time.Now().UTC()
	}
	return s.put(keyNode(node.ID), node)
}

func (s *BadgerStateStore) GetNode(id string) (model.Node, error) {
	var node model.Node
	err := s.get(keyNode(id), &node)
	return node, err
}

func (s *BadgerStateStore) ListNodes() ([]model.Node, error) {
	return s.listNodes()
}

func (s *BadgerStateStore) CreateDeployment(dep model.Deployment) error {
	if _, err := s.GetDeployment(dep.ID); err == nil {
		return fmt.Errorf("deployment %s already exists", dep.ID)
	}
	return s.put(keyDeployment(dep.ID), dep)
}

func (s *BadgerStateStore) DeleteDeployment(id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.db.Update(func(txn *badger.Txn) error {
		if err := txn.Delete([]byte(keyDeployment(id))); err != nil {
			return err
		}
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		prefix := []byte("workload:")
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			val, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}
			var w model.Workload
			if err := json.Unmarshal(val, &w); err != nil {
				return err
			}
			if w.DeploymentID == id {
				if err := txn.Delete(item.KeyCopy(nil)); err != nil {
					return err
				}
			}
		}
		return nil
	})
}

func (s *BadgerStateStore) GetDeployment(id string) (model.Deployment, error) {
	var dep model.Deployment
	err := s.get(keyDeployment(id), &dep)
	return dep, err
}

func (s *BadgerStateStore) ListDeployments() ([]model.Deployment, error) {
	return s.listDeployments()
}

func (s *BadgerStateStore) UpdateDeployment(dep model.Deployment) error {
	return s.put(keyDeployment(dep.ID), dep)
}

func (s *BadgerStateStore) UpsertWorkload(w model.Workload) error {
	w.UpdatedAt = time.Now().UTC()
	return s.put(keyWorkload(w.ID), w)
}

func (s *BadgerStateStore) GetWorkload(id string) (model.Workload, error) {
	var w model.Workload
	err := s.get(keyWorkload(id), &w)
	return w, err
}

func (s *BadgerStateStore) ListWorkloads() ([]model.Workload, error) {
	return s.listWorkloads()
}

func (s *BadgerStateStore) ListWorkloadsByDeployment(deploymentID string) ([]model.Workload, error) {
	items, err := s.listWorkloads()
	if err != nil {
		return nil, err
	}
	result := make([]model.Workload, 0, len(items))
	for _, w := range items {
		if w.DeploymentID == deploymentID {
			result = append(result, w)
		}
	}
	return result, nil
}

func (s *BadgerStateStore) DeleteWorkload(id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.db.Update(func(txn *badger.Txn) error {
		return txn.Delete([]byte(keyWorkload(id)))
	})
}

func (s *BadgerStateStore) CreateService(svc model.Service) error {
	if _, err := s.GetService(svc.ID); err == nil {
		return fmt.Errorf("service %s already exists", svc.ID)
	}
	return s.put(keyService(svc.ID), svc)
}

func (s *BadgerStateStore) GetService(id string) (model.Service, error) {
	var svc model.Service
	err := s.get(keyService(id), &svc)
	return svc, err
}

func (s *BadgerStateStore) ListServices() ([]model.Service, error) {
	return listPrefix[model.Service](s, "service:")
}

func (s *BadgerStateStore) UpdateService(svc model.Service) error {
	return s.put(keyService(svc.ID), svc)
}

func (s *BadgerStateStore) DeleteService(id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.db.Update(func(txn *badger.Txn) error {
		if err := txn.Delete([]byte(keyService(id))); err != nil {
			return err
		}
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		prefix := []byte(keyServiceEndpointPrefix(id))
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			if err := txn.Delete(it.Item().KeyCopy(nil)); err != nil {
				return err
			}
		}
		return nil
	})
}

func (s *BadgerStateStore) CreateNamespace(ns model.Namespace) error {
	if _, err := s.GetNamespace(ns.Name); err == nil {
		return fmt.Errorf("namespace %s already exists", ns.Name)
	}
	if ns.CreatedAt.IsZero() {
		ns.CreatedAt = time.Now().UTC()
	}
	if ns.UpdatedAt.IsZero() {
		ns.UpdatedAt = ns.CreatedAt
	}
	return s.put(keyNamespace(ns.Name), ns)
}

func (s *BadgerStateStore) GetNamespace(name string) (model.Namespace, error) {
	var ns model.Namespace
	err := s.get(keyNamespace(name), &ns)
	return ns, err
}

func (s *BadgerStateStore) ListNamespaces() ([]model.Namespace, error) {
	return listPrefix[model.Namespace](s, "namespace:")
}

func (s *BadgerStateStore) DeleteNamespace(name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.db.Update(func(txn *badger.Txn) error {
		return txn.Delete([]byte(keyNamespace(name)))
	})
}

func (s *BadgerStateStore) UpsertNamespaceQuota(quota model.NamespaceQuota) error {
	if quota.CreatedAt.IsZero() {
		quota.CreatedAt = time.Now().UTC()
	}
	quota.UpdatedAt = time.Now().UTC()
	return s.put(keyNamespaceQuota(quota.Namespace), quota)
}

func (s *BadgerStateStore) GetNamespaceQuota(namespace string) (model.NamespaceQuota, error) {
	var quota model.NamespaceQuota
	err := s.get(keyNamespaceQuota(namespace), &quota)
	return quota, err
}

func (s *BadgerStateStore) ListNamespaceQuotas() ([]model.NamespaceQuota, error) {
	return listPrefix[model.NamespaceQuota](s, "quota:")
}

func (s *BadgerStateStore) DeleteNamespaceQuota(namespace string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.db.Update(func(txn *badger.Txn) error {
		return txn.Delete([]byte(keyNamespaceQuota(namespace)))
	})
}

func (s *BadgerStateStore) UpsertSecret(secret model.Secret) error {
	if secret.CreatedAt.IsZero() {
		secret.CreatedAt = time.Now().UTC()
	}
	secret.UpdatedAt = time.Now().UTC()
	return s.put(keySecret(secret.Namespace, secret.Name), secret)
}

func (s *BadgerStateStore) GetSecret(namespace, name string) (model.Secret, error) {
	var secret model.Secret
	err := s.get(keySecret(namespace, name), &secret)
	return secret, err
}

func (s *BadgerStateStore) ListSecrets(namespace string) ([]model.Secret, error) {
	return listPrefix[model.Secret](s, keySecretPrefix(namespace))
}

func (s *BadgerStateStore) DeleteSecret(namespace, name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.db.Update(func(txn *badger.Txn) error {
		return txn.Delete([]byte(keySecret(namespace, name)))
	})
}

func (s *BadgerStateStore) UpsertPersistentVolume(pv model.PersistentVolume) error {
	if pv.CreatedAt.IsZero() {
		pv.CreatedAt = time.Now().UTC()
	}
	pv.UpdatedAt = time.Now().UTC()
	if pv.Phase == "" {
		pv.Phase = model.PersistentVolumeAvailable
	}
	return s.put(keyPersistentVolume(pv.Name), pv)
}

func (s *BadgerStateStore) GetPersistentVolume(name string) (model.PersistentVolume, error) {
	var pv model.PersistentVolume
	err := s.get(keyPersistentVolume(name), &pv)
	return pv, err
}

func (s *BadgerStateStore) ListPersistentVolumes() ([]model.PersistentVolume, error) {
	return listPrefix[model.PersistentVolume](s, "pv:")
}

func (s *BadgerStateStore) DeletePersistentVolume(name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.db.Update(func(txn *badger.Txn) error {
		return txn.Delete([]byte(keyPersistentVolume(name)))
	})
}

func (s *BadgerStateStore) UpsertPersistentVolumeClaim(pvc model.PersistentVolumeClaim) error {
	if pvc.CreatedAt.IsZero() {
		pvc.CreatedAt = time.Now().UTC()
	}
	pvc.UpdatedAt = time.Now().UTC()
	if pvc.Phase == "" {
		pvc.Phase = model.PersistentVolumeClaimPending
	}
	return s.put(keyPersistentVolumeClaim(pvc.Namespace, pvc.Name), pvc)
}

func (s *BadgerStateStore) GetPersistentVolumeClaim(namespace, name string) (model.PersistentVolumeClaim, error) {
	var pvc model.PersistentVolumeClaim
	err := s.get(keyPersistentVolumeClaim(namespace, name), &pvc)
	return pvc, err
}

func (s *BadgerStateStore) ListPersistentVolumeClaims(namespace string) ([]model.PersistentVolumeClaim, error) {
	return listPrefix[model.PersistentVolumeClaim](s, keyPersistentVolumeClaimPrefix(namespace))
}

func (s *BadgerStateStore) DeletePersistentVolumeClaim(namespace, name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.db.Update(func(txn *badger.Txn) error {
		return txn.Delete([]byte(keyPersistentVolumeClaim(namespace, name)))
	})
}

func (s *BadgerStateStore) UpsertNetworkPolicy(policy model.NetworkPolicy) error {
	if policy.CreatedAt.IsZero() {
		policy.CreatedAt = time.Now().UTC()
	}
	policy.UpdatedAt = time.Now().UTC()
	return s.put(keyNetworkPolicy(policy.Namespace, policy.Name), policy)
}

func (s *BadgerStateStore) GetNetworkPolicy(namespace, name string) (model.NetworkPolicy, error) {
	var np model.NetworkPolicy
	err := s.get(keyNetworkPolicy(namespace, name), &np)
	return np, err
}

func (s *BadgerStateStore) ListNetworkPolicies(namespace string) ([]model.NetworkPolicy, error) {
	return listPrefix[model.NetworkPolicy](s, keyNetworkPolicyPrefix(namespace))
}

func (s *BadgerStateStore) DeleteNetworkPolicy(namespace, name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.db.Update(func(txn *badger.Txn) error {
		return txn.Delete([]byte(keyNetworkPolicy(namespace, name)))
	})
}

func (s *BadgerStateStore) UpsertJob(job model.Job) error {
	if job.CreatedAt.IsZero() {
		job.CreatedAt = time.Now().UTC()
	}
	job.UpdatedAt = time.Now().UTC()
	if job.Status == "" {
		job.Status = model.JobPending
	}
	return s.put(keyJob(job.ID), job)
}

func (s *BadgerStateStore) GetJob(id string) (model.Job, error) {
	var job model.Job
	err := s.get(keyJob(id), &job)
	return job, err
}

func (s *BadgerStateStore) ListJobs(namespace string) ([]model.Job, error) {
	jobs, err := listPrefix[model.Job](s, "job:")
	if err != nil {
		return nil, err
	}
	if strings.TrimSpace(namespace) == "" {
		return jobs, nil
	}
	out := make([]model.Job, 0, len(jobs))
	for _, j := range jobs {
		if strings.EqualFold(strings.TrimSpace(j.Spec.Namespace), strings.TrimSpace(namespace)) {
			out = append(out, j)
		}
	}
	return out, nil
}

func (s *BadgerStateStore) DeleteJob(id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.db.Update(func(txn *badger.Txn) error {
		return txn.Delete([]byte(keyJob(id)))
	})
}

func (s *BadgerStateStore) UpsertCronJob(cj model.CronJob) error {
	if cj.CreatedAt.IsZero() {
		cj.CreatedAt = time.Now().UTC()
	}
	cj.UpdatedAt = time.Now().UTC()
	if cj.ScheduleEveryS <= 0 {
		cj.ScheduleEveryS = 60
	}
	return s.put(keyCronJob(cj.Namespace, cj.Name), cj)
}

func (s *BadgerStateStore) GetCronJob(namespace, name string) (model.CronJob, error) {
	var cj model.CronJob
	err := s.get(keyCronJob(namespace, name), &cj)
	return cj, err
}

func (s *BadgerStateStore) ListCronJobs(namespace string) ([]model.CronJob, error) {
	return listPrefix[model.CronJob](s, keyCronJobPrefix(namespace))
}

func (s *BadgerStateStore) DeleteCronJob(namespace, name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.db.Update(func(txn *badger.Txn) error {
		return txn.Delete([]byte(keyCronJob(namespace, name)))
	})
}

func (s *BadgerStateStore) UpsertRoleBinding(binding model.RoleBinding) error {
	if binding.CreatedAt.IsZero() {
		binding.CreatedAt = time.Now().UTC()
	}
	binding.UpdatedAt = time.Now().UTC()
	return s.put(keyRoleBinding(binding.Namespace, binding.Name), binding)
}

func (s *BadgerStateStore) GetRoleBinding(namespace, name string) (model.RoleBinding, error) {
	var b model.RoleBinding
	err := s.get(keyRoleBinding(namespace, name), &b)
	return b, err
}

func (s *BadgerStateStore) ListRoleBindings(namespace string) ([]model.RoleBinding, error) {
	return listPrefix[model.RoleBinding](s, keyRoleBindingPrefix(namespace))
}

func (s *BadgerStateStore) DeleteRoleBinding(namespace, name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.db.Update(func(txn *badger.Txn) error {
		return txn.Delete([]byte(keyRoleBinding(namespace, name)))
	})
}

func (s *BadgerStateStore) UpsertControllerLease(lease model.ControllerLease) error {
	if lease.UpdatedAt.IsZero() {
		lease.UpdatedAt = time.Now().UTC()
	}
	return s.put(keyControllerLease(), lease)
}

func (s *BadgerStateStore) GetControllerLease() (model.ControllerLease, error) {
	var lease model.ControllerLease
	err := s.get(keyControllerLease(), &lease)
	return lease, err
}

func (s *BadgerStateStore) DeleteControllerLease() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.db.Update(func(txn *badger.Txn) error {
		return txn.Delete([]byte(keyControllerLease()))
	})
}

func (s *BadgerStateStore) AppendControlPlaneTransition(t model.ControlPlaneTransition) error {
	if t.CreatedAt.IsZero() {
		t.CreatedAt = time.Now().UTC()
	}
	if strings.TrimSpace(t.ID) == "" {
		t.ID = fmt.Sprintf("tr-%d", t.CreatedAt.UnixNano())
	}
	return s.put(keyControlPlaneTransition(t.CreatedAt, t.ID), t)
}

func (s *BadgerStateStore) ListControlPlaneTransitions(limit int) ([]model.ControlPlaneTransition, error) {
	if limit <= 0 {
		limit = 100
	}
	items, err := listPrefix[model.ControlPlaneTransition](s, "transition:")
	if err != nil {
		return nil, err
	}
	if len(items) > limit {
		items = items[len(items)-limit:]
	}
	return items, nil
}

func (s *BadgerStateStore) ReplaceServiceEndpoints(serviceID string, endpoints []model.ServiceEndpoint) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.db.Update(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		prefix := []byte(keyServiceEndpointPrefix(serviceID))
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			if err := txn.Delete(it.Item().KeyCopy(nil)); err != nil {
				return err
			}
		}
		for _, ep := range endpoints {
			if ep.ID == "" {
				ep.ID = fmt.Sprintf("%s-%s-%d", ep.ServiceID, ep.WorkloadID, ep.Port)
			}
			if ep.UpdatedAt.IsZero() {
				ep.UpdatedAt = time.Now().UTC()
			}
			blob, err := json.Marshal(ep)
			if err != nil {
				return err
			}
			if err := txn.Set([]byte(keyServiceEndpoint(ep.ServiceID, ep.ID)), blob); err != nil {
				return err
			}
		}
		return nil
	})
}

func (s *BadgerStateStore) ListServiceEndpoints(serviceID string) ([]model.ServiceEndpoint, error) {
	return listPrefix[model.ServiceEndpoint](s, keyServiceEndpointPrefix(serviceID))
}

func (s *BadgerStateStore) UpsertAutoscalerPolicy(policy model.AutoscalerPolicy) error {
	if policy.UpdatedAt.IsZero() {
		policy.UpdatedAt = time.Now().UTC()
	}
	if policy.CreatedAt.IsZero() {
		policy.CreatedAt = policy.UpdatedAt
	}
	return s.put(keyAutoscalerPolicy(policy.ID), policy)
}

func (s *BadgerStateStore) GetAutoscalerPolicy(id string) (model.AutoscalerPolicy, error) {
	var policy model.AutoscalerPolicy
	err := s.get(keyAutoscalerPolicy(id), &policy)
	return policy, err
}

func (s *BadgerStateStore) ListAutoscalerPolicies() ([]model.AutoscalerPolicy, error) {
	return listPrefix[model.AutoscalerPolicy](s, "autoscaler:")
}

func (s *BadgerStateStore) DeleteAutoscalerPolicy(id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.db.Update(func(txn *badger.Txn) error {
		return txn.Delete([]byte(keyAutoscalerPolicy(id)))
	})
}

func (s *BadgerStateStore) AppendDeploymentMetric(sample model.DeploymentMetricSample) error {
	if sample.ID == "" {
		sample.ID = fmt.Sprintf("m-%d", time.Now().UnixNano())
	}
	if sample.Timestamp.IsZero() {
		sample.Timestamp = time.Now().UTC()
	}
	return s.put(keyDeploymentMetric(sample.DeploymentID, sample.Timestamp, sample.ID), sample)
}

func (s *BadgerStateStore) ListDeploymentMetrics(deploymentID string, limit int) ([]model.DeploymentMetricSample, error) {
	if limit <= 0 {
		limit = 100
	}
	items, err := listPrefix[model.DeploymentMetricSample](s, keyDeploymentMetricPrefix(deploymentID))
	if err != nil {
		return nil, err
	}
	if len(items) > limit {
		items = items[len(items)-limit:]
	}
	return items, nil
}

func (s *BadgerStateStore) EnqueueAssignment(a model.Assignment) error {
	if a.ID == "" {
		a.ID = fmt.Sprintf("%s-%d", a.WorkloadID, time.Now().UnixNano())
	}
	return s.put(keyAssignment(a.NodeID, a.ID), a)
}

func (s *BadgerStateStore) PopAssignments(nodeID string, max int) ([]model.Assignment, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	assignments := make([]model.Assignment, 0, max)
	prefix := []byte(keyAssignmentPrefix(nodeID))

	err := s.db.Update(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()

		for it.Seek(prefix); it.ValidForPrefix(prefix) && len(assignments) < max; it.Next() {
			item := it.Item()
			val, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}
			var a model.Assignment
			if err := json.Unmarshal(val, &a); err != nil {
				return err
			}
			assignments = append(assignments, a)
			if err := txn.Delete(item.KeyCopy(nil)); err != nil {
				return err
			}
		}
		return nil
	})
	return assignments, err
}

func (s *BadgerStateStore) Snapshot() (model.ClusterState, error) {
	nodes, err := s.listNodes()
	if err != nil {
		return model.ClusterState{}, err
	}
	deps, err := s.listDeployments()
	if err != nil {
		return model.ClusterState{}, err
	}
	workloads, err := s.listWorkloads()
	if err != nil {
		return model.ClusterState{}, err
	}
	return model.ClusterState{Nodes: nodes, Deployments: deps, Workloads: workloads}, nil
}

func (s *BadgerStateStore) AppendEvent(e model.Event) error {
	if e.ID == "" {
		e.ID = fmt.Sprintf("evt-%d", time.Now().UnixNano())
	}
	if e.Timestamp.IsZero() {
		e.Timestamp = time.Now().UTC()
	}
	return s.put(keyEvent(e.Timestamp, e.ID), e)
}

func (s *BadgerStateStore) ListEvents(limit int) ([]model.Event, error) {
	if limit <= 0 {
		limit = 100
	}
	events, err := listPrefix[model.Event](s, "event:")
	if err != nil {
		return nil, err
	}
	if len(events) > limit {
		events = events[len(events)-limit:]
	}
	return events, nil
}

func (s *BadgerStateStore) put(key string, in any) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	blob, err := json.Marshal(in)
	if err != nil {
		return err
	}
	return s.db.Update(func(txn *badger.Txn) error {
		return txn.Set([]byte(key), blob)
	})
}

func (s *BadgerStateStore) get(key string, out any) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(key))
		if err != nil {
			if errors.Is(err, badger.ErrKeyNotFound) {
				return ErrNotFound
			}
			return err
		}
		blob, err := item.ValueCopy(nil)
		if err != nil {
			return err
		}
		return json.Unmarshal(blob, out)
	})
}

func (s *BadgerStateStore) listNodes() ([]model.Node, error) {
	return listPrefix[model.Node](s, "node:")
}

func (s *BadgerStateStore) listDeployments() ([]model.Deployment, error) {
	return listPrefix[model.Deployment](s, "deployment:")
}

func (s *BadgerStateStore) listWorkloads() ([]model.Workload, error) {
	return listPrefix[model.Workload](s, "workload:")
}

func listPrefix[T any](s *BadgerStateStore, prefix string) ([]T, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	result := []T{}
	err := s.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()

		pfx := []byte(prefix)
		for it.Seek(pfx); it.ValidForPrefix(pfx); it.Next() {
			item := it.Item()
			blob, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}
			var t T
			if err := json.Unmarshal(blob, &t); err != nil {
				return err
			}
			result = append(result, t)
		}
		return nil
	})
	return result, err
}

func keyNode(nodeID string) string {
	return "node:" + nodeID
}

func keyDeployment(deploymentID string) string {
	return "deployment:" + deploymentID
}

func keyWorkload(workloadID string) string {
	return "workload:" + workloadID
}

func keyAssignmentPrefix(nodeID string) string {
	return "assignment:" + nodeID + ":"
}

func keyAssignment(nodeID, assignmentID string) string {
	return keyAssignmentPrefix(nodeID) + assignmentID
}

func keyEvent(ts time.Time, eventID string) string {
	return fmt.Sprintf("event:%s:%s", ts.UTC().Format(time.RFC3339Nano), eventID)
}

func keyService(serviceID string) string {
	return "service:" + serviceID
}

func keyServiceEndpointPrefix(serviceID string) string {
	return "service-endpoint:" + serviceID + ":"
}

func keyServiceEndpoint(serviceID, endpointID string) string {
	return keyServiceEndpointPrefix(serviceID) + endpointID
}

func keyAutoscalerPolicy(policyID string) string {
	return "autoscaler:" + policyID
}

func keyDeploymentMetricPrefix(deploymentID string) string {
	return "metric:" + deploymentID + ":"
}

func keyDeploymentMetric(deploymentID string, ts time.Time, metricID string) string {
	return fmt.Sprintf("%s%s:%s", keyDeploymentMetricPrefix(deploymentID), ts.UTC().Format(time.RFC3339Nano), metricID)
}

func keyNamespace(name string) string {
	return "namespace:" + name
}

func keyNamespaceQuota(namespace string) string {
	return "quota:" + namespace
}

func keySecretPrefix(namespace string) string {
	return "secret:" + namespace + ":"
}

func keySecret(namespace, name string) string {
	return keySecretPrefix(namespace) + name
}

func keyPersistentVolume(name string) string {
	return "pv:" + name
}

func keyPersistentVolumeClaimPrefix(namespace string) string {
	return "pvc:" + namespace + ":"
}

func keyPersistentVolumeClaim(namespace, name string) string {
	return keyPersistentVolumeClaimPrefix(namespace) + name
}

func keyNetworkPolicyPrefix(namespace string) string {
	return "netpol:" + namespace + ":"
}

func keyNetworkPolicy(namespace, name string) string {
	return keyNetworkPolicyPrefix(namespace) + name
}

func keyJob(id string) string {
	return "job:" + id
}

func keyCronJobPrefix(namespace string) string {
	return "cronjob:" + namespace + ":"
}

func keyCronJob(namespace, name string) string {
	return keyCronJobPrefix(namespace) + name
}

func keyRoleBindingPrefix(namespace string) string {
	if strings.TrimSpace(namespace) == "" {
		return "rbac:global:"
	}
	return "rbac:" + namespace + ":"
}

func keyRoleBinding(namespace, name string) string {
	return keyRoleBindingPrefix(namespace) + name
}

func keyControllerLease() string {
	return "controller:lease"
}

func keyControlPlaneTransition(ts time.Time, id string) string {
	return fmt.Sprintf("transition:%s:%s", ts.UTC().Format(time.RFC3339Nano), id)
}
