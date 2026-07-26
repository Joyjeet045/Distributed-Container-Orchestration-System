package service

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"net/http"
	"os"
	"os/exec"
	"sort"
	"strings"
	"sync"
	"time"

	"minikube-orchestrator/internal/model"
	"minikube-orchestrator/internal/store"
)

var kmsKeyOnce sync.Once
var kmsKeyValue []byte

func (o *Orchestrator) UpsertPersistentVolume(_ context.Context, pv model.PersistentVolume) (model.PersistentVolume, error) {
	if strings.TrimSpace(pv.Name) == "" {
		return model.PersistentVolume{}, errors.New("persistent volume name is required")
	}
	if pv.CapacityMB <= 0 {
		return model.PersistentVolume{}, errors.New("persistent volume capacityMb must be > 0")
	}
	if pv.Phase == "" {
		pv.Phase = model.PersistentVolumeAvailable
	}
	if err := o.store.UpsertPersistentVolume(pv); err != nil {
		return model.PersistentVolume{}, err
	}
	return o.store.GetPersistentVolume(pv.Name)
}

func (o *Orchestrator) ListPersistentVolumes(_ context.Context) ([]model.PersistentVolume, error) {
	return o.store.ListPersistentVolumes()
}

func (o *Orchestrator) UpsertPersistentVolumeClaim(_ context.Context, pvc model.PersistentVolumeClaim) (model.PersistentVolumeClaim, error) {
	if strings.TrimSpace(pvc.Namespace) == "" {
		pvc.Namespace = "default"
	}
	if strings.TrimSpace(pvc.Name) == "" {
		return model.PersistentVolumeClaim{}, errors.New("persistent volume claim name is required")
	}
	if pvc.RequestedCapacity <= 0 {
		return model.PersistentVolumeClaim{}, errors.New("persistent volume claim requestedCapacityMb must be > 0")
	}
	if err := o.ensureNamespaceExists(pvc.Namespace); err != nil {
		return model.PersistentVolumeClaim{}, err
	}
	if pvc.Phase == "" {
		pvc.Phase = model.PersistentVolumeClaimPending
	}
	if err := o.store.UpsertPersistentVolumeClaim(pvc); err != nil {
		return model.PersistentVolumeClaim{}, err
	}
	if err := o.reconcilePVCBindings(); err != nil {
		return model.PersistentVolumeClaim{}, err
	}
	return o.store.GetPersistentVolumeClaim(pvc.Namespace, pvc.Name)
}

func (o *Orchestrator) ListPersistentVolumeClaims(_ context.Context, namespace string) ([]model.PersistentVolumeClaim, error) {
	if strings.TrimSpace(namespace) == "" {
		namespace = "default"
	}
	return o.store.ListPersistentVolumeClaims(namespace)
}

func (o *Orchestrator) reconcilePVCBindings() error {
	pvs, err := o.store.ListPersistentVolumes()
	if err != nil {
		return err
	}
	pvcs, err := o.store.ListPersistentVolumeClaims("default")
	if err != nil {
		return err
	}
	namespaces, err := o.store.ListNamespaces()
	if err != nil {
		return err
	}
	for _, ns := range namespaces {
		items, lerr := o.store.ListPersistentVolumeClaims(ns.Name)
		if lerr == nil {
			pvcs = append(pvcs, items...)
		}
	}

	sort.SliceStable(pvs, func(i, j int) bool { return pvs[i].CapacityMB < pvs[j].CapacityMB })
	for _, pvc := range pvcs {
		if pvc.Phase == model.PersistentVolumeClaimBound && strings.TrimSpace(pvc.VolumeName) != "" {
			continue
		}
		for i := range pvs {
			pv := pvs[i]
			if pv.Phase != model.PersistentVolumeAvailable {
				continue
			}
			if pv.CapacityMB < pvc.RequestedCapacity {
				continue
			}
			if strings.TrimSpace(pvc.StorageClass) != "" && strings.TrimSpace(pv.StorageClass) != "" && pvc.StorageClass != pv.StorageClass {
				continue
			}
			if strings.TrimSpace(pvc.AccessMode) != "" && strings.TrimSpace(pv.AccessMode) != "" && pvc.AccessMode != pv.AccessMode {
				continue
			}
			pv.Phase = model.PersistentVolumeBound
			pv.ClaimNamespace = pvc.Namespace
			pv.ClaimName = pvc.Name
			pvc.VolumeName = pv.Name
			pvc.Phase = model.PersistentVolumeClaimBound
			if err := o.store.UpsertPersistentVolume(pv); err != nil {
				return err
			}
			if err := o.store.UpsertPersistentVolumeClaim(pvc); err != nil {
				return err
			}
			break
		}
	}
	return nil
}

func (o *Orchestrator) UpsertNetworkPolicy(_ context.Context, policy model.NetworkPolicy) (model.NetworkPolicy, error) {
	if strings.TrimSpace(policy.Namespace) == "" {
		policy.Namespace = "default"
	}
	if strings.TrimSpace(policy.Name) == "" {
		return model.NetworkPolicy{}, errors.New("network policy name is required")
	}
	if len(policy.PodSelector) == 0 {
		return model.NetworkPolicy{}, errors.New("network policy podSelector is required")
	}
	if err := o.ensureNamespaceExists(policy.Namespace); err != nil {
		return model.NetworkPolicy{}, err
	}
	if err := o.store.UpsertNetworkPolicy(policy); err != nil {
		return model.NetworkPolicy{}, err
	}
	return o.store.GetNetworkPolicy(policy.Namespace, policy.Name)
}

func (o *Orchestrator) ListNetworkPolicies(_ context.Context, namespace string) ([]model.NetworkPolicy, error) {
	if strings.TrimSpace(namespace) == "" {
		namespace = "default"
	}
	return o.store.ListNetworkPolicies(namespace)
}

func (o *Orchestrator) IsNetworkAccessAllowed(_ context.Context, sourceNamespace string, sourceLabels map[string]string, svc model.Service, port int) (bool, string, error) {
	if strings.TrimSpace(sourceNamespace) == "" {
		sourceNamespace = "default"
	}
	policies, err := o.store.ListNetworkPolicies(svc.Spec.Namespace)
	if err != nil {
		return false, "", err
	}
	applicable := make([]model.NetworkPolicy, 0)
	for _, p := range policies {
		if labelsMatchSelector(svc.Spec.Selector, p.PodSelector) {
			applicable = append(applicable, p)
		}
	}
	if len(applicable) == 0 {
		return true, "no_applicable_policy", nil
	}
	for _, p := range applicable {
		for _, rule := range p.Ingress {
			if rule.Port > 0 && rule.Port != port {
				continue
			}
			if len(rule.From) == 0 {
				return true, "allow_empty_from", nil
			}
			for _, peer := range rule.From {
				if strings.TrimSpace(peer.Namespace) != "" && !strings.EqualFold(peer.Namespace, sourceNamespace) {
					continue
				}
				if len(peer.Labels) > 0 && !labelsMatchSelector(sourceLabels, peer.Labels) {
					continue
				}
				if strings.TrimSpace(p.Group) != "" {
					if sourceLabels["group"] != p.Group {
						continue
					}
				}
				return true, "allow_matched_rule", nil
			}
		}
	}
	return false, "denied_by_network_policy", nil
}

func (o *Orchestrator) UpsertRoleBinding(_ context.Context, b model.RoleBinding) (model.RoleBinding, error) {
	if strings.TrimSpace(b.Name) == "" {
		return model.RoleBinding{}, errors.New("role binding name is required")
	}
	if strings.TrimSpace(b.Subject) == "" {
		return model.RoleBinding{}, errors.New("role binding subject is required")
	}
	if len(b.Roles) == 0 {
		return model.RoleBinding{}, errors.New("role binding roles are required")
	}
	if err := o.store.UpsertRoleBinding(b); err != nil {
		return model.RoleBinding{}, err
	}
	return o.store.GetRoleBinding(b.Namespace, b.Name)
}

func (o *Orchestrator) ListRoleBindings(_ context.Context, namespace string) ([]model.RoleBinding, error) {
	return o.store.ListRoleBindings(namespace)
}

func (o *Orchestrator) EffectiveRolesForSubject(_ context.Context, subject, namespace string) ([]string, error) {
	all, err := o.store.ListRoleBindings("")
	if err != nil {
		return nil, err
	}
	if strings.TrimSpace(namespace) != "" {
		nsItems, nsErr := o.store.ListRoleBindings(namespace)
		if nsErr == nil {
			all = append(all, nsItems...)
		}
	}
	byName := map[string]model.RoleBinding{}
	for _, rb := range all {
		byName[rb.Name] = rb
	}
	roleSet := map[string]struct{}{}
	var visit func(name string)
	visit = func(name string) {
		rb, ok := byName[name]
		if !ok {
			return
		}
		for _, r := range rb.Roles {
			if strings.TrimSpace(r) != "" {
				roleSet[strings.ToLower(strings.TrimSpace(r))] = struct{}{}
			}
		}
		for _, parent := range rb.Inherits {
			visit(parent)
		}
	}
	for _, rb := range all {
		if strings.EqualFold(strings.TrimSpace(rb.Subject), strings.TrimSpace(subject)) {
			visit(rb.Name)
		}
	}
	out := make([]string, 0, len(roleSet))
	for role := range roleSet {
		out = append(out, role)
	}
	sort.Strings(out)
	return out, nil
}

func (o *Orchestrator) TryAcquireControllerLeadership(_ context.Context, controllerID string, leaseDuration time.Duration) (bool, model.ControllerLease, error) {
	if strings.TrimSpace(controllerID) == "" {
		return false, model.ControllerLease{}, errors.New("controller id is required")
	}
	if leaseDuration <= 0 {
		leaseDuration = 10 * time.Second
	}
	now := time.Now().UTC()
	lease, err := o.store.GetControllerLease()
	if err != nil && !errors.Is(err, store.ErrNotFound) {
		return false, model.ControllerLease{}, err
	}
	if errors.Is(err, store.ErrNotFound) || strings.TrimSpace(lease.LeaderID) == "" || now.After(lease.ExpiresAt) || lease.LeaderID == controllerID {
		term := lease.Term
		if lease.LeaderID != controllerID {
			term++
		}
		lease = model.ControllerLease{LeaderID: controllerID, ExpiresAt: now.Add(leaseDuration), Term: term, UpdatedAt: now}
		if err := o.store.UpsertControllerLease(lease); err != nil {
			return false, model.ControllerLease{}, err
		}
		action := "LeaderRenewed"
		if term > 0 && lease.LeaderID == controllerID {
			action = "LeaderAcquired"
		}
		_ = o.store.AppendControlPlaneTransition(model.ControlPlaneTransition{NodeID: controllerID, Term: term, Action: action, Description: "controller leadership updated", CreatedAt: now})
		return true, lease, nil
	}
	return lease.LeaderID == controllerID, lease, nil
}

func (o *Orchestrator) ReleaseControllerLeadership(_ context.Context, controllerID string) error {
	lease, err := o.store.GetControllerLease()
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil
		}
		return err
	}
	if lease.LeaderID != controllerID {
		return nil
	}
	if err := o.store.DeleteControllerLease(); err != nil {
		return err
	}
	return o.store.AppendControlPlaneTransition(model.ControlPlaneTransition{NodeID: controllerID, Term: lease.Term, Action: "LeaderReleased", Description: "controller released lease", CreatedAt: time.Now().UTC()})
}

func (o *Orchestrator) ReconcileShard(ctx context.Context, shardIndex, shardCount int) error {
	if shardCount <= 1 {
		return o.ReconcileAll(ctx)
	}
	deps, err := o.store.ListDeployments()
	if err != nil {
		return err
	}
	for _, dep := range deps {
		if dep.DeletionTimestamp != nil {
			if o.belongsToShard(dep.ID, shardIndex, shardCount) {
				_ = o.reconcileDeletion(dep.ID)
			}
			continue
		}
		if o.belongsToShard(dep.ID, shardIndex, shardCount) {
			_ = o.reconcileDeployment(dep.ID)
		}
	}
	if shardIndex == 0 {
		if err := o.reconcileAutoscaling(); err != nil {
			return err
		}
		if err := o.evaluateWorkloadHealth(); err != nil {
			return err
		}
		if err := o.reconcileAllServiceEndpoints(); err != nil {
			return err
		}
		if err := o.reconcilePVCBindings(); err != nil {
			return err
		}
		if err := o.reconcileJobsAndCron(ctx); err != nil {
			return err
		}
	}
	return o.updateNodeHealth(ctx)
}

func (o *Orchestrator) belongsToShard(id string, shardIndex, shardCount int) bool {
	h := fnv.New32a()
	_, _ = h.Write([]byte(id))
	return int(h.Sum32()%uint32(shardCount)) == shardIndex
}

func (o *Orchestrator) RecoverCluster(ctx context.Context) error {
	if err := o.reconcilePVCBindings(); err != nil {
		return err
	}
	if err := o.reconcileJobsAndCron(ctx); err != nil {
		return err
	}
	return o.ReconcileAll(ctx)
}

func (o *Orchestrator) UpsertJob(_ context.Context, job model.Job) (model.Job, error) {
	if strings.TrimSpace(job.Spec.Namespace) == "" {
		job.Spec.Namespace = "default"
	}
	if strings.TrimSpace(job.Spec.Name) == "" {
		return model.Job{}, errors.New("job name is required")
	}
	if job.Spec.Completions <= 0 {
		job.Spec.Completions = 1
	}
	if job.Spec.BackoffLimit < 0 {
		job.Spec.BackoffLimit = 0
	}
	if strings.TrimSpace(job.ID) == "" {
		job.ID = fmt.Sprintf("job-%d", time.Now().UnixNano())
	}
	if job.Status == "" {
		job.Status = model.JobPending
	}
	if err := o.store.UpsertJob(job); err != nil {
		return model.Job{}, err
	}
	return o.store.GetJob(job.ID)
}

func (o *Orchestrator) ListJobs(_ context.Context, namespace string) ([]model.Job, error) {
	if strings.TrimSpace(namespace) == "" {
		namespace = "default"
	}
	return o.store.ListJobs(namespace)
}

func (o *Orchestrator) UpsertCronJob(_ context.Context, cj model.CronJob) (model.CronJob, error) {
	if strings.TrimSpace(cj.Namespace) == "" {
		cj.Namespace = "default"
	}
	if strings.TrimSpace(cj.Name) == "" {
		return model.CronJob{}, errors.New("cronjob name is required")
	}
	if cj.ScheduleEveryS <= 0 {
		return model.CronJob{}, errors.New("cronjob scheduleEverySeconds must be > 0")
	}
	if cj.HistoryLimit <= 0 {
		cj.HistoryLimit = 5
	}
	if err := o.store.UpsertCronJob(cj); err != nil {
		return model.CronJob{}, err
	}
	return o.store.GetCronJob(cj.Namespace, cj.Name)
}

func (o *Orchestrator) ListCronJobs(_ context.Context, namespace string) ([]model.CronJob, error) {
	if strings.TrimSpace(namespace) == "" {
		namespace = "default"
	}
	return o.store.ListCronJobs(namespace)
}

func (o *Orchestrator) reconcileJobsAndCron(ctx context.Context) error {
	cronJobs, err := o.store.ListCronJobs("default")
	if err != nil {
		return err
	}
	namespaces, err := o.store.ListNamespaces()
	if err != nil {
		return err
	}
	for _, ns := range namespaces {
		items, lerr := o.store.ListCronJobs(ns.Name)
		if lerr == nil {
			cronJobs = append(cronJobs, items...)
		}
	}
	now := time.Now().UTC()
	for _, cj := range cronJobs {
		if cj.Suspend {
			continue
		}
		if cj.LastRunAt.IsZero() || now.Sub(cj.LastRunAt) >= time.Duration(cj.ScheduleEveryS)*time.Second {
			job := model.Job{Spec: cj.Template, Status: model.JobPending}
			job.Spec.Namespace = cj.Namespace
			job.Spec.Name = fmt.Sprintf("%s-%d", cj.Name, now.Unix())
			job.Spec.ScheduleReason = "cron"
			if _, jerr := o.UpsertJob(ctx, job); jerr == nil {
				cj.LastRunAt = now
				_ = o.store.UpsertCronJob(cj)
			}
		}
	}

	jobs, err := o.store.ListJobs("default")
	if err != nil {
		return err
	}
	for _, ns := range namespaces {
		items, lerr := o.store.ListJobs(ns.Name)
		if lerr == nil {
			jobs = append(jobs, items...)
		}
	}
	for _, job := range jobs {
		switch job.Status {
		case model.JobPending:
			if strings.TrimSpace(job.Deployment) == "" {
				spec := job.Spec.Template
				spec.Namespace = job.Spec.Namespace
				spec.Name = "job-" + job.ID
				spec.Replicas = 1
				dep, derr := o.CreateDeployment(ctx, spec)
				if derr == nil {
					job.Deployment = dep.ID
					job.Status = model.JobRunning
					_ = o.store.UpsertJob(job)
				}
			}
		case model.JobRunning:
			if strings.TrimSpace(job.Deployment) == "" {
				continue
			}
			workloads, werr := o.store.ListWorkloadsByDeployment(job.Deployment)
			if werr != nil {
				continue
			}
			for _, w := range workloads {
				if w.Status == model.WorkloadRunning {
					job.Succeeded++
					job.RunCount++
					job.Status = model.JobSucceeded
					job.CompletedAt = now
					_ = o.store.UpsertJob(job)
					_ = o.DeleteDeployment(ctx, job.Deployment)
					break
				}
				if w.Status == model.WorkloadFailed {
					job.Failed++
					if job.Failed > job.Spec.BackoffLimit {
						job.Status = model.JobFailed
						job.CompletedAt = now
					} else {
						job.Status = model.JobPending
						job.Deployment = ""
					}
					_ = o.store.UpsertJob(job)
					_ = o.DeleteDeployment(ctx, job.Deployment)
					break
				}
			}
		case model.JobSucceeded, model.JobFailed:
			if job.Spec.TTLSeconds > 0 && !job.CompletedAt.IsZero() && now.Sub(job.CompletedAt) >= time.Duration(job.Spec.TTLSeconds)*time.Second {
				_ = o.store.DeleteJob(job.ID)
			}
		}
	}
	return nil
}

func (o *Orchestrator) DrainNode(_ context.Context, nodeID string) error {
	node, err := o.store.GetNode(nodeID)
	if err != nil {
		return err
	}
	node.Health.Draining = true
	node.Health.Isolated = true
	node.Status = model.NodeUnknown
	if err := o.store.UpsertNode(node); err != nil {
		return err
	}
	return o.remediateUnhealthyNode(nodeID, "manual drain")
}

func (o *Orchestrator) RemoveNode(_ context.Context, nodeID string) error {
	if err := o.DrainNode(context.Background(), nodeID); err != nil {
		return err
	}
	node, err := o.store.GetNode(nodeID)
	if err != nil {
		return err
	}
	node.Status = model.NodeDown
	node.Health.Reason = "node removed"
	return o.store.UpsertNode(node)
}

func (o *Orchestrator) rebalanceWorkloads() error {
	nodes, err := o.store.ListNodes()
	if err != nil {
		return err
	}
	if len(nodes) < 2 {
		return nil
	}
	sort.SliceStable(nodes, func(i, j int) bool {
		ui := safeUtilization(nodes[i].Used.MilliCPU+nodes[i].Used.MemoryMB, nodes[i].Allocatable.MilliCPU+nodes[i].Allocatable.MemoryMB)
		uj := safeUtilization(nodes[j].Used.MilliCPU+nodes[j].Used.MemoryMB, nodes[j].Allocatable.MilliCPU+nodes[j].Allocatable.MemoryMB)
		return ui > uj
	})
	heavy := nodes[0]
	light := nodes[len(nodes)-1]
	heavyUtil := safeUtilization(heavy.Used.MilliCPU+heavy.Used.MemoryMB, heavy.Allocatable.MilliCPU+heavy.Allocatable.MemoryMB)
	lightUtil := safeUtilization(light.Used.MilliCPU+light.Used.MemoryMB, light.Allocatable.MilliCPU+light.Allocatable.MemoryMB)
	if heavyUtil-lightUtil < 0.25 {
		return nil
	}
	workloads, err := o.store.ListWorkloads()
	if err != nil {
		return err
	}
	for _, w := range workloads {
		if w.NodeID == heavy.ID && w.Status == model.WorkloadRunning {
			w.Status = model.WorkloadTerminating
			if err := o.store.UpsertWorkload(w); err != nil {
				return err
			}
			if err := o.store.EnqueueAssignment(model.Assignment{Action: "delete", NodeID: w.NodeID, WorkloadID: w.ID, ContainerID: w.ContainerID, Resources: w.Resources}); err != nil {
				return err
			}
			_ = o.emitEvent(model.Event{Level: model.EventInfo, Type: "rebalance", Reason: "WorkloadEvicted", Message: "Workload evicted for node rebalance", DeploymentID: w.DeploymentID, WorkloadID: w.ID, NodeID: w.NodeID})
			break
		}
	}
	return nil
}

func (o *Orchestrator) runAdmissionCheck(kind string, payload any) error {
	cmdText := strings.TrimSpace(os.Getenv("ORCH_ADMISSION_PLUGIN_CMD"))
	if cmdText == "" {
		return nil
	}
	parts := strings.Fields(cmdText)
	if len(parts) == 0 {
		return nil
	}
	cmd := exec.Command(parts[0], parts[1:]...)
	in := map[string]any{"kind": kind, "payload": payload}
	blob, _ := json.Marshal(in)
	cmd.Stdin = strings.NewReader(string(blob))
	out, err := cmd.Output()
	if err != nil {
		return fmt.Errorf("admission plugin failed: %w", err)
	}
	var resp struct {
		Allowed bool   `json:"allowed"`
		Reason  string `json:"reason"`
	}
	if err := json.Unmarshal(out, &resp); err != nil {
		return fmt.Errorf("admission plugin invalid response: %w", err)
	}
	if !resp.Allowed {
		if strings.TrimSpace(resp.Reason) == "" {
			resp.Reason = "admission denied by plugin"
		}
		return errors.New(resp.Reason)
	}
	return nil
}

func (o *Orchestrator) encryptSecretData(in map[string]string) (map[string]string, error) {
	key := getKMSKey()
	if len(key) == 0 {
		return in, nil
	}
	out := make(map[string]string, len(in))
	for k, v := range in {
		enc, err := encryptStringAESGCM(key, v)
		if err != nil {
			return nil, err
		}
		out[k] = "enc:v1:" + enc
	}
	return out, nil
}

func (o *Orchestrator) decryptSecretData(in map[string]string) map[string]string {
	key := getKMSKey()
	if len(key) == 0 {
		return in
	}
	out := make(map[string]string, len(in))
	for k, v := range in {
		if strings.HasPrefix(v, "enc:v1:") {
			raw, err := decryptStringAESGCM(key, strings.TrimPrefix(v, "enc:v1:"))
			if err == nil {
				out[k] = raw
				continue
			}
		}
		out[k] = v
	}
	return out
}

func (o *Orchestrator) resolveSecretFilesForDeployment(dep model.Deployment) ([]model.SecretFile, error) {
	files := make([]model.SecretFile, 0)
	for _, sm := range dep.Spec.SecretMounts {
		if strings.TrimSpace(sm.SecretName) == "" {
			continue
		}
		secret, err := o.store.GetSecret(dep.Spec.Namespace, sm.SecretName)
		if err != nil {
			return nil, err
		}
		data := o.decryptSecretData(secret.Data)
		for k, v := range data {
			base := strings.TrimRight(strings.TrimSpace(sm.MountPath), "/")
			if base == "" {
				base = "/etc/secrets"
			}
			files = append(files, model.SecretFile{Path: base + "/" + k, Content: v})
		}
	}
	sort.SliceStable(files, func(i, j int) bool { return files[i].Path < files[j].Path })
	return files, nil
}

func getKMSKey() []byte {
	kmsKeyOnce.Do(func() {
		raw := strings.TrimSpace(os.Getenv("ORCH_KMS_KEY"))
		if raw == "" {
			if fp := strings.TrimSpace(os.Getenv("ORCH_KMS_KEY_FILE")); fp != "" {
				blob, err := os.ReadFile(fp)
				if err == nil {
					raw = strings.TrimSpace(string(blob))
				}
			}
		}
		if raw == "" {
			if url := strings.TrimSpace(os.Getenv("ORCH_KMS_KEY_ENDPOINT")); url != "" {
				resp, err := http.Get(url)
				if err == nil {
					defer resp.Body.Close()
					blob, _ := io.ReadAll(resp.Body)
					raw = strings.TrimSpace(string(blob))
				}
			}
		}
		if raw == "" {
			return
		}
		decoded, err := base64.StdEncoding.DecodeString(raw)
		if err == nil && len(decoded) >= 32 {
			kmsKeyValue = decoded[:32]
			return
		}
		if err == nil && len(decoded) > 0 {
			h := sha256.Sum256(decoded)
			kmsKeyValue = h[:]
			return
		}
		hash := sha256.Sum256([]byte(raw))
		kmsKeyValue = hash[:]
	})
	return kmsKeyValue
}

func encryptStringAESGCM(key []byte, plain string) (string, error) {
	if len(key) < 32 {
		return "", errors.New("kms key must be at least 32 bytes")
	}
	block, err := aes.NewCipher(key[:32])
	if err != nil {
		return "", err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return "", err
	}
	nonce := make([]byte, gcm.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return "", err
	}
	ciphertext := gcm.Seal(nil, nonce, []byte(plain), nil)
	blob := append(nonce, ciphertext...)
	return base64.StdEncoding.EncodeToString(blob), nil
}

func decryptStringAESGCM(key []byte, enc string) (string, error) {
	if len(key) < 32 {
		return "", errors.New("kms key must be at least 32 bytes")
	}
	blob, err := base64.StdEncoding.DecodeString(enc)
	if err != nil {
		return "", err
	}
	block, err := aes.NewCipher(key[:32])
	if err != nil {
		return "", err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return "", err
	}
	nonceSize := gcm.NonceSize()
	if len(blob) < nonceSize {
		return "", errors.New("ciphertext too short")
	}
	nonce, ciphertext := blob[:nonceSize], blob[nonceSize:]
	plain, err := gcm.Open(nil, nonce, ciphertext, nil)
	if err != nil {
		return "", err
	}
	return string(plain), nil
}
