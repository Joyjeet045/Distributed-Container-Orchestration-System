package worker

import (
	"context"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"minikube-orchestrator/internal/model"
)

type AssignmentClient interface {
	RegisterNode(node model.Node) error
	Heartbeat(nodeID string, used model.Resource) error
	PollAssignments(nodeID string, max int) ([]model.Assignment, error)
	ListNodeWorkloads(nodeID string) ([]model.Workload, error)
	ReportWorkloadStatus(workloadID string, status model.WorkloadStatus, containerID string) error
}

type Runtime interface {
	RunWorkload(ctx context.Context, workloadID, image, imagePullSecret string) (string, error)
	StopWorkload(ctx context.Context, workloadID, containerID string) error
	ListManagedWorkloads(ctx context.Context) (map[string]string, error)
}

type Runner struct {
	nodeID  string
	client  AssignmentClient
	runtime Runtime
	used    model.Resource

	restartMaxRetries int
	restartBackoff    time.Duration

	mu sync.RWMutex

	secretFilesMu sync.Mutex
	secretFiles   map[string][]string
}

func NewRunner(nodeID string, client AssignmentClient, runtime Runtime, restartMaxRetries int, restartBackoff time.Duration) *Runner {
	if restartMaxRetries <= 0 {
		restartMaxRetries = 3
	}
	if restartBackoff <= 0 {
		restartBackoff = 250 * time.Millisecond
	}
	return &Runner{nodeID: nodeID, client: client, runtime: runtime, restartMaxRetries: restartMaxRetries, restartBackoff: restartBackoff, secretFiles: map[string][]string{}}
}

func (r *Runner) Register(node model.Node) error {
	return r.client.RegisterNode(node)
}

func (r *Runner) Heartbeat() error {
	r.mu.RLock()
	used := r.used
	r.mu.RUnlock()
	return r.client.Heartbeat(r.nodeID, used)
}

func (r *Runner) ProcessAssignments(ctx context.Context, max int) (int, error) {
	assignments, err := r.client.PollAssignments(r.nodeID, max)
	if err != nil {
		return 0, err
	}

	processed := 0
	for _, a := range assignments {
		action := a.Action
		if action == "" {
			action = "create"
		}
		switch action {
		case "delete":
			r.cleanupSecretFiles(a.WorkloadID)
			if err := r.runtime.StopWorkload(ctx, a.WorkloadID, a.ContainerID); err != nil {
				log.Printf("workload %s delete failed: %v", a.WorkloadID, err)
				_ = r.client.ReportWorkloadStatus(a.WorkloadID, model.WorkloadFailed, "")
				processed++
				continue
			}
			r.mu.Lock()
			r.used.MilliCPU -= a.Resources.MilliCPU
			r.used.MemoryMB -= a.Resources.MemoryMB
			if r.used.MilliCPU < 0 {
				r.used.MilliCPU = 0
			}
			if r.used.MemoryMB < 0 {
				r.used.MemoryMB = 0
			}
			r.mu.Unlock()
			_ = r.client.ReportWorkloadStatus(a.WorkloadID, model.WorkloadTerminated, "")
			processed++
		case "create":
			if err := r.materializeSecretFiles(a.WorkloadID, a.SecretFiles); err != nil {
				log.Printf("workload %s secret file materialization failed: %v", a.WorkloadID, err)
				_ = r.client.ReportWorkloadStatus(a.WorkloadID, model.WorkloadFailed, "")
				processed++
				continue
			}
			containerID, runErr := r.runWithBackoff(ctx, a)
			if runErr != nil {
				log.Printf("workload %s failed after retries: %v", a.WorkloadID, runErr)
				r.cleanupSecretFiles(a.WorkloadID)
				_ = r.client.ReportWorkloadStatus(a.WorkloadID, model.WorkloadFailed, "")
				processed++
				continue
			}

			r.mu.Lock()
			r.used.MilliCPU += a.Resources.MilliCPU
			r.used.MemoryMB += a.Resources.MemoryMB
			r.mu.Unlock()
			if err := r.client.ReportWorkloadStatus(a.WorkloadID, model.WorkloadRunning, containerID); err != nil {
				log.Printf("report workload status failed: %v", err)
			}
			processed++
		default:
			log.Printf("unknown assignment action %q for workload %s", action, a.WorkloadID)
			processed++
		}
	}

	return processed, nil
}

func (r *Runner) materializeSecretFiles(workloadID string, files []model.SecretFile) error {
	if len(files) == 0 {
		return nil
	}
	dir := filepath.Join(os.TempDir(), "orch-secrets", workloadID)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	created := make([]string, 0, len(files))
	for _, f := range files {
		name := filepath.Base(strings.TrimSpace(f.Path))
		if name == "" || name == "." {
			continue
		}
		target := filepath.Join(dir, name)
		if err := os.WriteFile(target, []byte(f.Content), 0o600); err != nil {
			return err
		}
		created = append(created, target)
	}
	r.secretFilesMu.Lock()
	r.secretFiles[workloadID] = created
	r.secretFilesMu.Unlock()
	return nil
}

func (r *Runner) cleanupSecretFiles(workloadID string) {
	r.secretFilesMu.Lock()
	paths := r.secretFiles[workloadID]
	delete(r.secretFiles, workloadID)
	r.secretFilesMu.Unlock()
	for _, p := range paths {
		_ = os.Remove(p)
	}
	if len(paths) > 0 {
		_ = os.Remove(filepath.Dir(paths[0]))
	}
}

func (r *Runner) UsedResource() model.Resource {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.used
}

func (r *Runner) NodeID() string {
	return r.nodeID
}

func (r *Runner) runWithBackoff(ctx context.Context, a model.Assignment) (string, error) {
	var lastErr error
	for attempt := 0; attempt < r.restartMaxRetries; attempt++ {
		containerID, err := r.runtime.RunWorkload(ctx, a.WorkloadID, a.Image, a.ImagePullSecret)
		if err == nil {
			return containerID, nil
		}
		lastErr = err
		if attempt == r.restartMaxRetries-1 {
			break
		}
		backoff := r.restartBackoff * time.Duration(1<<attempt)
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-time.After(backoff):
		}
	}
	return "", lastErr
}

func (r *Runner) ReconcileRuntime(ctx context.Context) error {
	desired, err := r.client.ListNodeWorkloads(r.nodeID)
	if err != nil {
		return err
	}
	desiredMap := map[string]model.Workload{}
	for _, w := range desired {
		desiredMap[w.ID] = w
	}

	actual, err := r.runtime.ListManagedWorkloads(ctx)
	if err != nil {
		return err
	}

	for workloadID := range desiredMap {
		if _, ok := actual[workloadID]; !ok {
			_ = r.client.ReportWorkloadStatus(workloadID, model.WorkloadFailed, "")
		}
	}
	for workloadID, containerID := range actual {
		if _, ok := desiredMap[workloadID]; !ok {
			_ = r.runtime.StopWorkload(ctx, workloadID, containerID)
		}
	}
	return nil
}
