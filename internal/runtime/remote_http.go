package runtime

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"
)

type RemoteHTTPRuntime struct {
	baseURL string
	token   string
	client  *http.Client
}

func init() {
	RegisterDriver("remote-http", func() (ContainerRuntime, error) {
		base := strings.TrimSpace(os.Getenv("ORCH_REMOTE_RUNTIME_URL"))
		if base == "" {
			base = "http://localhost:18080"
		}
		tok := strings.TrimSpace(os.Getenv("ORCH_REMOTE_RUNTIME_TOKEN"))
		return NewRemoteHTTPRuntime(base, tok), nil
	})
}

func NewRemoteHTTPRuntime(baseURL, token string) *RemoteHTTPRuntime {
	return &RemoteHTTPRuntime{
		baseURL: strings.TrimRight(strings.TrimSpace(baseURL), "/"),
		token:   strings.TrimSpace(token),
		client:  &http.Client{Timeout: 5 * time.Second},
	}
}

func (r *RemoteHTTPRuntime) RunWorkload(ctx context.Context, workloadID, imageRef, imagePullSecret string) (string, error) {
	payload := map[string]any{"workloadId": workloadID, "image": imageRef, "imagePullSecret": imagePullSecret}
	var out struct {
		ContainerID string `json:"containerId"`
	}
	if err := r.doJSON(ctx, http.MethodPost, "/v1/runtime/workloads", payload, &out); err != nil {
		return "", err
	}
	if strings.TrimSpace(out.ContainerID) == "" {
		return "", fmt.Errorf("remote runtime returned empty container id")
	}
	return out.ContainerID, nil
}

func (r *RemoteHTTPRuntime) StopWorkload(ctx context.Context, workloadID, containerID string) error {
	payload := map[string]any{"workloadId": workloadID, "containerId": containerID}
	return r.doJSON(ctx, http.MethodPost, "/v1/runtime/workloads/stop", payload, nil)
}

func (r *RemoteHTTPRuntime) ListManagedWorkloads(ctx context.Context) (map[string]string, error) {
	var out struct {
		Workloads map[string]string `json:"workloads"`
	}
	if err := r.doJSON(ctx, http.MethodGet, "/v1/runtime/workloads", nil, &out); err != nil {
		return nil, err
	}
	if out.Workloads == nil {
		out.Workloads = map[string]string{}
	}
	return out.Workloads, nil
}

func (r *RemoteHTTPRuntime) doJSON(ctx context.Context, method, path string, in any, out any) error {
	var body *bytes.Reader
	if in != nil {
		blob, err := json.Marshal(in)
		if err != nil {
			return err
		}
		body = bytes.NewReader(blob)
	} else {
		body = bytes.NewReader(nil)
	}
	req, err := http.NewRequestWithContext(ctx, method, r.baseURL+path, body)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	if r.token != "" {
		req.Header.Set("Authorization", "Bearer "+r.token)
	}
	resp, err := r.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("remote runtime status %d", resp.StatusCode)
	}
	if out == nil {
		return nil
	}
	return json.NewDecoder(resp.Body).Decode(out)
}
