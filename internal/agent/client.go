package agent

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"minikube-orchestrator/internal/model"
)

type Client struct {
	baseURL string
	token   string
	http    *http.Client
}

func NewClient(baseURL, token string) *Client {
	return NewClientWithHTTPClient(baseURL, token, nil)
}

func NewClientWithHTTPClient(baseURL, token string, httpClient *http.Client) *Client {
	if httpClient == nil {
		httpClient = &http.Client{Timeout: 20 * time.Second}
	}
	return &Client{
		baseURL: strings.TrimRight(baseURL, "/"),
		token:   token,
		http:    httpClient,
	}
}

func (c *Client) RegisterNode(node model.Node) error {
	return c.post("/v1/nodes/register", node, nil)
}

func (c *Client) Heartbeat(nodeID string, used model.Resource) error {
	body := map[string]any{"used": used}
	return c.post("/v1/nodes/"+nodeID+"/heartbeat", body, nil)
}

func (c *Client) PollAssignments(nodeID string, max int) ([]model.Assignment, error) {
	url := fmt.Sprintf("%s/v1/workers/%s/assignments?max=%d", c.baseURL, nodeID, max)
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	c.attachAuth(req)
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		blob, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("poll assignments failed: %s", strings.TrimSpace(string(blob)))
	}
	var payload struct {
		Assignments []model.Assignment `json:"assignments"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return nil, err
	}
	return payload.Assignments, nil
}

func (c *Client) ListNodeWorkloads(nodeID string) ([]model.Workload, error) {
	url := fmt.Sprintf("%s/v1/workers/%s/workloads", c.baseURL, nodeID)
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	c.attachAuth(req)
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		blob, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("list node workloads failed: %s", strings.TrimSpace(string(blob)))
	}
	var payload struct {
		Workloads []model.Workload `json:"workloads"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return nil, err
	}
	return payload.Workloads, nil
}

func (c *Client) ReportWorkloadStatus(workloadID string, status model.WorkloadStatus, containerID string) error {
	body := map[string]any{"status": status, "containerId": containerID}
	return c.post("/v1/workloads/"+workloadID+"/status", body, nil)
}

func (c *Client) post(path string, in any, out any) error {
	blob, err := json.Marshal(in)
	if err != nil {
		return err
	}
	url := c.baseURL + path
	req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(blob))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	c.attachAuth(req)

	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("request failed (%d): %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	if out != nil {
		return json.NewDecoder(resp.Body).Decode(out)
	}
	return nil
}

func (c *Client) attachAuth(req *http.Request) {
	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}
	req.Header.Set("X-Trace-ID", newTraceID())
}

func newTraceID() string {
	b := make([]byte, 12)
	if _, err := rand.Read(b); err != nil {
		return fmt.Sprintf("trace-%d", time.Now().UnixNano())
	}
	return "trace-" + hex.EncodeToString(b)
}
