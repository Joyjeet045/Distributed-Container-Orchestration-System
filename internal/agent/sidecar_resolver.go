package agent

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"

	"minikube-orchestrator/internal/model"
)

type SidecarDNSRecord struct {
	Name          string                  `json:"name"`
	TTLSeconds    int                     `json:"ttlSeconds"`
	Endpoints     []model.ServiceEndpoint `json:"endpoints"`
	CacheStrategy string                  `json:"cacheStrategy"`
}

func (c *Client) ResolveForSidecar(serviceName string) (SidecarDNSRecord, error) {
	q := url.QueryEscape(serviceName)
	req, err := http.NewRequest(http.MethodGet, fmt.Sprintf("%s/v1/dns/sidecar/resolve?name=%s", c.baseURL, q), nil)
	if err != nil {
		return SidecarDNSRecord{}, err
	}
	c.attachAuth(req)
	resp, err := c.http.Do(req)
	if err != nil {
		return SidecarDNSRecord{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		blob, _ := io.ReadAll(resp.Body)
		return SidecarDNSRecord{}, fmt.Errorf("sidecar resolve failed: %s", string(blob))
	}
	var out SidecarDNSRecord
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return SidecarDNSRecord{}, err
	}
	return out, nil
}
