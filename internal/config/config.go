package config

import (
	"os"
	"strconv"
	"strings"
	"time"

	"minikube-orchestrator/internal/model"
)

type APIServerConfig struct {
	HTTPAddr        string
	HTTPSEnabled    bool
	HTTPSAddr       string
	TLSCertFile     string
	TLSKeyFile      string
	TLSClientCAFile string
	GRPCAddr        string
	DNSAddr         string
	EnableDNSServer bool
	DBPath          string
	APIToken        string
	AuthMode        string
	JWTSecret       string
	JWTIssuer       string
	JWTAudience     string
	JWKSURL         string
	SchedulerPolicy string
	ControllerID    string
	ControllerShard int
	ControllerTotal int
	ControllerLease int
	DNSExtraAddrs   []string
}

type WorkerConfig struct {
	WorkerID          string
	ServerURL         string
	TLSServerName     string
	TLSCACertFile     string
	TLSClientCertFile string
	TLSClientKeyFile  string
	NodeLabels        map[string]string
	NodeTaints        []model.Taint
	HeartbeatInterval time.Duration
	AgentToken        string
	RuntimeDriver     string
	RestartMaxRetries int
	RestartBackoffMS  int
	MetricsAddr       string
}

func LoadAPIServerConfig() APIServerConfig {
	return APIServerConfig{
		HTTPAddr:        envOrDefault("ORCH_HTTP_ADDR", ":8080"),
		HTTPSEnabled:    strings.EqualFold(envOrDefault("ORCH_HTTPS_ENABLED", "false"), "true"),
		HTTPSAddr:       envOrDefault("ORCH_HTTPS_ADDR", ":8443"),
		TLSCertFile:     envOrDefault("ORCH_TLS_CERT_FILE", ""),
		TLSKeyFile:      envOrDefault("ORCH_TLS_KEY_FILE", ""),
		TLSClientCAFile: envOrDefault("ORCH_TLS_CLIENT_CA_FILE", ""),
		GRPCAddr:        envOrDefault("ORCH_GRPC_ADDR", ":9090"),
		DNSAddr:         envOrDefault("ORCH_DNS_ADDR", ":1053"),
		EnableDNSServer: strings.EqualFold(envOrDefault("ORCH_ENABLE_DNS_SERVER", "true"), "true"),
		DBPath:          envOrDefault("ORCH_DB_PATH", "./badger"),
		APIToken:        envOrDefault("ORCH_API_TOKEN", "dev-token"),
		AuthMode:        envOrDefault("ORCH_AUTH_MODE", "static"),
		JWTSecret:       envOrDefault("ORCH_JWT_SECRET", ""),
		JWTIssuer:       envOrDefault("ORCH_JWT_ISSUER", ""),
		JWTAudience:     envOrDefault("ORCH_JWT_AUDIENCE", ""),
		JWKSURL:         envOrDefault("ORCH_JWKS_URL", ""),
		SchedulerPolicy: envOrDefault("ORCH_SCHEDULER_POLICY", "least-loaded"),
		ControllerID:    envOrDefault("ORCH_CONTROLLER_ID", "controller-1"),
		ControllerShard: parseNonNegativeInt(envOrDefault("ORCH_CONTROLLER_SHARD_INDEX", "0"), 0),
		ControllerTotal: parsePositiveInt(envOrDefault("ORCH_CONTROLLER_SHARD_TOTAL", "1"), 1),
		ControllerLease: parsePositiveInt(envOrDefault("ORCH_CONTROLLER_LEASE_SECONDS", "10"), 10),
		DNSExtraAddrs:   parseCSV(envOrDefault("ORCH_DNS_EXTRA_ADDRS", "")),
	}
}

func LoadWorkerConfig() WorkerConfig {
	heartbeat := envOrDefault("ORCH_HEARTBEAT_SECONDS", "5")
	heartbeatSec, err := strconv.Atoi(heartbeat)
	if err != nil || heartbeatSec <= 0 {
		heartbeatSec = 5
	}

	return WorkerConfig{
		WorkerID:          envOrDefault("ORCH_WORKER_ID", "worker-local"),
		ServerURL:         strings.TrimRight(envOrDefault("ORCH_SERVER_URL", "http://localhost:8080"), "/"),
		TLSServerName:     envOrDefault("ORCH_TLS_SERVER_NAME", ""),
		TLSCACertFile:     envOrDefault("ORCH_TLS_CA_CERT_FILE", ""),
		TLSClientCertFile: envOrDefault("ORCH_TLS_CLIENT_CERT_FILE", ""),
		TLSClientKeyFile:  envOrDefault("ORCH_TLS_CLIENT_KEY_FILE", ""),
		NodeLabels:        parseLabels(envOrDefault("ORCH_NODE_LABELS", "")),
		NodeTaints:        parseTaints(envOrDefault("ORCH_NODE_TAINTS", "")),
		HeartbeatInterval: time.Duration(heartbeatSec) * time.Second,
		AgentToken:        envOrDefault("ORCH_AGENT_TOKEN", "dev-token"),
		RuntimeDriver:     envOrDefault("ORCH_RUNTIME_DRIVER", "docker"),
		RestartMaxRetries: parsePositiveInt(envOrDefault("ORCH_RESTART_MAX_RETRIES", "3"), 3),
		RestartBackoffMS:  parsePositiveInt(envOrDefault("ORCH_RESTART_BACKOFF_MS", "250"), 250),
		MetricsAddr:       envOrDefault("ORCH_WORKER_METRICS_ADDR", ":8081"),
	}
}

func parsePositiveInt(raw string, fallback int) int {
	v, err := strconv.Atoi(strings.TrimSpace(raw))
	if err != nil || v <= 0 {
		return fallback
	}
	return v
}

func parseNonNegativeInt(raw string, fallback int) int {
	v, err := strconv.Atoi(strings.TrimSpace(raw))
	if err != nil || v < 0 {
		return fallback
	}
	return v
}

func parseCSV(raw string) []string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil
	}
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		v := strings.TrimSpace(p)
		if v != "" {
			out = append(out, v)
		}
	}
	return out
}

func envOrDefault(key, fallback string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return fallback
}

func parseLabels(raw string) map[string]string {
	labels := map[string]string{}
	if strings.TrimSpace(raw) == "" {
		return labels
	}

	for _, segment := range strings.Split(raw, ",") {
		pair := strings.SplitN(strings.TrimSpace(segment), "=", 2)
		if len(pair) != 2 {
			continue
		}
		k := strings.TrimSpace(pair[0])
		v := strings.TrimSpace(pair[1])
		if k == "" || v == "" {
			continue
		}
		labels[k] = v
	}
	return labels
}

func parseTaints(raw string) []model.Taint {
	if strings.TrimSpace(raw) == "" {
		return nil
	}
	items := []model.Taint{}
	for _, seg := range strings.Split(raw, ",") {
		parts := strings.Split(strings.TrimSpace(seg), ":")
		if len(parts) != 2 {
			continue
		}
		kv := strings.SplitN(strings.TrimSpace(parts[0]), "=", 2)
		if len(kv) != 2 {
			continue
		}
		key := strings.TrimSpace(kv[0])
		val := strings.TrimSpace(kv[1])
		effect := strings.TrimSpace(parts[1])
		if key == "" || effect == "" {
			continue
		}
		items = append(items, model.Taint{Key: key, Value: val, Effect: effect})
	}
	return items
}
