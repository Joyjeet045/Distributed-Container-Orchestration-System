package api

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"minikube-orchestrator/internal/auth"
	"minikube-orchestrator/internal/model"
	"minikube-orchestrator/internal/service"
	"minikube-orchestrator/internal/store"
)

type HTTPServer struct {
	orch      *service.Orchestrator
	verifier  *auth.Verifier
	apiPrefix string
	mux       *http.ServeMux
	bmu       sync.Mutex
	breakers  map[string]breakerState
}

type breakerState struct {
	Failures   int
	OpenUntil  time.Time
	LastReason string
}

func NewHTTPServer(orch *service.Orchestrator, verifier *auth.Verifier) *HTTPServer {
	s := &HTTPServer{orch: orch, verifier: verifier, apiPrefix: "/v1", mux: http.NewServeMux(), breakers: map[string]breakerState{}}
	s.routes()
	return s
}

func (s *HTTPServer) Handler() http.Handler {
	h := http.Handler(s.mux)
	h = s.authorizationMiddleware(h)
	h = s.auditMiddleware(h)
	h = s.authMiddleware(h)
	h = s.traceMiddleware(h)
	h = s.versionMiddleware(h)
	return h
}

func (s *HTTPServer) traceMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		traceID := strings.TrimSpace(r.Header.Get("X-Trace-ID"))
		if traceID == "" {
			traceID = "trace-local-" + strconv.FormatInt(time.Now().UnixNano(), 10)
		}
		w.Header().Set("X-Trace-ID", traceID)
		start := time.Now()
		next.ServeHTTP(w, r)
		log.Printf("trace id=%s method=%s path=%s latency_ms=%d", traceID, r.Method, r.URL.Path, time.Since(start).Milliseconds())
	})
}

func (s *HTTPServer) routes() {
	s.mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	s.mux.HandleFunc("/metrics", s.handlePrometheusMetrics)

	s.mux.HandleFunc("/v1/nodes/register", s.handleRegisterNode)
	s.mux.HandleFunc("/v1/nodes/", s.handleNodeSubroutes)
	s.mux.HandleFunc("/v1/deployments", s.handleDeployments)
	s.mux.HandleFunc("/v1/deployments/", s.handleDeploymentByID)
	s.mux.HandleFunc("/v1/autoscalers", s.handleAutoscalers)
	s.mux.HandleFunc("/v1/autoscalers/", s.handleAutoscalerByID)
	s.mux.HandleFunc("/v1/metrics/deployments", s.handleDeploymentMetrics)
	s.mux.HandleFunc("/v1/metrics/prometheus", s.handlePrometheusMetrics)
	s.mux.HandleFunc("/v1/health/nodes/trends", s.handleNodeHealthTrends)
	s.mux.HandleFunc("/v1/cluster", s.handleCluster)
	s.mux.HandleFunc("/v1/workers/", s.handleWorkerSubroutes)
	s.mux.HandleFunc("/v1/workloads/", s.handleWorkloadSubroutes)
	s.mux.HandleFunc("/v1/services", s.handleServices)
	s.mux.HandleFunc("/v1/services/", s.handleServiceSubroutes)
	s.mux.HandleFunc("/v1/namespaces", s.handleNamespaces)
	s.mux.HandleFunc("/v1/quotas", s.handleQuotas)
	s.mux.HandleFunc("/v1/secrets", s.handleSecrets)
	s.mux.HandleFunc("/v1/persistent-volumes", s.handlePersistentVolumes)
	s.mux.HandleFunc("/v1/persistent-volume-claims", s.handlePersistentVolumeClaims)
	s.mux.HandleFunc("/v1/network-policies", s.handleNetworkPolicies)
	s.mux.HandleFunc("/v1/jobs", s.handleJobs)
	s.mux.HandleFunc("/v1/cronjobs", s.handleCronJobs)
	s.mux.HandleFunc("/v1/rbac/rolebindings", s.handleRoleBindings)
	s.mux.HandleFunc("/v1/recovery/run", s.handleRecoveryRun)
	s.mux.HandleFunc("/v1/dns/resolve", s.handleDNSResolve)
	s.mux.HandleFunc("/v1/dns/sidecar/resolve", s.handleDNSSidecarResolve)
	s.mux.HandleFunc("/v1/network/services/", s.handleNetworkServiceSubroutes)
	s.mux.HandleFunc("/v1/events", s.handleEvents)
	s.mux.HandleFunc("/v1/events/stream", s.handleEventStream)
}

func (s *HTTPServer) versionMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/api/v1/") {
			r.URL.Path = strings.TrimPrefix(r.URL.Path, "/api")
		} else if r.URL.Path == "/api/v1" {
			r.URL.Path = "/v1"
		}

		if v := strings.TrimSpace(r.Header.Get("X-API-Version")); v != "" && v != "v1" && v != "1" {
			writeAPIError(w, http.StatusBadRequest, "unsupported_api_version", "Unsupported API version", "supported version: v1")
			return
		}

		w.Header().Set("X-API-Version", "v1")
		next.ServeHTTP(w, r)
	})
}

func (s *HTTPServer) authMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/healthz" || r.URL.Path == "/metrics" {
			next.ServeHTTP(w, r)
			return
		}

		principal, err := s.verifier.VerifyBearer(r.Header.Get("Authorization"))
		if err != nil {
			log.Printf("audit event=auth_failed method=%s path=%s remote=%s reason=%q", r.Method, r.URL.Path, r.RemoteAddr, err.Error())
			writeAPIError(w, http.StatusUnauthorized, "unauthorized", "Authorization failed", err.Error())
			return
		}
		if strings.TrimSpace(principal.Subject) != "" {
			namespace := requestNamespace(r)
			boundRoles, berr := s.orch.EffectiveRolesForSubject(r.Context(), principal.Subject, namespace)
			if berr == nil && len(boundRoles) > 0 {
				principal.Roles = mergeRoles(principal.Roles, boundRoles)
			}
		}
		next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), principalContextKey{}, principal)))
	})
}

func requestNamespace(r *http.Request) string {
	ns := strings.TrimSpace(r.URL.Query().Get("namespace"))
	if ns != "" {
		return ns
	}
	ns = strings.TrimSpace(r.Header.Get("X-Namespace"))
	if ns != "" {
		return ns
	}
	return "default"
}

func mergeRoles(existing, incoming []string) []string {
	seen := map[string]struct{}{}
	out := make([]string, 0, len(existing)+len(incoming))
	for _, r := range existing {
		k := strings.ToLower(strings.TrimSpace(r))
		if k == "" {
			continue
		}
		if _, ok := seen[k]; ok {
			continue
		}
		seen[k] = struct{}{}
		out = append(out, r)
	}
	for _, r := range incoming {
		k := strings.ToLower(strings.TrimSpace(r))
		if k == "" {
			continue
		}
		if _, ok := seen[k]; ok {
			continue
		}
		seen[k] = struct{}{}
		out = append(out, r)
	}
	return out
}

func (s *HTTPServer) authorizationMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/healthz" || r.URL.Path == "/metrics" {
			next.ServeHTTP(w, r)
			return
		}
		principal, ok := principalFromContext(r.Context())
		if !ok {
			writeAPIError(w, http.StatusUnauthorized, "unauthorized", "Authorization failed", "missing principal")
			return
		}
		if !isAuthorized(principal, r.Method, r.URL.Path) {
			log.Printf("audit event=authorization_denied method=%s path=%s actor=%s roles=%q", r.Method, r.URL.Path, principal.Subject, strings.Join(principal.Roles, ","))
			writeAPIError(w, http.StatusForbidden, "forbidden", "Insufficient role for endpoint", "required role mismatch")
			return
		}
		next.ServeHTTP(w, r)
	})
}

func (s *HTTPServer) auditMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !shouldAuditRequest(r.Method, r.URL.Path) {
			next.ServeHTTP(w, r)
			return
		}

		rec := &statusRecorder{ResponseWriter: w, statusCode: http.StatusOK}
		start := time.Now()
		next.ServeHTTP(rec, r)
		latency := time.Since(start).Milliseconds()

		actor := "anonymous"
		roles := ""
		if principal, ok := principalFromContext(r.Context()); ok {
			if strings.TrimSpace(principal.Subject) != "" {
				actor = principal.Subject
			}
			roles = strings.Join(principal.Roles, ",")
		}

		log.Printf(
			"audit event=api_sensitive_request method=%s path=%s status=%d actor=%s roles=%q remote=%s latency_ms=%d",
			r.Method,
			r.URL.Path,
			rec.statusCode,
			actor,
			roles,
			r.RemoteAddr,
			latency,
		)
	})
}

func shouldAuditRequest(method, path string) bool {
	if path == "/healthz" || path == "/metrics" || strings.HasPrefix(path, "/v1/metrics/") {
		return false
	}
	if strings.HasPrefix(path, "/v1/secrets") {
		return true
	}
	switch method {
	case http.MethodPost, http.MethodPut, http.MethodPatch, http.MethodDelete:
		return strings.HasPrefix(path, "/v1/")
	default:
		return false
	}
}

type statusRecorder struct {
	http.ResponseWriter
	statusCode int
}

func (r *statusRecorder) WriteHeader(code int) {
	r.statusCode = code
	r.ResponseWriter.WriteHeader(code)
}

type principalContextKey struct{}

func principalFromContext(ctx context.Context) (auth.Principal, bool) {
	v := ctx.Value(principalContextKey{})
	p, ok := v.(auth.Principal)
	return p, ok
}

func isAuthorized(p auth.Principal, method, path string) bool {
	required := requiredRoles(method, path)
	if len(required) == 0 {
		return true
	}
	roleSet := map[string]struct{}{}
	for _, r := range p.Roles {
		roleSet[strings.ToLower(strings.TrimSpace(r))] = struct{}{}
	}
	for _, rr := range required {
		if _, ok := roleSet[rr]; ok {
			return true
		}
	}
	return false
}

func requiredRoles(method, path string) []string {
	if method == http.MethodGet && (path == "/v1/cluster" || path == "/v1/deployments" || strings.HasPrefix(path, "/v1/workers/") || strings.HasPrefix(path, "/v1/services") || strings.HasPrefix(path, "/v1/dns/") || strings.HasPrefix(path, "/v1/network/services/")) {
		return []string{"viewer", "operator", "admin"}
	}
	if method == http.MethodGet && (path == "/v1/namespaces" || path == "/v1/quotas" || path == "/v1/secrets") {
		return []string{"viewer", "operator", "admin"}
	}
	if method == http.MethodGet && (path == "/v1/persistent-volumes" || path == "/v1/persistent-volume-claims" || path == "/v1/network-policies" || path == "/v1/jobs" || path == "/v1/cronjobs" || path == "/v1/rbac/rolebindings") {
		return []string{"viewer", "operator", "admin"}
	}
	if method == http.MethodPost && (path == "/v1/namespaces" || path == "/v1/quotas" || path == "/v1/secrets") {
		return []string{"operator", "admin"}
	}
	if method == http.MethodPost && (path == "/v1/persistent-volumes" || path == "/v1/persistent-volume-claims" || path == "/v1/network-policies" || path == "/v1/jobs" || path == "/v1/cronjobs" || path == "/v1/rbac/rolebindings") {
		return []string{"operator", "admin"}
	}
	if method == http.MethodPost && path == "/v1/recovery/run" {
		return []string{"admin"}
	}
	if method == http.MethodGet && (path == "/v1/metrics/prometheus" || path == "/v1/health/nodes/trends") {
		return []string{"viewer", "operator", "admin"}
	}
	if method == http.MethodPost && strings.HasSuffix(path, "/drain") {
		return []string{"operator", "admin"}
	}
	if method == http.MethodPost && strings.HasSuffix(path, "/remove") {
		return []string{"admin"}
	}
	if method == http.MethodPost && path == "/v1/services" {
		return []string{"operator", "admin"}
	}
	if method == http.MethodGet && path == "/v1/autoscalers" {
		return []string{"viewer", "operator", "admin"}
	}
	if method == http.MethodPost && (path == "/v1/autoscalers" || path == "/v1/metrics/deployments") {
		return []string{"operator", "admin"}
	}
	if method == http.MethodDelete && strings.HasPrefix(path, "/v1/autoscalers/") {
		return []string{"admin"}
	}
	if method == http.MethodPut && strings.HasPrefix(path, "/v1/deployments/") {
		return []string{"operator", "admin"}
	}
	if method == http.MethodPost && strings.HasSuffix(path, "/rollback") {
		return []string{"operator", "admin"}
	}
	if method == http.MethodGet && (path == "/v1/events" || path == "/v1/events/stream") {
		return []string{"viewer", "operator", "admin"}
	}
	if method == http.MethodPost && (path == "/v1/deployments" || path == "/v1/nodes/register" || strings.Contains(path, "/heartbeat") || strings.Contains(path, "/status")) {
		return []string{"operator", "admin"}
	}
	if method == http.MethodDelete && strings.HasPrefix(path, "/v1/deployments/") {
		return []string{"admin"}
	}
	return []string{"admin"}
}

func (s *HTTPServer) handleRegisterNode(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeAPIError(w, http.StatusMethodNotAllowed, "method_not_allowed", "Method not allowed", "use POST")
		return
	}
	var node model.Node
	if err := json.NewDecoder(r.Body).Decode(&node); err != nil {
		writeAPIError(w, http.StatusBadRequest, "invalid_payload", "Invalid payload", err.Error())
		return
	}
	if err := validateNode(node); err != nil {
		writeAPIError(w, http.StatusBadRequest, "validation_failed", "Node validation failed", err.Error())
		return
	}
	if err := s.orch.RegisterNode(r.Context(), node); err != nil {
		writeAPIError(w, http.StatusBadRequest, "register_failed", "Failed to register node", err.Error())
		return
	}
	writeJSON(w, http.StatusCreated, map[string]any{"ok": true})
}

func (s *HTTPServer) handleNodeSubroutes(w http.ResponseWriter, r *http.Request) {
	parts := strings.Split(strings.TrimPrefix(r.URL.Path, "/v1/nodes/"), "/")
	if len(parts) != 2 {
		writeAPIError(w, http.StatusNotFound, "not_found", "Route not found", "unknown node subroute")
		return
	}
	if parts[1] == "drain" {
		if r.Method != http.MethodPost {
			writeAPIError(w, http.StatusMethodNotAllowed, "method_not_allowed", "Method not allowed", "use POST")
			return
		}
		if err := s.orch.DrainNode(r.Context(), parts[0]); err != nil {
			writeAPIError(w, http.StatusBadRequest, "drain_failed", "Node drain failed", err.Error())
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"ok": true})
		return
	}
	if parts[1] == "remove" {
		if r.Method != http.MethodPost {
			writeAPIError(w, http.StatusMethodNotAllowed, "method_not_allowed", "Method not allowed", "use POST")
			return
		}
		if err := s.orch.RemoveNode(r.Context(), parts[0]); err != nil {
			writeAPIError(w, http.StatusBadRequest, "remove_failed", "Node remove failed", err.Error())
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"ok": true})
		return
	}
	if parts[1] != "heartbeat" {
		writeAPIError(w, http.StatusNotFound, "not_found", "Route not found", "unknown node subroute")
		return
	}
	if r.Method != http.MethodPost {
		writeAPIError(w, http.StatusMethodNotAllowed, "method_not_allowed", "Method not allowed", "use POST")
		return
	}
	var req struct {
		Used model.Resource `json:"used"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeAPIError(w, http.StatusBadRequest, "invalid_payload", "Invalid payload", err.Error())
		return
	}
	if err := s.orch.HeartbeatNode(r.Context(), parts[0], req.Used); err != nil {
		if errors.Is(err, store.ErrNotFound) {
			writeAPIError(w, http.StatusNotFound, "node_not_found", "Node not found", parts[0])
			return
		}
		writeAPIError(w, http.StatusBadRequest, "heartbeat_failed", "Heartbeat failed", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

func (s *HTTPServer) handleDeployments(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPost:
		var req struct {
			Spec model.DeploymentSpec `json:"spec"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeAPIError(w, http.StatusBadRequest, "invalid_payload", "Invalid payload", err.Error())
			return
		}
		if err := validateDeploymentSpec(req.Spec); err != nil {
			writeAPIError(w, http.StatusBadRequest, "validation_failed", "Deployment spec validation failed", err.Error())
			return
		}
		dep, err := s.orch.CreateDeployment(context.Background(), req.Spec)
		if err != nil {
			writeAPIError(w, http.StatusBadRequest, "create_failed", "Create deployment failed", err.Error())
			return
		}
		writeJSON(w, http.StatusCreated, dep)
	case http.MethodGet:
		deps, err := s.orch.ListDeployments(context.Background())
		if err != nil {
			writeAPIError(w, http.StatusInternalServerError, "list_failed", "List deployments failed", err.Error())
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"deployments": deps})
	default:
		writeAPIError(w, http.StatusMethodNotAllowed, "method_not_allowed", "Method not allowed", "use GET or POST")
	}
}

func (s *HTTPServer) handleDeploymentByID(w http.ResponseWriter, r *http.Request) {
	parts := strings.Split(strings.TrimPrefix(r.URL.Path, "/v1/deployments/"), "/")
	if len(parts) == 0 || strings.TrimSpace(parts[0]) == "" {
		writeAPIError(w, http.StatusNotFound, "not_found", "Route not found", "deployment id is empty")
		return
	}
	id := parts[0]

	if len(parts) == 2 && parts[1] == "rollback" {
		if r.Method != http.MethodPost {
			writeAPIError(w, http.StatusMethodNotAllowed, "method_not_allowed", "Method not allowed", "use POST")
			return
		}
		var req struct {
			Revision int `json:"revision"`
		}
		if r.Body != nil {
			_ = json.NewDecoder(r.Body).Decode(&req)
		}
		updated, err := s.orch.RollbackDeployment(r.Context(), id, req.Revision)
		if err != nil {
			writeAPIError(w, http.StatusBadRequest, "rollback_failed", "Rollback deployment failed", err.Error())
			return
		}
		writeJSON(w, http.StatusOK, updated)
		return
	}

	if len(parts) != 1 {
		writeAPIError(w, http.StatusNotFound, "not_found", "Route not found", "unknown deployment subroute")
		return
	}

	switch r.Method {
	case http.MethodDelete:
		if err := s.orch.DeleteDeployment(context.Background(), id); err != nil {
			writeAPIError(w, http.StatusBadRequest, "delete_failed", "Delete deployment failed", err.Error())
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"ok": true})
	case http.MethodPut:
		var req struct {
			Spec model.DeploymentSpec `json:"spec"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeAPIError(w, http.StatusBadRequest, "invalid_payload", "Invalid payload", err.Error())
			return
		}
		if err := validateDeploymentSpec(req.Spec); err != nil {
			writeAPIError(w, http.StatusBadRequest, "validation_failed", "Deployment spec validation failed", err.Error())
			return
		}
		updated, err := s.orch.UpdateDeployment(r.Context(), id, req.Spec)
		if err != nil {
			writeAPIError(w, http.StatusBadRequest, "update_failed", "Update deployment failed", err.Error())
			return
		}
		writeJSON(w, http.StatusOK, updated)
	default:
		writeAPIError(w, http.StatusMethodNotAllowed, "method_not_allowed", "Method not allowed", "use DELETE, PUT, or POST /rollback")
	}
}

func (s *HTTPServer) handleAutoscalers(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPost:
		var req struct {
			Policy model.AutoscalerPolicy `json:"policy"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeAPIError(w, http.StatusBadRequest, "invalid_payload", "Invalid payload", err.Error())
			return
		}
		if err := validateAutoscalerPolicy(req.Policy); err != nil {
			writeAPIError(w, http.StatusBadRequest, "validation_failed", "Autoscaler policy validation failed", err.Error())
			return
		}
		created, err := s.orch.UpsertAutoscalerPolicy(r.Context(), req.Policy)
		if err != nil {
			writeAPIError(w, http.StatusBadRequest, "autoscaler_upsert_failed", "Failed to upsert autoscaler policy", err.Error())
			return
		}
		writeJSON(w, http.StatusCreated, created)
	case http.MethodGet:
		items, err := s.orch.ListAutoscalerPolicies(r.Context())
		if err != nil {
			writeAPIError(w, http.StatusInternalServerError, "autoscaler_list_failed", "Failed to list autoscaler policies", err.Error())
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"policies": items})
	default:
		writeAPIError(w, http.StatusMethodNotAllowed, "method_not_allowed", "Method not allowed", "use GET or POST")
	}
}

func (s *HTTPServer) handleAutoscalerByID(w http.ResponseWriter, r *http.Request) {
	parts := strings.Split(strings.TrimPrefix(r.URL.Path, "/v1/autoscalers/"), "/")
	if len(parts) != 1 || strings.TrimSpace(parts[0]) == "" {
		writeAPIError(w, http.StatusNotFound, "not_found", "Route not found", "autoscaler id is required")
		return
	}
	if r.Method != http.MethodDelete {
		writeAPIError(w, http.StatusMethodNotAllowed, "method_not_allowed", "Method not allowed", "use DELETE")
		return
	}
	if err := s.orch.DeleteAutoscalerPolicy(r.Context(), parts[0]); err != nil {
		writeAPIError(w, http.StatusBadRequest, "autoscaler_delete_failed", "Failed to delete autoscaler policy", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

func (s *HTTPServer) handleDeploymentMetrics(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeAPIError(w, http.StatusMethodNotAllowed, "method_not_allowed", "Method not allowed", "use POST")
		return
	}
	var req struct {
		Metric model.DeploymentMetricSample `json:"metric"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeAPIError(w, http.StatusBadRequest, "invalid_payload", "Invalid payload", err.Error())
		return
	}
	if err := validateDeploymentMetricSample(req.Metric); err != nil {
		writeAPIError(w, http.StatusBadRequest, "validation_failed", "Deployment metric validation failed", err.Error())
		return
	}
	if err := s.orch.IngestDeploymentMetric(r.Context(), req.Metric); err != nil {
		writeAPIError(w, http.StatusBadRequest, "metric_ingest_failed", "Failed to ingest deployment metric", err.Error())
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]any{"ok": true})
}

func (s *HTTPServer) handlePrometheusMetrics(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeAPIError(w, http.StatusMethodNotAllowed, "method_not_allowed", "Method not allowed", "use GET")
		return
	}
	metrics, err := s.orch.PrometheusMetrics(r.Context())
	if err != nil {
		writeAPIError(w, http.StatusInternalServerError, "metrics_failed", "Failed to render metrics", err.Error())
		return
	}
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(metrics))
}

func (s *HTTPServer) handleNodeHealthTrends(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeAPIError(w, http.StatusMethodNotAllowed, "method_not_allowed", "Method not allowed", "use GET")
		return
	}
	window := 20
	if raw := strings.TrimSpace(r.URL.Query().Get("window")); raw != "" {
		if v, err := strconv.Atoi(raw); err == nil && v > 0 {
			window = v
		}
	}
	trends, err := s.orch.ListNodeHealthTrends(r.Context(), window)
	if err != nil {
		writeAPIError(w, http.StatusInternalServerError, "node_health_trends_failed", "Failed to list node health trends", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"trends": trends})
}

func (s *HTTPServer) handleCluster(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeAPIError(w, http.StatusMethodNotAllowed, "method_not_allowed", "Method not allowed", "use GET")
		return
	}
	state, err := s.orch.ClusterState(context.Background())
	if err != nil {
		writeAPIError(w, http.StatusInternalServerError, "cluster_failed", "Failed to read cluster state", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, state)
}

func (s *HTTPServer) handleWorkerSubroutes(w http.ResponseWriter, r *http.Request) {
	parts := strings.Split(strings.TrimPrefix(r.URL.Path, "/v1/workers/"), "/")
	if len(parts) != 2 {
		writeAPIError(w, http.StatusNotFound, "not_found", "Route not found", "unknown worker subroute")
		return
	}
	if r.Method != http.MethodGet {
		writeAPIError(w, http.StatusMethodNotAllowed, "method_not_allowed", "Method not allowed", "use GET")
		return
	}
	if parts[1] == "assignments" {
		max := 10
		if raw := r.URL.Query().Get("max"); raw != "" {
			if v, err := strconv.Atoi(raw); err == nil && v > 0 {
				max = v
			}
		}
		items, err := s.orch.PollAssignments(r.Context(), parts[0], max)
		if err != nil {
			writeAPIError(w, http.StatusInternalServerError, "poll_failed", "Failed to poll assignments", err.Error())
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"assignments": items})
		return
	}
	if parts[1] == "workloads" {
		items, err := s.orch.ListNodeWorkloads(r.Context(), parts[0])
		if err != nil {
			writeAPIError(w, http.StatusInternalServerError, "workloads_failed", "Failed to list desired node workloads", err.Error())
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"workloads": items})
		return
	}

	writeAPIError(w, http.StatusNotFound, "not_found", "Route not found", "unknown worker subroute")
}

func (s *HTTPServer) handleWorkloadSubroutes(w http.ResponseWriter, r *http.Request) {
	parts := strings.Split(strings.TrimPrefix(r.URL.Path, "/v1/workloads/"), "/")
	if len(parts) != 2 || parts[1] != "status" {
		writeAPIError(w, http.StatusNotFound, "not_found", "Route not found", "unknown workload subroute")
		return
	}
	if r.Method != http.MethodPost {
		writeAPIError(w, http.StatusMethodNotAllowed, "method_not_allowed", "Method not allowed", "use POST")
		return
	}
	var req struct {
		Status      model.WorkloadStatus `json:"status"`
		ContainerID string               `json:"containerId"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeAPIError(w, http.StatusBadRequest, "invalid_payload", "Invalid payload", err.Error())
		return
	}
	if err := s.orch.ReportWorkloadStatus(r.Context(), parts[0], req.Status, req.ContainerID); err != nil {
		writeAPIError(w, http.StatusBadRequest, "status_update_failed", "Workload status update failed", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

func (s *HTTPServer) handleServices(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPost:
		var req struct {
			Spec model.ServiceSpec `json:"spec"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeAPIError(w, http.StatusBadRequest, "invalid_payload", "Invalid payload", err.Error())
			return
		}
		if err := validateServiceSpec(req.Spec); err != nil {
			writeAPIError(w, http.StatusBadRequest, "validation_failed", "Service spec validation failed", err.Error())
			return
		}
		svc, err := s.orch.CreateService(r.Context(), req.Spec)
		if err != nil {
			writeAPIError(w, http.StatusBadRequest, "create_service_failed", "Create service failed", err.Error())
			return
		}
		writeJSON(w, http.StatusCreated, svc)
	case http.MethodGet:
		items, err := s.orch.ListServices(r.Context())
		if err != nil {
			writeAPIError(w, http.StatusInternalServerError, "list_services_failed", "List services failed", err.Error())
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"services": items})
	default:
		writeAPIError(w, http.StatusMethodNotAllowed, "method_not_allowed", "Method not allowed", "use GET or POST")
	}
}

func (s *HTTPServer) handleNamespaces(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPost:
		var req struct {
			Namespace model.Namespace `json:"namespace"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeAPIError(w, http.StatusBadRequest, "invalid_payload", "Invalid payload", err.Error())
			return
		}
		if strings.TrimSpace(req.Namespace.Name) == "" {
			writeAPIError(w, http.StatusBadRequest, "validation_failed", "Namespace validation failed", "namespace name is required")
			return
		}
		ns, err := s.orch.CreateNamespace(r.Context(), req.Namespace)
		if err != nil {
			writeAPIError(w, http.StatusBadRequest, "namespace_create_failed", "Create namespace failed", err.Error())
			return
		}
		writeJSON(w, http.StatusCreated, ns)
	case http.MethodGet:
		items, err := s.orch.ListNamespaces(r.Context())
		if err != nil {
			writeAPIError(w, http.StatusInternalServerError, "namespace_list_failed", "List namespaces failed", err.Error())
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"namespaces": items})
	default:
		writeAPIError(w, http.StatusMethodNotAllowed, "method_not_allowed", "Method not allowed", "use GET or POST")
	}
}

func (s *HTTPServer) handleQuotas(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPost:
		var req struct {
			Quota model.NamespaceQuota `json:"quota"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeAPIError(w, http.StatusBadRequest, "invalid_payload", "Invalid payload", err.Error())
			return
		}
		if strings.TrimSpace(req.Quota.Namespace) == "" {
			writeAPIError(w, http.StatusBadRequest, "validation_failed", "Quota validation failed", "namespace is required")
			return
		}
		quota, err := s.orch.UpsertNamespaceQuota(r.Context(), req.Quota)
		if err != nil {
			writeAPIError(w, http.StatusBadRequest, "quota_upsert_failed", "Upsert quota failed", err.Error())
			return
		}
		writeJSON(w, http.StatusCreated, quota)
	case http.MethodGet:
		items, err := s.orch.ListNamespaceQuotas(r.Context())
		if err != nil {
			writeAPIError(w, http.StatusInternalServerError, "quota_list_failed", "List quotas failed", err.Error())
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"quotas": items})
	default:
		writeAPIError(w, http.StatusMethodNotAllowed, "method_not_allowed", "Method not allowed", "use GET or POST")
	}
}

func (s *HTTPServer) handleSecrets(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPost:
		var req struct {
			Secret model.Secret `json:"secret"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeAPIError(w, http.StatusBadRequest, "invalid_payload", "Invalid payload", err.Error())
			return
		}
		if strings.TrimSpace(req.Secret.Name) == "" {
			writeAPIError(w, http.StatusBadRequest, "validation_failed", "Secret validation failed", "secret name is required")
			return
		}
		secret, err := s.orch.UpsertSecret(r.Context(), req.Secret)
		if err != nil {
			writeAPIError(w, http.StatusBadRequest, "secret_upsert_failed", "Upsert secret failed", err.Error())
			return
		}
		writeJSON(w, http.StatusCreated, secret)
	case http.MethodGet:
		namespace := strings.TrimSpace(r.URL.Query().Get("namespace"))
		items, err := s.orch.ListSecrets(r.Context(), namespace)
		if err != nil {
			writeAPIError(w, http.StatusInternalServerError, "secret_list_failed", "List secrets failed", err.Error())
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"secrets": items})
	default:
		writeAPIError(w, http.StatusMethodNotAllowed, "method_not_allowed", "Method not allowed", "use GET or POST")
	}
}

func (s *HTTPServer) handlePersistentVolumes(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPost:
		var req struct {
			Volume model.PersistentVolume `json:"volume"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeAPIError(w, http.StatusBadRequest, "invalid_payload", "Invalid payload", err.Error())
			return
		}
		pv, err := s.orch.UpsertPersistentVolume(r.Context(), req.Volume)
		if err != nil {
			writeAPIError(w, http.StatusBadRequest, "pv_upsert_failed", "Upsert persistent volume failed", err.Error())
			return
		}
		writeJSON(w, http.StatusCreated, pv)
	case http.MethodGet:
		items, err := s.orch.ListPersistentVolumes(r.Context())
		if err != nil {
			writeAPIError(w, http.StatusInternalServerError, "pv_list_failed", "List persistent volumes failed", err.Error())
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"volumes": items})
	default:
		writeAPIError(w, http.StatusMethodNotAllowed, "method_not_allowed", "Method not allowed", "use GET or POST")
	}
}

func (s *HTTPServer) handlePersistentVolumeClaims(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPost:
		var req struct {
			Claim model.PersistentVolumeClaim `json:"claim"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeAPIError(w, http.StatusBadRequest, "invalid_payload", "Invalid payload", err.Error())
			return
		}
		pvc, err := s.orch.UpsertPersistentVolumeClaim(r.Context(), req.Claim)
		if err != nil {
			writeAPIError(w, http.StatusBadRequest, "pvc_upsert_failed", "Upsert persistent volume claim failed", err.Error())
			return
		}
		writeJSON(w, http.StatusCreated, pvc)
	case http.MethodGet:
		namespace := strings.TrimSpace(r.URL.Query().Get("namespace"))
		items, err := s.orch.ListPersistentVolumeClaims(r.Context(), namespace)
		if err != nil {
			writeAPIError(w, http.StatusInternalServerError, "pvc_list_failed", "List persistent volume claims failed", err.Error())
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"claims": items})
	default:
		writeAPIError(w, http.StatusMethodNotAllowed, "method_not_allowed", "Method not allowed", "use GET or POST")
	}
}

func (s *HTTPServer) handleNetworkPolicies(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPost:
		var req struct {
			Policy model.NetworkPolicy `json:"policy"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeAPIError(w, http.StatusBadRequest, "invalid_payload", "Invalid payload", err.Error())
			return
		}
		policy, err := s.orch.UpsertNetworkPolicy(r.Context(), req.Policy)
		if err != nil {
			writeAPIError(w, http.StatusBadRequest, "network_policy_upsert_failed", "Upsert network policy failed", err.Error())
			return
		}
		writeJSON(w, http.StatusCreated, policy)
	case http.MethodGet:
		namespace := strings.TrimSpace(r.URL.Query().Get("namespace"))
		items, err := s.orch.ListNetworkPolicies(r.Context(), namespace)
		if err != nil {
			writeAPIError(w, http.StatusInternalServerError, "network_policy_list_failed", "List network policies failed", err.Error())
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"policies": items})
	default:
		writeAPIError(w, http.StatusMethodNotAllowed, "method_not_allowed", "Method not allowed", "use GET or POST")
	}
}

func (s *HTTPServer) handleJobs(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPost:
		var req struct {
			Job model.Job `json:"job"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeAPIError(w, http.StatusBadRequest, "invalid_payload", "Invalid payload", err.Error())
			return
		}
		job, err := s.orch.UpsertJob(r.Context(), req.Job)
		if err != nil {
			writeAPIError(w, http.StatusBadRequest, "job_upsert_failed", "Upsert job failed", err.Error())
			return
		}
		writeJSON(w, http.StatusCreated, job)
	case http.MethodGet:
		namespace := strings.TrimSpace(r.URL.Query().Get("namespace"))
		items, err := s.orch.ListJobs(r.Context(), namespace)
		if err != nil {
			writeAPIError(w, http.StatusInternalServerError, "job_list_failed", "List jobs failed", err.Error())
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"jobs": items})
	default:
		writeAPIError(w, http.StatusMethodNotAllowed, "method_not_allowed", "Method not allowed", "use GET or POST")
	}
}

func (s *HTTPServer) handleCronJobs(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPost:
		var req struct {
			CronJob model.CronJob `json:"cronJob"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeAPIError(w, http.StatusBadRequest, "invalid_payload", "Invalid payload", err.Error())
			return
		}
		cj, err := s.orch.UpsertCronJob(r.Context(), req.CronJob)
		if err != nil {
			writeAPIError(w, http.StatusBadRequest, "cronjob_upsert_failed", "Upsert cronjob failed", err.Error())
			return
		}
		writeJSON(w, http.StatusCreated, cj)
	case http.MethodGet:
		namespace := strings.TrimSpace(r.URL.Query().Get("namespace"))
		items, err := s.orch.ListCronJobs(r.Context(), namespace)
		if err != nil {
			writeAPIError(w, http.StatusInternalServerError, "cronjob_list_failed", "List cronjobs failed", err.Error())
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"cronJobs": items})
	default:
		writeAPIError(w, http.StatusMethodNotAllowed, "method_not_allowed", "Method not allowed", "use GET or POST")
	}
}

func (s *HTTPServer) handleRoleBindings(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPost:
		var req struct {
			Binding model.RoleBinding `json:"binding"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeAPIError(w, http.StatusBadRequest, "invalid_payload", "Invalid payload", err.Error())
			return
		}
		binding, err := s.orch.UpsertRoleBinding(r.Context(), req.Binding)
		if err != nil {
			writeAPIError(w, http.StatusBadRequest, "role_binding_upsert_failed", "Upsert role binding failed", err.Error())
			return
		}
		writeJSON(w, http.StatusCreated, binding)
	case http.MethodGet:
		namespace := strings.TrimSpace(r.URL.Query().Get("namespace"))
		items, err := s.orch.ListRoleBindings(r.Context(), namespace)
		if err != nil {
			writeAPIError(w, http.StatusInternalServerError, "role_binding_list_failed", "List role bindings failed", err.Error())
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"bindings": items})
	default:
		writeAPIError(w, http.StatusMethodNotAllowed, "method_not_allowed", "Method not allowed", "use GET or POST")
	}
}

func (s *HTTPServer) handleRecoveryRun(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeAPIError(w, http.StatusMethodNotAllowed, "method_not_allowed", "Method not allowed", "use POST")
		return
	}
	if err := s.orch.RecoverCluster(r.Context()); err != nil {
		writeAPIError(w, http.StatusInternalServerError, "recovery_failed", "Cluster recovery run failed", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

func (s *HTTPServer) handleServiceSubroutes(w http.ResponseWriter, r *http.Request) {
	parts := strings.Split(strings.TrimPrefix(r.URL.Path, "/v1/services/"), "/")
	if len(parts) != 2 {
		writeAPIError(w, http.StatusNotFound, "not_found", "Route not found", "unknown service subroute")
		return
	}
	serviceID := parts[0]
	subroute := parts[1]

	if subroute == "endpoints" {
		if r.Method != http.MethodGet {
			writeAPIError(w, http.StatusMethodNotAllowed, "method_not_allowed", "Method not allowed", "use GET")
			return
		}
		items, err := s.orch.ListServiceEndpoints(r.Context(), serviceID)
		if err != nil {
			if errors.Is(err, store.ErrNotFound) {
				writeAPIError(w, http.StatusNotFound, "service_not_found", "Service not found", serviceID)
				return
			}
			writeAPIError(w, http.StatusInternalServerError, "list_endpoints_failed", "List service endpoints failed", err.Error())
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"endpoints": items})
		return
	}

	if subroute == "proxy-target" {
		if r.Method != http.MethodGet {
			writeAPIError(w, http.StatusMethodNotAllowed, "method_not_allowed", "Method not allowed", "use GET")
			return
		}
		strategy := strings.TrimSpace(r.URL.Query().Get("strategy"))
		ep, err := s.orch.SelectServiceEndpoint(r.Context(), serviceID, strategy)
		if err != nil {
			if errors.Is(err, store.ErrNotFound) {
				writeAPIError(w, http.StatusNotFound, "service_not_found", "Service not found", serviceID)
				return
			}
			writeAPIError(w, http.StatusBadRequest, "select_target_failed", "Failed to select service endpoint", err.Error())
			return
		}
		defer s.orch.ReleaseServiceEndpoint(r.Context(), ep.ID)
		writeJSON(w, http.StatusOK, map[string]any{"endpoint": ep})
		return
	}

	writeAPIError(w, http.StatusNotFound, "not_found", "Route not found", "unknown service subroute")
}

func (s *HTTPServer) handleDNSResolve(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeAPIError(w, http.StatusMethodNotAllowed, "method_not_allowed", "Method not allowed", "use GET")
		return
	}
	name := strings.TrimSpace(r.URL.Query().Get("name"))
	if name == "" {
		writeAPIError(w, http.StatusBadRequest, "invalid_query", "Invalid query", "name is required")
		return
	}
	dnsName, endpoints, err := s.orch.ResolveServiceName(r.Context(), name)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			writeAPIError(w, http.StatusNotFound, "service_not_found", "Service not found", name)
			return
		}
		writeAPIError(w, http.StatusInternalServerError, "resolve_failed", "Service resolve failed", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"name": dnsName, "endpoints": endpoints})
}

func (s *HTTPServer) handleDNSSidecarResolve(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeAPIError(w, http.StatusMethodNotAllowed, "method_not_allowed", "Method not allowed", "use GET")
		return
	}
	name := strings.TrimSpace(r.URL.Query().Get("name"))
	if name == "" {
		writeAPIError(w, http.StatusBadRequest, "invalid_query", "Invalid query", "name is required")
		return
	}
	dnsName, endpoints, ttl, err := s.orch.ResolveServiceDNS(r.Context(), name)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			writeAPIError(w, http.StatusNotFound, "service_not_found", "Service not found", name)
			return
		}
		writeAPIError(w, http.StatusInternalServerError, "resolve_failed", "Service resolve failed", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"name":          dnsName,
		"endpoints":     endpoints,
		"ttlSeconds":    ttl,
		"cacheStrategy": "sidecar-refresh-before-expiry",
	})
}

func (s *HTTPServer) handleNetworkServiceSubroutes(w http.ResponseWriter, r *http.Request) {
	parts := strings.Split(strings.TrimPrefix(r.URL.Path, "/v1/network/services/"), "/")
	if len(parts) < 2 {
		writeAPIError(w, http.StatusNotFound, "not_found", "Route not found", "unknown network service route")
		return
	}
	if r.Method != http.MethodGet {
		writeAPIError(w, http.StatusMethodNotAllowed, "method_not_allowed", "Method not allowed", "use GET")
		return
	}

	if parts[1] == "connect" {
		strategy := strings.TrimSpace(r.URL.Query().Get("strategy"))
		svc, endpoint, routedHost, routedPort, err := s.orch.RouteServiceTraffic(r.Context(), parts[0], strategy)
		if err != nil {
			if errors.Is(err, store.ErrNotFound) {
				writeAPIError(w, http.StatusNotFound, "service_not_found", "Service not found", parts[0])
				return
			}
			writeAPIError(w, http.StatusBadRequest, "connect_failed", "Service connect failed", err.Error())
			return
		}
		defer s.orch.ReleaseServiceEndpoint(r.Context(), endpoint.ID)
		sourceNamespace := strings.TrimSpace(r.URL.Query().Get("sourceNamespace"))
		sourceLabels := parseLabelQuery(r.URL.Query().Get("sourceLabels"))
		allowed, reason, perr := s.orch.IsNetworkAccessAllowed(r.Context(), sourceNamespace, sourceLabels, svc, endpoint.Port)
		if perr != nil {
			writeAPIError(w, http.StatusInternalServerError, "network_policy_check_failed", "Network policy check failed", perr.Error())
			return
		}
		if !allowed {
			writeAPIError(w, http.StatusForbidden, "network_policy_denied", "Network policy denied traffic", reason)
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{
			"service":    svc,
			"endpoint":   endpoint,
			"routedHost": routedHost,
			"routedPort": routedPort,
		})
		return
	}

	if parts[1] == "proxy" {
		strategy := strings.TrimSpace(r.URL.Query().Get("strategy"))
		retries := parsePositiveIntOrDefault(r.URL.Query().Get("retries"), 1)
		timeoutMs := parsePositiveIntOrDefault(r.URL.Query().Get("timeoutMs"), 1500)
		cbThreshold := parsePositiveIntOrDefault(r.URL.Query().Get("cbThreshold"), 3)
		cbOpenSec := parsePositiveIntOrDefault(r.URL.Query().Get("cbOpenSec"), 15)
		if open, reason := s.circuitOpen(parts[0]); open {
			writeAPIError(w, http.StatusServiceUnavailable, "circuit_open", "Circuit breaker open", reason)
			return
		}
		svc, endpoint, _, _, err := s.orch.RouteServiceTraffic(r.Context(), parts[0], strategy)
		if err != nil {
			if errors.Is(err, store.ErrNotFound) {
				writeAPIError(w, http.StatusNotFound, "service_not_found", "Service not found", parts[0])
				return
			}
			s.recordCircuitFailure(parts[0], cbThreshold, cbOpenSec, err.Error())
			writeAPIError(w, http.StatusBadRequest, "proxy_failed", "Service proxy failed", err.Error())
			return
		}
		defer s.orch.ReleaseServiceEndpoint(r.Context(), endpoint.ID)
		sourceNamespace := strings.TrimSpace(r.URL.Query().Get("sourceNamespace"))
		sourceLabels := parseLabelQuery(r.URL.Query().Get("sourceLabels"))
		allowed, reason, perr := s.orch.IsNetworkAccessAllowed(r.Context(), sourceNamespace, sourceLabels, svc, endpoint.Port)
		if perr != nil {
			writeAPIError(w, http.StatusInternalServerError, "network_policy_check_failed", "Network policy check failed", perr.Error())
			return
		}
		if !allowed {
			writeAPIError(w, http.StatusForbidden, "network_policy_denied", "Network policy denied traffic", reason)
			return
		}

		targetURL := &url.URL{Scheme: "http", Host: endpoint.Address + ":" + strconv.Itoa(endpoint.Port)}
		client := &http.Client{Timeout: time.Duration(timeoutMs) * time.Millisecond}
		var resp *http.Response
		var lastErr error
		attempts := retries + 1
		for attempt := 0; attempt < attempts; attempt++ {
			proxyReq, reqErr := http.NewRequestWithContext(r.Context(), http.MethodGet, targetURL.String(), nil)
			if reqErr != nil {
				writeAPIError(w, http.StatusInternalServerError, "proxy_request_failed", "Proxy request build failed", reqErr.Error())
				return
			}
			resp, lastErr = client.Do(proxyReq)
			if lastErr == nil && resp != nil && resp.StatusCode < 500 {
				break
			}
			if resp != nil {
				_ = resp.Body.Close()
			}
		}
		if lastErr != nil {
			s.recordCircuitFailure(parts[0], cbThreshold, cbOpenSec, lastErr.Error())
			writeAPIError(w, http.StatusBadGateway, "proxy_upstream_failed", "Upstream request failed", lastErr.Error())
			return
		}
		if resp == nil {
			s.recordCircuitFailure(parts[0], cbThreshold, cbOpenSec, "empty upstream response")
			writeAPIError(w, http.StatusBadGateway, "proxy_upstream_failed", "Upstream request failed", "empty upstream response")
			return
		}
		defer resp.Body.Close()
		if resp.StatusCode >= 500 {
			s.recordCircuitFailure(parts[0], cbThreshold, cbOpenSec, "upstream 5xx")
		} else {
			s.recordCircuitSuccess(parts[0])
		}

		for key, values := range resp.Header {
			for _, v := range values {
				w.Header().Add(key, v)
			}
		}
		w.Header().Set("X-Orch-Proxied-Service", parts[0])
		w.Header().Set("X-Orch-Proxy-Retries", strconv.Itoa(retries))
		w.WriteHeader(resp.StatusCode)
		if _, err := io.Copy(w, resp.Body); err != nil {
			log.Printf("proxy body copy failed: %v", err)
		}
		return
	}

	writeAPIError(w, http.StatusNotFound, "not_found", "Route not found", "unknown network service route")
	return
}

func (s *HTTPServer) handleEvents(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeAPIError(w, http.StatusMethodNotAllowed, "method_not_allowed", "Method not allowed", "use GET")
		return
	}
	limit := 100
	if raw := strings.TrimSpace(r.URL.Query().Get("limit")); raw != "" {
		v, err := strconv.Atoi(raw)
		if err == nil && v > 0 {
			limit = v
		}
	}
	events, err := s.orch.ListEvents(r.Context(), limit)
	if err != nil {
		writeAPIError(w, http.StatusInternalServerError, "events_failed", "Failed to list events", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"events": events})
}

func (s *HTTPServer) handleEventStream(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeAPIError(w, http.StatusMethodNotAllowed, "method_not_allowed", "Method not allowed", "use GET")
		return
	}
	flusher, ok := w.(http.Flusher)
	if !ok {
		writeAPIError(w, http.StatusInternalServerError, "stream_unsupported", "Streaming unsupported", "http flusher unavailable")
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	eventCh, unsubscribe := s.orch.SubscribeEvents(64)
	defer unsubscribe()

	keepAlive := time.NewTicker(20 * time.Second)
	defer keepAlive.Stop()

	for {
		select {
		case <-r.Context().Done():
			return
		case <-keepAlive.C:
			_, _ = w.Write([]byte(": keepalive\n\n"))
			flusher.Flush()
		case evt, ok := <-eventCh:
			if !ok {
				return
			}
			blob, err := json.Marshal(evt)
			if err != nil {
				continue
			}
			_, _ = w.Write([]byte("event: orchestration\n"))
			_, _ = w.Write([]byte("data: " + string(blob) + "\n\n"))
			flusher.Flush()
		}
	}
}

func parsePositiveIntOrDefault(raw string, fallback int) int {
	v, err := strconv.Atoi(strings.TrimSpace(raw))
	if err != nil || v <= 0 {
		return fallback
	}
	return v
}

func parseLabelQuery(raw string) map[string]string {
	out := map[string]string{}
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return out
	}
	for _, seg := range strings.Split(raw, ",") {
		parts := strings.SplitN(strings.TrimSpace(seg), "=", 2)
		if len(parts) != 2 {
			continue
		}
		k := strings.TrimSpace(parts[0])
		v := strings.TrimSpace(parts[1])
		if k != "" && v != "" {
			out[k] = v
		}
	}
	return out
}

func (s *HTTPServer) circuitOpen(serviceName string) (bool, string) {
	s.bmu.Lock()
	defer s.bmu.Unlock()
	b := s.breakers[serviceName]
	if b.OpenUntil.After(time.Now().UTC()) {
		return true, b.LastReason
	}
	return false, ""
}

func (s *HTTPServer) recordCircuitFailure(serviceName string, threshold, openSec int, reason string) {
	s.bmu.Lock()
	defer s.bmu.Unlock()
	b := s.breakers[serviceName]
	b.Failures++
	b.LastReason = reason
	if b.Failures >= threshold {
		b.OpenUntil = time.Now().UTC().Add(time.Duration(openSec) * time.Second)
		b.Failures = 0
	}
	s.breakers[serviceName] = b
}

func (s *HTTPServer) recordCircuitSuccess(serviceName string) {
	s.bmu.Lock()
	defer s.bmu.Unlock()
	b := s.breakers[serviceName]
	b.Failures = 0
	b.OpenUntil = time.Time{}
	b.LastReason = ""
	s.breakers[serviceName] = b
}

func writeJSON(w http.ResponseWriter, code int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	if err := json.NewEncoder(w).Encode(payload); err != nil {
		log.Printf("json encode error: %v", err)
	}
}
