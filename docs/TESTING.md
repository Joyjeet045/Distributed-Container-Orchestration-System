# Testing Guide — Distributed Container Orchestration System

This document explains every way to test this system: automated unit/integration/e2e tests, manual REST scenarios using `curl`, live compose verification, and chaos/failure scenarios.

---

## Table of Contents

1. [Running the automated test suite](#1-running-the-automated-test-suite)
2. [Unit test coverage by package](#2-unit-test-coverage-by-package)
3. [Integration test scenarios](#3-integration-test-scenarios)
4. [End-to-end test scenarios](#4-end-to-end-test-scenarios)
5. [Manual REST scenario walkthrough](#5-manual-rest-scenario-walkthrough)
6. [Advanced feature scenarios](#6-advanced-feature-scenarios)
7. [Observability verification](#7-observability-verification)
8. [Security and auth scenarios](#8-security-and-auth-scenarios)
9. [Chaos and failure scenarios](#9-chaos-and-failure-scenarios)
10. [Environment variables reference](#10-environment-variables-reference)

---

## 1. Running the automated test suite

### Prerequisites

- Go 1.21+
- Docker (only needed for Docker runtime tests; all unit/integration tests use in-memory runtime)

### Commands

```bash
# Full suite — all packages
go test -mod=mod ./...

# With verbose output
go test -mod=mod -v ./...

# Single package
go test -mod=mod ./internal/service/...

# Single test by name
go test -mod=mod ./internal/service -run TestRollingUpdateHistoryAndProgress -v

# Race detector (recommended for CI)
go test -mod=mod -race ./...

# Coverage report
go test -mod=mod -coverprofile=coverage.out ./...
go tool cover -html=coverage.out
```

### Expected output (all green)

```
ok  minikube-orchestrator/internal/api
ok  minikube-orchestrator/internal/auth
ok  minikube-orchestrator/internal/dnsserver
ok  minikube-orchestrator/internal/e2e
ok  minikube-orchestrator/internal/loadbalancer
ok  minikube-orchestrator/internal/scheduler
ok  minikube-orchestrator/internal/service
ok  minikube-orchestrator/internal/worker
```

---

## 2. Unit test coverage by package

### `internal/scheduler` — Placement engine

| Test | What it verifies |
|---|---|
| `TestPolicyBestFitPrefersTighterNode` | Best-fit policy selects node with least leftover capacity |
| `TestTaintsRequireMatchingToleration` | Nodes with untolerated taints are excluded |
| `TestCustomPolicyPluginSelectsExpectedNode` | Registered score plugin scores override the default |
| `TestExternalPluginFailureFallsBackToBuiltInPolicy` | External command failure causes transparent fallback |

```bash
go test -mod=mod -v ./internal/scheduler/...
```

### `internal/loadbalancer` — Endpoint selection

| Test | What it verifies |
|---|---|
| `TestRoundRobinSelection` | Sequential calls cycle through all ready endpoints |
| `TestLeastConnectionsSelection` | Endpoint with fewest in-flight requests is selected first |
| `TestHealthAwareFilteringByReadyFlag` | Not-ready endpoints are never selected |

```bash
go test -mod=mod -v ./internal/loadbalancer/...
```

### `internal/auth` — Authentication

| Test | What it verifies |
|---|---|
| `TestOIDCJWKSVerifierAcceptsRS256Token` | Valid RS256 JWT against a mock JWKS endpoint succeeds |
| `TestHybridFallsBackToOIDC` | Hybrid mode tries static + JWT first then OIDC |

```bash
go test -mod=mod -v ./internal/auth/...
```

### `internal/dnsserver` — DNS resolution

| Test | What it verifies |
|---|---|
| `TestDNSServerAnswersARecordForService` | UDP A-record query resolves to correct IP via mock resolver |

```bash
go test -mod=mod -v ./internal/dnsserver/...
```

### `internal/worker` — Assignment execution

| Test | What it verifies |
|---|---|
| `TestRunnerCreateWithBackoffAndDeleteAction` | Create assignment runs container and reports status; delete stops it |
| `TestRunnerRuntimeDriftReconcile` | Extra containers not in desired set are stopped; missing ones reported failed |

```bash
go test -mod=mod -v ./internal/worker/...
```

---

## 3. Integration test scenarios

All tests in `internal/service` and `internal/api` use in-process httptest servers + BadgerDB in a temp directory. No external processes.

### `internal/service` — Orchestrator core

#### Scheduling and placement

```bash
go test -mod=mod -v ./internal/service -run TestPriorityPreemptionAndEvents
```
**Scenario:** Two deployments compete for a node with 1000 millicores. A high-priority deployment (priority=10) needs 600m; a low-priority one already owns 800m. The orchestrator must preempt the low-priority workload to fit the high-priority one and emit a `PreemptedLowerPriorityWorkload` event.

**Expected:** low workload count = 0, high workload count = 1, event `PreemptedLowerPriorityWorkload` present.

---

#### Scale-down and conditions

```bash
go test -mod=mod -v ./internal/service -run TestScaleDownReconcileAndConditions
```
**Scenario:** Deployment created with replicas=3, then updated to replicas=1. Reconciler must terminate excess workloads and report the correct deployment conditions (`Available=true`, `Progressing=false`).

---

#### Deletion finalizer lifecycle

```bash
go test -mod=mod -v ./internal/service -run TestDeletionFinalizerLifecycle
```
**Scenario:** Delete a deployment. Workloads receive terminating assignments. Finalizer blocks physical deletion until all workloads are terminated, then deployment record is removed.

---

#### Event subscription stream

```bash
go test -mod=mod -v ./internal/service -run TestEventSubscriptionStream
```
**Scenario:** Subscribe to event stream, create a deployment, confirm events arrive on the channel including `DeploymentCreated` and `AssignmentEnqueued`.

---

#### Service endpoint reconciliation

```bash
go test -mod=mod -v ./internal/service -run TestServiceEndpointReconcileAndResolve
```
**Scenario:** Register node, create deployment, mark workloads running, create service with matching selector. Reconcile must produce `ServiceEndpoint` records. DNS resolution must return the endpoint.

---

#### Health-aware endpoint selection

```bash
go test -mod=mod -v ./internal/service -run TestServiceEndpointSelectionSkipsUnhealthyNodes
```
**Scenario:** Two endpoints: one on a `Ready` node, one on a `Down` node. Selector must always return the healthy endpoint.

---

#### Readiness probe gating

```bash
go test -mod=mod -v ./internal/service -run TestProbeDrivenEndpointReadinessAndLiveness
```
**Scenario:** Deployment has `initialDelaySeconds=10` readiness probe. Before delay expires, endpoint `Ready=false`. After delay, `Ready=true`. Liveness probe failure marks workload failed.

---

#### Node critical health + remediation

```bash
go test -mod=mod -v ./internal/service -run TestNodeCriticalHealthTriggersDrainRemediation
```
**Scenario:** Node stops sending heartbeats for 2×TTL. Reconciler classifies node as `Critical`, isolates and drains it, terminates workloads and enqueues delete assignments.

---

#### Rolling update progress

```bash
go test -mod=mod -v ./internal/service -run TestRollingUpdateHistoryAndProgress
```
**Scenario:** Update a running deployment image. Reconciler creates new-revision workloads respecting `maxSurge`/`maxUnavailable`. Old workloads are terminated as new ones become running. Revision history grows.

---

#### Rollback and canary strategy

```bash
go test -mod=mod -v ./internal/service -run TestRollbackAndCanaryStrategy
```
**Scenario:** Update deployment. Trigger rollback to revision 1. Confirm spec reverts. Also verify canary strategy limits initial placement to the configured percentage.

---

#### Autoscaling scale-up with cooldown

```bash
go test -mod=mod -v ./internal/service -run TestAutoscalingScaleUpAndCooldown
```
**Scenario:** Ingest CPU metrics above target threshold. Reconcile triggers scale-up. A second reconcile within cooldown period must be blocked.

---

#### Predictive autoscaling

```bash
go test -mod=mod -v ./internal/service -run TestPredictiveAutoscalingScalesFromTrend
```
**Scenario:** Ingest three samples showing a rising CPU trend (0.10 → 0.30 → 0.50). With `PredictiveScaleFactor=8`, the projected next value exceeds target utilization. Reconciler scales up.

---

#### Auto-rollback on liveness failures

```bash
go test -mod=mod -v ./internal/service -run TestAutoRollbackTriggeredByLivenessFailures
```
**Scenario:** Deploy v2. Workload fails liveness probe. `AutoRollbackOnFailure=true` triggers automatic rollback to v1 after failure threshold.

---

#### Progress deadline failure

```bash
go test -mod=mod -v ./internal/service -run TestRolloutProgressDeadlineMarksDeploymentFailed
```
**Scenario:** Set `progressDeadlineSeconds=1`. Update deployment. Workload stays pending. After 1s, reconciler marks deployment `Failed` with `ProgressDeadlineExceeded`.

---

### `internal/api` — HTTP middleware and endpoints

#### API version alias routing

```bash
go test -mod=mod -v ./internal/api -run TestAPIVersionAliasRoute
```
`/api/v1/cluster` rewrites to `/v1/cluster` transparently.

#### Unsupported version rejection

```bash
go test -mod=mod -v ./internal/api -run TestUnsupportedVersionRejected
```
`X-API-Version: v2` returns 400 with `unsupported_api_version` code.

#### Public metrics endpoint

```bash
go test -mod=mod -v ./internal/api -run TestPublicMetricsEndpointDoesNotRequireAuth
```
`GET /metrics` returns Prometheus text without an `Authorization` header.

#### Audit request selection logic

```bash
go test -mod=mod -v ./internal/api -run TestShouldAuditRequest
```
Validates which method+path combinations trigger audit logging vs. which are exempt (health, metrics).

#### JWT role-based access control

```bash
go test -mod=mod -v ./internal/api -run TestJWTAuthorizationBlocksDeleteForViewer
```
A JWT token with `roles=["viewer"]` is rejected with 403 on `DELETE /v1/deployments/{id}`.

#### Validation error shape

```bash
go test -mod=mod -v ./internal/api -run TestValidationErrorShape
```
Malformed deployment spec returns 400 with a structured `{"error":{"code":"validation_failed",...}}` body.

#### Service discovery via REST

```bash
go test -mod=mod -v ./internal/api -run TestServiceDiscoveryEndpoints
```
Full flow: register node → create deployment → mark running → create service → reconcile → list endpoints → DNS resolve → proxy-target selection.

#### Autoscaler endpoints

```bash
go test -mod=mod -v ./internal/api -run TestAutoscalerAndMetricsEndpoints
```
Create policy, ingest metrics, reconcile, confirm replica count change.

#### Namespace / quota / secrets

```bash
go test -mod=mod -v ./internal/api -run TestNamespaceQuotaAndSecretsEndpoints
```
Create namespace, set CPU quota, attempt over-quota deployment (rejected), create within quota (allowed), create and list secrets.

---

## 4. End-to-end test scenarios

These tests in `internal/e2e` spin up a full in-process HTTP server and use the real agent client.

### Full deployment flow

```bash
go test -mod=mod -v ./internal/e2e -run TestEndToEndDeploymentFlow
```
**Full cycle:**
1. Register worker node via agent client
2. Send heartbeat
3. POST deployment via HTTP
4. Worker polls assignments via REST
5. Worker reports `Running` via REST
6. Verify cluster state has running workloads

### Auth enforcement

```bash
go test -mod=mod -v ./internal/e2e -run TestAuthRequired
```
Requests without `Authorization` header return 401.

### Chaos: node loss triggers remediation

```bash
go test -mod=mod -v ./internal/e2e -run TestChaosNodeLossTriggersRemediation
```
**Scenario:**
1. Register node, deploy workloads
2. Simulate node TTL expiry by forcing node `LastSeen` to the past
3. Run reconcile
4. Confirm node status = `Down`, workloads get termination assignments

### Chaos: controller restart preserves state

```bash
go test -mod=mod -v ./internal/e2e -run TestChaosControllerRestartPreservesState
```
**Scenario:**
1. Create deployment and confirm workloads are scheduled
2. Create a new orchestrator pointed at the same BadgerDB directory (simulates restart)
3. Confirm all deployments and workloads are intact after reload from persisted state

---

## 5. Manual REST scenario walkthrough

Start the stack:

```bash
docker compose up -d --build
```

Set a convenience alias:

```bash
# PowerShell
$H = @{ "Authorization" = "Bearer dev-token"; "Content-Type" = "application/json" }
$BASE = "http://localhost:8080"
```

### 5.1 Verify cluster health

```bash
curl http://localhost:8080/healthz
# → ok

curl -H "Authorization: Bearer dev-token" http://localhost:8080/v1/cluster
# → {"nodes":[{"id":"worker-1",...},{"id":"worker-2",...}],"deployments":[],"workloads":[]}
```

### 5.2 Create a deployment

```bash
curl -X POST http://localhost:8080/v1/deployments \
  -H "Authorization: Bearer dev-token" \
  -H "Content-Type: application/json" \
  -d '{
    "spec": {
      "name": "web",
      "image": "nginx:latest",
      "replicas": 2,
      "resources": { "milliCPU": 250, "memoryMB": 256 },
      "labels": { "app": "web" }
    }
  }'
```

**Expected:** 201 response with deployment object including `id`, `status: "Pending"`.

### 5.3 List deployments

```bash
curl -H "Authorization: Bearer dev-token" http://localhost:8080/v1/deployments
```

### 5.4 Scale the deployment

```bash
DEP_ID="<id from 5.2>"
curl -X PUT http://localhost:8080/v1/deployments/$DEP_ID \
  -H "Authorization: Bearer dev-token" \
  -H "Content-Type: application/json" \
  -d '{ "spec": { "name": "web", "image": "nginx:latest", "replicas": 4, "resources": { "milliCPU": 250, "memoryMB": 256 }, "labels": { "app": "web" } } }'
```

### 5.5 Rollback to previous revision

```bash
curl -X POST http://localhost:8080/v1/deployments/$DEP_ID/rollback \
  -H "Authorization: Bearer dev-token" \
  -H "Content-Type: application/json" \
  -d '{ "revision": 1 }'
```

### 5.6 Create a service

```bash
curl -X POST http://localhost:8080/v1/services \
  -H "Authorization: Bearer dev-token" \
  -H "Content-Type: application/json" \
  -d '{
    "spec": {
      "name": "web-svc",
      "selector": { "app": "web" },
      "ports": [{ "name": "http", "port": 80, "targetPort": 80, "protocol": "TCP" }]
    }
  }'
```

### 5.7 Resolve the service via DNS endpoint

```bash
curl -H "Authorization: Bearer dev-token" \
  "http://localhost:8080/v1/dns/resolve?name=web-svc"
# → {"name":"web-svc.default.svc.cluster.local","endpoints":[...]}
```

### 5.8 List events

```bash
curl -H "Authorization: Bearer dev-token" "http://localhost:8080/v1/events?limit=20"
```

### 5.9 Watch event stream (SSE)

```bash
curl -H "Authorization: Bearer dev-token" \
  "http://localhost:8080/v1/events/stream"
# streams: event: orchestration\ndata: {...}\n\n
```

### 5.10 Delete a deployment

```bash
curl -X DELETE -H "Authorization: Bearer dev-token" \
  http://localhost:8080/v1/deployments/$DEP_ID
```

---

## 6. Advanced feature scenarios

### 6.1 Namespace and resource quotas

```bash
# Create namespace
curl -X POST http://localhost:8080/v1/namespaces \
  -H "Authorization: Bearer dev-token" \
  -H "Content-Type: application/json" \
  -d '{ "namespace": { "name": "team-a", "tenant": "acme" } }'

# Set quota: max 2 deployments, 2000 millicores total
curl -X POST http://localhost:8080/v1/quotas \
  -H "Authorization: Bearer dev-token" \
  -H "Content-Type: application/json" \
  -d '{ "quota": { "namespace": "team-a", "maxDeployments": 2, "maxMilliCPU": 2000, "maxMemoryMB": 2048 } }'

# Deploy within quota (succeeds)
curl -X POST http://localhost:8080/v1/deployments \
  -H "Authorization: Bearer dev-token" \
  -H "Content-Type: application/json" \
  -d '{ "spec": { "namespace": "team-a", "name": "app1", "image": "nginx:latest", "replicas": 1, "resources": { "milliCPU": 500, "memoryMB": 512 } } }'

# Third deployment (fails with quota_exceeded)
curl -X POST http://localhost:8080/v1/deployments \
  -H "Authorization: Bearer dev-token" \
  -H "Content-Type: application/json" \
  -d '{ "spec": { "namespace": "team-a", "name": "app3", "image": "nginx:latest", "replicas": 1, "resources": { "milliCPU": 500, "memoryMB": 512 } } }'
# → 400 namespace quota exceeded
```

### 6.2 Secrets with KMS-style encryption

```bash
# Without KMS key set → stored as plaintext
curl -X POST http://localhost:8080/v1/secrets \
  -H "Authorization: Bearer dev-token" \
  -H "Content-Type: application/json" \
  -d '{ "secret": { "namespace": "default", "name": "db-creds", "data": { "password": "s3cr3t" } } }'

# With KMS key (set env before starting server):
# ORCH_KMS_KEY=<base64-encoded-32-byte-key>
# Data is stored encrypted, decrypted transparently on read
curl -H "Authorization: Bearer dev-token" \
  "http://localhost:8080/v1/secrets?namespace=default"
```

### 6.3 Persistent volumes and claims

```bash
# Provision a volume
curl -X POST http://localhost:8080/v1/persistent-volumes \
  -H "Authorization: Bearer dev-token" \
  -H "Content-Type: application/json" \
  -d '{ "volume": { "name": "data-vol-1", "capacityMb": 1024, "storageClass": "fast", "accessMode": "ReadWriteOnce" } }'

# Create a claim
curl -X POST http://localhost:8080/v1/persistent-volume-claims \
  -H "Authorization: Bearer dev-token" \
  -H "Content-Type: application/json" \
  -d '{ "claim": { "namespace": "default", "name": "app-data", "requestedCapacityMb": 512, "storageClass": "fast", "accessMode": "ReadWriteOnce" } }'

# Claim is now Bound — check:
curl -H "Authorization: Bearer dev-token" \
  "http://localhost:8080/v1/persistent-volume-claims?namespace=default"
# → phase: "Bound", volumeName: "data-vol-1"
```

### 6.4 Network policies

```bash
# Allow ingress only from namespace "team-a" on port 80
curl -X POST http://localhost:8080/v1/network-policies \
  -H "Authorization: Bearer dev-token" \
  -H "Content-Type: application/json" \
  -d '{
    "policy": {
      "namespace": "default",
      "name": "allow-team-a",
      "podSelector": { "app": "web" },
      "ingress": [{
        "from": [{ "namespace": "team-a" }],
        "port": 80
      }]
    }
  }'

# Traffic from team-a to web-svc:80 — allowed:
curl -H "Authorization: Bearer dev-token" \
  "http://localhost:8080/v1/network/services/web-svc/connect?sourceNamespace=team-a"

# Traffic from team-b — denied:
curl -H "Authorization: Bearer dev-token" \
  "http://localhost:8080/v1/network/services/web-svc/connect?sourceNamespace=team-b"
# → 403 network_policy_denied
```

### 6.5 Jobs and CronJobs

```bash
# One-time job
curl -X POST http://localhost:8080/v1/jobs \
  -H "Authorization: Bearer dev-token" \
  -H "Content-Type: application/json" \
  -d '{
    "job": {
      "spec": {
        "name": "batch-run-1",
        "namespace": "default",
        "completions": 1,
        "backoffLimit": 2,
        "ttlSeconds": 300,
        "template": {
          "name": "batch", "image": "alpine:latest",
          "replicas": 1, "resources": { "milliCPU": 100, "memoryMB": 128 }
        }
      }
    }
  }'

# Recurring CronJob (every 60 seconds)
curl -X POST http://localhost:8080/v1/cronjobs \
  -H "Authorization: Bearer dev-token" \
  -H "Content-Type: application/json" \
  -d '{
    "cronJob": {
      "namespace": "default",
      "name": "hourly-report",
      "scheduleEverySeconds": 60,
      "historyLimit": 5,
      "template": {
        "name": "report", "image": "alpine:latest",
        "replicas": 1, "resources": { "milliCPU": 50, "memoryMB": 64 }
      }
    }
  }'

# List jobs
curl -H "Authorization: Bearer dev-token" \
  "http://localhost:8080/v1/jobs?namespace=default"
```

### 6.6 RBAC role bindings with inheritance

```bash
# Create a base role binding
curl -X POST http://localhost:8080/v1/rbac/rolebindings \
  -H "Authorization: Bearer dev-token" \
  -H "Content-Type: application/json" \
  -d '{
    "binding": {
      "name": "base-viewer",
      "subject": "ci-bot",
      "roles": ["viewer"],
      "namespace": "default"
    }
  }'

# Create a binding that inherits from base-viewer and adds operator
curl -X POST http://localhost:8080/v1/rbac/rolebindings \
  -H "Authorization: Bearer dev-token" \
  -H "Content-Type: application/json" \
  -d '{
    "binding": {
      "name": "ci-operator",
      "subject": "ci-bot",
      "roles": ["operator"],
      "inherits": ["base-viewer"],
      "namespace": "default"
    }
  }'
# ci-bot now has effective roles: viewer + operator
```

### 6.7 Autoscaling policy

```bash
DEP_ID="<deployment id>"

# Set autoscaler
curl -X POST http://localhost:8080/v1/autoscalers \
  -H "Authorization: Bearer dev-token" \
  -H "Content-Type: application/json" \
  -d '{
    "policy": {
      "deploymentId": "'"$DEP_ID"'",
      "minReplicas": 1,
      "maxReplicas": 6,
      "targetCPUUtilization": 0.7,
      "scaleUpCooldownSec": 30,
      "scaleDownCooldownSec": 60,
      "predictiveScalingEnabled": true,
      "predictiveLookbackSamples": 5,
      "predictiveScaleFactor": 2
    }
  }'

# Ingest metric above threshold
curl -X POST http://localhost:8080/v1/metrics/deployments \
  -H "Authorization: Bearer dev-token" \
  -H "Content-Type: application/json" \
  -d '{
    "metric": {
      "deploymentId": "'"$DEP_ID"'",
      "cpuUsage": 0.90,
      "memoryUsage": 0.50
    }
  }'
```

### 6.8 Node drain and remove

```bash
# Gracefully drain a node (workloads get reassigned)
curl -X POST http://localhost:8080/v1/nodes/worker-1/drain \
  -H "Authorization: Bearer dev-token"

# Remove node from cluster
curl -X POST http://localhost:8080/v1/nodes/worker-1/remove \
  -H "Authorization: Bearer dev-token"
```

### 6.9 Recovery run

After a simulated crash or state inconsistency, trigger a full reconcile + PVC rebind + job loop:

```bash
curl -X POST http://localhost:8080/v1/recovery/run \
  -H "Authorization: Bearer dev-token"
# → {"ok": true}
```

### 6.10 L7 proxy with retry and circuit breaker

```bash
# Proxy to service with 2 retries, 2s timeout, circuit opens after 3 failures
curl -H "Authorization: Bearer dev-token" \
  "http://localhost:8080/v1/network/services/web-svc/proxy?strategy=least-connections&retries=2&timeoutMs=2000&cbThreshold=3&cbOpenSec=30"
```

---

## 7. Observability verification

### Prometheus metrics

```bash
# Internal Prometheus text format
curl http://localhost:8080/metrics

# Authenticated version
curl -H "Authorization: Bearer dev-token" \
  http://localhost:8080/v1/metrics/prometheus

# Prometheus server ready
curl http://localhost:9091/-/ready
# → Prometheus Server is Ready.
```

Available metrics from the control plane:
- `orch_nodes_total` — total registered nodes
- `orch_nodes_ready` — nodes in Ready state
- `orch_deployments_total` — total deployments
- `orch_workloads_running` — running workloads
- `orch_workloads_pending` — pending workloads
- `orch_workloads_failed` — failed workloads
- `orch_autoscaler_policies_total` — configured policies

Worker metrics (per worker on `:8081`):
- `worker_used_millicpu` — current used CPU
- `worker_used_memory_mb` — current used memory
- `worker_identity{node_id="..."}` — label gauge for identification

### Grafana dashboard

1. Open [http://localhost:3000](http://localhost:3000) (admin / admin)
2. Navigate to **Dashboards → Cluster Overview**
3. The pre-provisioned dashboard includes panels for:
   - Node topology and status
   - Resource utilization (CPU/memory)
   - Workload state breakdown
   - Failure and alert events
   - Health/heartbeat age
   - Scaling activity timeline

### Node health trends

```bash
curl -H "Authorization: Bearer dev-token" \
  "http://localhost:8080/v1/health/nodes/trends?window=30"
# → per-node avg/max utilization, anomaly detection flag
```

### Alert rules

Prometheus alert rules are provisioned at `deploy/observability/prometheus/alerts.yml`. They cover:
- Node down for >60s
- High workload failure rate
- Control-plane autoscaler policy count drop

---

## 8. Security and auth scenarios

### Static token (default)

```bash
# Valid
curl -H "Authorization: Bearer dev-token" http://localhost:8080/v1/cluster

# Invalid
curl -H "Authorization: Bearer wrong-token" http://localhost:8080/v1/cluster
# → 401 unauthorized
```

### JWT HMAC mode

```bash
# Start server with:
# ORCH_AUTH_MODE=jwt ORCH_JWT_SECRET=supersecret

# Generate token (any JWT library), e.g. via Go playground or jwt.io
# Claims: {"sub":"alice","roles":["operator"],"exp":<future unix>}
# Signed with HS256 + "supersecret"

curl -H "Authorization: Bearer <token>" http://localhost:8080/v1/cluster
```

### RBAC role enforcement

| Role | Allowed |
|---|---|
| `viewer` | GET on cluster, deployments, services, events, metrics |
| `operator` | All viewer + POST deployments, services, autoscalers, metrics ingest, drain |
| `admin` | All operator + DELETE deployments, autoscalers, remove node |

```bash
# Viewer can GET
curl -H "Authorization: Bearer <viewer-jwt>" http://localhost:8080/v1/cluster  # 200

# Viewer cannot DELETE
curl -X DELETE -H "Authorization: Bearer <viewer-jwt>" \
  http://localhost:8080/v1/deployments/dep-123  # 403 forbidden
```

### Audit log

Every mutating request and secret read produces a structured log line:
```
audit event=api_sensitive_request method=POST path=/v1/deployments status=201 actor=alice roles="operator" remote=172.18.0.1:12345 latency_ms=4
```

### HTTPS + mTLS (optional)

```bash
# Generate self-signed certs for testing
openssl req -x509 -newkey rsa:4096 -keyout server.key -out server.crt -days 365 -nodes -subj "/CN=localhost"

# Start server with TLS
ORCH_HTTPS_ENABLED=true \
ORCH_HTTPS_ADDR=:8443 \
ORCH_TLS_CERT_FILE=server.crt \
ORCH_TLS_KEY_FILE=server.key \
./apiserver

# Connect with TLS
curl --cacert server.crt \
  -H "Authorization: Bearer dev-token" \
  https://localhost:8443/v1/cluster

# mTLS: also set ORCH_TLS_CLIENT_CA_FILE and provide client certs on worker
```

---

## 9. Chaos and failure scenarios

### Scenario A: Node goes silent

1. Start the compose stack
2. Create a deployment with 2 replicas
3. Scale down the worker containers:
   ```bash
   docker compose stop worker-1
   ```
4. Wait 20s for TTL to expire
5. Check cluster state — `worker-1` should show `Down`
6. Workloads on `worker-1` should have termination assignments enqueued
7. Start `worker-1` again — it re-registers and receives new assignments

### Scenario B: Control plane restart with persistent state

1. Start the stack, create several deployments and services
2. Restart the API server container:
   ```bash
   docker compose restart api-server
   ```
3. Immediately query cluster state — all deployments, services, and workloads should be intact (BadgerDB volume persisted)
4. Workers reconnect via heartbeat and continue normal operation

### Scenario C: Resource exhaustion and preemption

1. Create a deployment with high resource request that fills a node
2. Create a higher-priority deployment that needs the same resources
3. The orchestrator should preempt a lower-priority workload and schedule the high-priority one
4. Check events for `PreemptedLowerPriorityWorkload`

### Scenario D: Rolling update with simulated stuck rollout

1. Create deployment with `progressDeadlineSeconds: 10`
2. Update to a new image
3. Do NOT report workload status (simulating a stuck worker)
4. After 10s, reconciler marks deployment `Failed` with `ProgressDeadlineExceeded`
5. If `autoRollbackOnFailure: true`, a rollback is automatically triggered

### Scenario E: Autoscaler under load

1. Create deployment + autoscaler with `targetCPUUtilization: 0.7`, `minReplicas: 1`, `maxReplicas: 5`
2. Ingest several metric samples above 0.7 via POST `/v1/metrics/deployments`
3. After reconcile, replica count should increase
4. Ingest low metrics below 0.7 × 0.65 = 0.455 threshold
5. After scale-down cooldown, replica count decreases

### Scenario F: Circuit breaker on proxy

1. Set up a service pointing to a non-existent upstream
2. Make 3 proxy requests to `GET /v1/network/services/{name}/proxy`
3. After 3 failures (`cbThreshold=3`), subsequent requests return 503 with `circuit_open`
4. After `cbOpenSec` seconds, circuit resets and proxy attempts resume

---

## 10. Environment variables reference

### API Server

| Variable | Default | Description |
|---|---|---|
| `ORCH_HTTP_ADDR` | `:8080` | HTTP listen address |
| `ORCH_HTTPS_ENABLED` | `false` | Enable HTTPS |
| `ORCH_HTTPS_ADDR` | `:8443` | HTTPS listen address |
| `ORCH_TLS_CERT_FILE` | — | Path to TLS certificate |
| `ORCH_TLS_KEY_FILE` | — | Path to TLS private key |
| `ORCH_TLS_CLIENT_CA_FILE` | — | Path to CA bundle for mTLS client verification |
| `ORCH_GRPC_ADDR` | `:9090` | gRPC listen address |
| `ORCH_DNS_ADDR` | `:1053` | Primary DNS UDP listen address |
| `ORCH_DNS_EXTRA_ADDRS` | — | Comma-separated extra DNS addresses |
| `ORCH_ENABLE_DNS_SERVER` | `true` | Start embedded DNS server |
| `ORCH_DB_PATH` | `./badger` | BadgerDB data directory |
| `ORCH_API_TOKEN` | `dev-token` | Static bearer token |
| `ORCH_AUTH_MODE` | `static` | `static` / `jwt` / `oidc` / `hybrid` |
| `ORCH_JWT_SECRET` | — | HMAC-HS256 signing secret |
| `ORCH_JWT_ISSUER` | — | Expected `iss` claim |
| `ORCH_JWT_AUDIENCE` | — | Expected `aud` claim |
| `ORCH_JWKS_URL` | — | JWKS endpoint for OIDC RS256 |
| `ORCH_SCHEDULER_POLICY` | `least-loaded` | `least-loaded` / `best-fit` / `external:<cmd>` |
| `ORCH_CONTROLLER_ID` | `controller-1` | Unique ID for this controller instance |
| `ORCH_CONTROLLER_SHARD_INDEX` | `0` | Shard index (0-based) |
| `ORCH_CONTROLLER_SHARD_TOTAL` | `1` | Total shard count |
| `ORCH_CONTROLLER_LEASE_SECONDS` | `10` | Leadership lease duration |
| `ORCH_KMS_KEY` | — | Base64 AES-256 key for secret encryption |
| `ORCH_KMS_KEY_FILE` | — | File path containing KMS key |
| `ORCH_KMS_KEY_ENDPOINT` | — | HTTP endpoint returning KMS key |
| `ORCH_ADMISSION_PLUGIN_CMD` | — | External admission check command |

### Worker

| Variable | Default | Description |
|---|---|---|
| `ORCH_WORKER_ID` | `worker-local` | Unique worker node ID |
| `ORCH_SERVER_URL` | `http://localhost:8080` | Control plane URL |
| `ORCH_AGENT_TOKEN` | `dev-token` | Bearer token for API authentication |
| `ORCH_RUNTIME_DRIVER` | `docker` | `docker` / `inmemory` / `remote-http` |
| `ORCH_HEARTBEAT_SECONDS` | `5` | Heartbeat interval |
| `ORCH_RESTART_MAX_RETRIES` | `3` | Max container start retries |
| `ORCH_RESTART_BACKOFF_MS` | `250` | Initial backoff between retries (ms) |
| `ORCH_WORKER_METRICS_ADDR` | `:8081` | Prometheus metrics listen address |
| `ORCH_NODE_LABELS` | — | Comma-separated `key=value` node labels |
| `ORCH_NODE_TAINTS` | — | Comma-separated `key=value:effect` taints |
| `ORCH_TLS_SERVER_NAME` | — | Expected TLS server name override |
| `ORCH_TLS_CA_CERT_FILE` | — | CA bundle for verifying control plane cert |
| `ORCH_TLS_CLIENT_CERT_FILE` | — | Client certificate for mTLS |
| `ORCH_TLS_CLIENT_KEY_FILE` | — | Client private key for mTLS |
| `ORCH_REMOTE_RUNTIME_URL` | `http://localhost:18080` | Remote HTTP runtime base URL |
| `ORCH_REMOTE_RUNTIME_TOKEN` | — | Bearer token for remote runtime |
