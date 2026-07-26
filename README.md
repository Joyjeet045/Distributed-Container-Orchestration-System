# Distributed Container Orchestration System

A distributed container orchestrator built from scratch in Go, inspired by Kubernetes and Nomad. Implements all core orchestration concepts including scheduling, rolling updates, autoscaling, service discovery, persistent volumes, network policies, jobs/cron, RBAC, HA leadership, and full observability — without external dependencies beyond BadgerDB and Docker.

---

## Table of Contents

- [Architecture](#architecture)
- [Features](#features)
- [Quick Start](#quick-start)
- [REST API reference](#rest-api-reference)
- [Testing](#testing)
- [Configuration](#configuration)
- [Observability](#observability)
- [Docs](#docs)

---

## Architecture

```
┌────────────────────────────────────────────────────────────────┐
│                        Control Plane                           │
│  API Server (HTTP+gRPC) · Controller Manager · Scheduler       │
│  DNS Server · Load Balancer · Auth · State Store (BadgerDB)    │
└─────────────────────────────┬──────────────────────────────────┘
                              │ HTTP REST / gRPC
              ┌───────────────┼───────────────┐
              │               │               │
       ┌──────▼──────┐ ┌──────▼──────┐ ┌──────▼──────┐
       │  Worker-1   │ │  Worker-2   │ │  Worker-N   │
       │  (Agent)    │ │  (Agent)    │ │  (Agent)    │
       └──────┬──────┘ └──────┬──────┘ └──────┬──────┘
              │               │               │
       ┌──────▼──────┐ ┌──────▼──────┐ ┌──────▼──────┐
       │   Docker /  │ │   Docker /  │ │  Remote HTTP│
       │  in-memory  │ │  in-memory  │ │   Runtime   │
       └─────────────┘ └─────────────┘ └─────────────┘
```

See [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) for the detailed component breakdown, data-flow diagrams, and design decisions.

---

## Features

| Category | Capabilities |
|---|---|
| **Scheduling** | Least-loaded, best-fit, affinity/anti-affinity labels, taints/tolerations, priority preemption, external plugin scoring |
| **Deployments** | Rolling update (`maxSurge`/`maxUnavailable`), canary strategy, revision history, progress deadline, rollback |
| **Autoscaling** | CPU/memory/custom-metric reactive scaling, stabilization windows, cooldowns, predictive linear-trend overlay |
| **Health** | Node TTL heartbeat, liveness/readiness probes, isolation/drain, anomaly spike detection, auto-rollback |
| **Services** | Cluster IP, NodePort, label-selector endpoint reconciliation, round-robin/least-connections LB |
| **DNS** | Embedded UDP DNS server, `<name>.default.svc.cluster.local` A-records, multi-listener HA |
| **Storage** | PersistentVolume/Claim models, storage-class aware binding, stateful workload path |
| **Network** | NetworkPolicy ingress rules, peer/namespace/group filtering, enforcement in proxy path |
| **Secrets** | AES-GCM at-rest encryption (env/file/HTTP KMS), secret file mount lifecycle on workers |
| **Jobs/CronJobs** | One-time jobs with retry/backoff, schedule-based cron spawning, TTL cleanup |
| **Multi-tenancy** | Namespaces, per-namespace resource quotas, RBAC role bindings with inheritance |
| **HA** | Lease-based leader election, shard-aware controller reconciliation, term-tracked transitions |
| **Admission** | External command admission plugin for deployment/service creates |
| **Auth** | Static token, HMAC JWT (HS256), OIDC/JWKS (RS256), hybrid |
| **Security** | Role-based authorization, HTTPS with cert hot-reload, mTLS, audit logging |
| **Observability** | Prometheus metrics (control plane + workers), Grafana dashboards, alert rules, trace ID propagation |
| **gRPC** | Full protobuf-defined API surface alongside REST |

---

## Quick Start

### Prerequisites

- Go 1.21+
- Docker with Compose plugin

### 1. Build

```bash
go build -mod=mod ./...
```

### 2. Run tests

```bash
go test -mod=mod ./...
```

### 3. Start the full stack

```bash
docker compose up -d --build
docker compose ps
```

This starts: `api-server`, `worker-1`, `worker-2`, `prometheus`, `grafana`.

### 4. Verify health

```bash
curl http://localhost:8080/healthz
# → ok

curl -H "Authorization: Bearer dev-token" http://localhost:8080/v1/cluster
# → {"nodes":[{"id":"worker-1",...},{"id":"worker-2",...}],...}
```

### 5. Create your first deployment

```bash
curl -X POST http://localhost:8080/v1/deployments \
  -H "Authorization: Bearer dev-token" \
  -H "Content-Type: application/json" \
  -d '{
    "spec": {
      "name": "nginx-demo",
      "image": "nginx:latest",
      "replicas": 2,
      "resources": { "milliCPU": 250, "memoryMB": 256 },
      "labels": { "app": "nginx" }
    }
  }'
```

### 6. Open dashboards

| Service | URL | Credentials |
|---|---|---|
| Grafana | http://localhost:3000 | admin / admin |
| Prometheus | http://localhost:9091 | — |
| Metrics | http://localhost:8080/metrics | — |

### 7. Tear down

```bash
docker compose down
```

---

## REST API reference

### Core resources

| Method | Path | Role | Description |
|---|---|---|---|
| POST | `/v1/nodes/register` | operator | Register a worker node |
| POST | `/v1/nodes/{id}/heartbeat` | operator | Node heartbeat with resource usage |
| POST | `/v1/nodes/{id}/drain` | operator | Gracefully drain a node |
| POST | `/v1/nodes/{id}/remove` | admin | Remove node from cluster |
| POST | `/v1/deployments` | operator | Create deployment |
| GET | `/v1/deployments` | viewer | List deployments |
| PUT | `/v1/deployments/{id}` | operator | Update deployment spec |
| DELETE | `/v1/deployments/{id}` | admin | Delete deployment |
| POST | `/v1/deployments/{id}/rollback` | operator | Rollback to revision |
| GET | `/v1/cluster` | viewer | Cluster state snapshot |
| GET | `/v1/workers/{id}/assignments` | operator | Poll work assignments |
| POST | `/v1/workloads/{id}/status` | operator | Report workload status |

### Services and discovery

| Method | Path | Description |
|---|---|---|
| POST | `/v1/services` | Create service |
| GET | `/v1/services` | List services |
| GET | `/v1/services/{id}/endpoints` | List endpoints |
| GET | `/v1/services/{id}/proxy-target` | Select LB endpoint |
| GET | `/v1/dns/resolve?name=` | Service DNS lookup |
| GET | `/v1/dns/sidecar/resolve?name=` | Sidecar-optimized resolve |
| GET | `/v1/network/services/{name}/connect` | Route traffic (policy check) |
| GET | `/v1/network/services/{name}/proxy` | Proxy with retry + circuit breaker |

### Autoscaling and metrics

| Method | Path | Description |
|---|---|---|
| POST | `/v1/autoscalers` | Create/update autoscaler policy |
| GET | `/v1/autoscalers` | List policies |
| DELETE | `/v1/autoscalers/{id}` | Delete policy |
| POST | `/v1/metrics/deployments` | Ingest metric sample |
| GET | `/v1/metrics/prometheus` | Prometheus text output (authed) |
| GET | `/metrics` | Prometheus text output (public) |
| GET | `/v1/health/nodes/trends` | Node health trend analytics |

### Multi-tenancy

| Method | Path | Description |
|---|---|---|
| POST/GET | `/v1/namespaces` | Namespaces |
| POST/GET | `/v1/quotas` | Namespace resource quotas |
| POST/GET | `/v1/secrets` | Secrets |
| POST/GET | `/v1/rbac/rolebindings` | RBAC role bindings |

### Storage

| Method | Path | Description |
|---|---|---|
| POST/GET | `/v1/persistent-volumes` | Persistent volumes |
| POST/GET | `/v1/persistent-volume-claims` | PV claims with auto-binding |

### Policy and workloads

| Method | Path | Description |
|---|---|---|
| POST/GET | `/v1/network-policies` | Network ingress policies |
| POST/GET | `/v1/jobs` | One-time jobs |
| POST/GET | `/v1/cronjobs` | Scheduled recurring jobs |

### Operations

| Method | Path | Description |
|---|---|---|
| GET | `/v1/events` | Event history |
| GET | `/v1/events/stream` | SSE live event stream |
| POST | `/v1/recovery/run` | Full cluster reconcile + recovery |
| GET | `/healthz` | Liveness probe (no auth) |

---

## Testing

See [docs/TESTING.md](docs/TESTING.md) for:
- Complete test suite commands
- Per-package unit test descriptions (46 test functions)
- Integration test scenarios with expected outcomes
- Manual curl-based scenario walkthroughs
- Advanced feature scenarios (quotas, secrets, PV, network policies, jobs, RBAC)
- Chaos and failure scenarios
- Observability and security testing

```bash
# All tests
go test -mod=mod ./...

# With race detector
go test -mod=mod -race ./...

# Specific scenario
go test -mod=mod -v ./internal/service -run TestRollingUpdateHistoryAndProgress
go test -mod=mod -v ./internal/e2e -run TestChaosNodeLossTriggersRemediation
```

---

## Configuration

All configuration is environment-driven. Key variables:

### API Server

| Variable | Default | Purpose |
|---|---|---|
| `ORCH_HTTP_ADDR` | `:8080` | HTTP listen address |
| `ORCH_AUTH_MODE` | `static` | `static` / `jwt` / `oidc` / `hybrid` |
| `ORCH_API_TOKEN` | `dev-token` | Static bearer token |
| `ORCH_SCHEDULER_POLICY` | `least-loaded` | `least-loaded` / `best-fit` / `external:<cmd>` |
| `ORCH_DB_PATH` | `./badger` | Persistence directory |
| `ORCH_HTTPS_ENABLED` | `false` | Enable TLS |
| `ORCH_CONTROLLER_ID` | `controller-1` | Instance ID for HA leadership |
| `ORCH_CONTROLLER_SHARD_TOTAL` | `1` | Controller shard count |
| `ORCH_KMS_KEY` | — | AES-256 secret encryption key (base64) |

### Worker

| Variable | Default | Purpose |
|---|---|---|
| `ORCH_WORKER_ID` | `worker-local` | Worker node identity |
| `ORCH_SERVER_URL` | `http://localhost:8080` | Control plane address |
| `ORCH_RUNTIME_DRIVER` | `docker` | `docker` / `inmemory` / `remote-http` |
| `ORCH_NODE_LABELS` | — | `key=value` comma-separated labels |

Full reference in [docs/TESTING.md#10-environment-variables-reference](docs/TESTING.md#10-environment-variables-reference).

---

## Observability

The stack provisions Prometheus and Grafana out of the box.

- **Prometheus** scrapes control plane at `http://api-server:8080/metrics` and workers at `:8081/metrics`
- **Grafana** auto-provisions a cluster overview dashboard with topology, resource, failure, and scaling panels
- **Alert rules** for node down, high failure rate, and autoscaler health are in `deploy/observability/prometheus/alerts.yml`

---

## Docs

| Document | Contents |
|---|---|
| [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) | System design, component breakdown, data flow diagrams |
| [docs/TESTING.md](docs/TESTING.md) | All test cases, manual scenarios, chaos testing, env reference |
| [docs/remaining.md](docs/remaining.md) | Phase completion status |

---

## Project Layout

```
.
├── api/proto/           Protobuf definitions
├── cmd/apiserver/       Control plane binary
├── cmd/worker/          Worker agent binary
├── deploy/docker/       Dockerfiles
├── deploy/observability/ Prometheus + Grafana configs
├── docs/                Architecture and testing docs
├── internal/
│   ├── agent/           Worker HTTP client
│   ├── api/             HTTP + gRPC servers
│   ├── auth/            Token verification
│   ├── config/          Env-driven config
│   ├── controller/      Reconcile loop + HA leadership
│   ├── dnsserver/       Embedded DNS server
│   ├── e2e/             End-to-end tests
│   ├── loadbalancer/    Endpoint selection
│   ├── model/           Domain types
│   ├── plugin/          Plugin SDK + manager
│   ├── runtime/         Container runtime drivers
│   ├── scheduler/       Node placement engine
│   ├── service/         Orchestrator core + features
│   ├── store/           BadgerDB persistence
│   └── worker/          Assignment execution
├── docker-compose.yml
└── go.mod
```
