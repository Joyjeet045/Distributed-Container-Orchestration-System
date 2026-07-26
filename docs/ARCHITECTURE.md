# Architecture — Distributed Container Orchestration System

## Overview

This system is a ground-up implementation of a distributed container orchestrator written entirely in Go. It follows the same conceptual layering as Kubernetes but is designed to be understandable, self-contained, and extensible without the operational overhead of a production cluster.

```
┌─────────────────────────────────────────────────────────────────────┐
│                          Control Plane                              │
│                                                                     │
│  ┌──────────────┐  ┌─────────────────┐  ┌──────────────────────┐  │
│  │  API Server  │  │   Controller    │  │     Scheduler        │  │
│  │  (HTTP+gRPC) │  │    Manager      │  │  (Placement Engine)  │  │
│  └──────┬───────┘  └────────┬────────┘  └──────────┬───────────┘  │
│         │                  │                       │               │
│         └──────────────────┼───────────────────────┘               │
│                            │                                        │
│  ┌─────────────────────────▼──────────────────────────────────┐   │
│  │            Orchestrator Service (Core Logic)               │   │
│  └─────────────────────────┬──────────────────────────────────┘   │
│                            │                                        │
│  ┌─────────────────────────▼──────────────────────────────────┐   │
│  │                   State Store (BadgerDB)                    │   │
│  └────────────────────────────────────────────────────────────┘   │
│                                                                     │
│  Supporting: DNS Server · Load Balancer · Auth Verifier            │
└──────────────────────────────────┬──────────────────────────────────┘
                                   │ HTTP REST
          ┌───────────────────────┬┴──────────────────────┐
          │                       │                       │
   ┌──────▼──────┐        ┌──────▼──────┐        ┌──────▼──────┐
   │  Worker-1   │        │  Worker-2   │        │  Worker-N   │
   │  (Agent)    │        │  (Agent)    │        │  (Agent)    │
   └──────┬──────┘        └──────┬──────┘        └──────┬──────┘
          │                      │                       │
   ┌──────▼──────┐        ┌──────▼──────┐        ┌──────▼──────┐
   │  Container  │        │  Container  │        │  Container  │
   │   Runtime   │        │   Runtime   │        │   Runtime   │
   │  (Docker/   │        │  (Docker/   │        │  (Docker/   │
   │  in-memory) │        │  in-memory) │        │  remote-http│
   └─────────────┘        └─────────────┘        └─────────────┘
```

---

## Component Details

### API Server (`cmd/apiserver`)

The central entry point for all cluster operations. Handles:

- **HTTP REST** — full CRUD surface for all resource types
- **gRPC** — protobuf-defined service for programmatic access
- **Auth middleware** — static token, HMAC JWT, OIDC/JWKS, hybrid
- **Authorization middleware** — role-based per-endpoint access control with RBAC role-binding inheritance
- **Audit middleware** — structured log lines for all mutating operations and secret reads
- **Trace middleware** — `X-Trace-ID` propagation and per-request latency logging
- **Version middleware** — `X-API-Version` enforcement and `/api/v1` alias routing
- **Circuit breaker** — per-service open/close state tracked in proxy path

### Controller Manager (`internal/controller`)

Runs the reconciliation loop inside the API server process. Features:

- **Shard-aware reconciliation** — splits the deployment set by consistent hash across multiple instances
- **Lease-based leader election** — one controller at a time holds a time-limited lease stored in BadgerDB; standby instances log and yield
- **Term-tracked transitions** — every leadership change is appended to the control-plane transition log

### Orchestrator Service (`internal/service`)

The heart of the system. Responsible for:

| Concern | What it does |
|---|---|
| Deployment lifecycle | Create → reconcile → scale → rollout → rollback → delete |
| Workload placement | Delegates to Scheduler; handles preemption for priority deployments |
| Rolling updates | `maxSurge`/`maxUnavailable` with canary and deadline tracking |
| Auto-rollback | Triggers rollback on failure threshold or progress deadline breach |
| Autoscaling | CPU/memory/custom-metric-driven replica adjustment with cooldowns and predictive linear-trend overlay |
| Health monitoring | Node TTL heartbeat tracking, isolation, draining, workload liveness/readiness probe evaluation |
| Service discovery | Endpoint reconciliation against running workloads with namespace-scoped filtering |
| PV/PVC binding | Sorted best-fit binding of persistent volumes to claims |
| Network policies | Ingress peer/group/port rule evaluation for service routing |
| Namespace quota | Deployment count, CPU, and memory budget enforcement per namespace |
| Secret encryption | AES-GCM at-rest encryption using env/file/HTTP KMS key source |
| Job/CronJob loop | Schedule-based job spawning, completion tracking, TTL cleanup, retry backoff |
| RBAC inheritance | Effective role resolution that walks binding inheritance chains |
| Recovery | Full reconcile + PVC rebind + job loop triggered by POST `/v1/recovery/run` |

### Scheduler (`internal/scheduler`)

Stateless scoring engine. Per `SelectNode` call:

1. Filter by node status (`Ready`), resource fit, taint tolerations
2. Score candidates using active policy:
   - `least-loaded` — minimize post-placement CPU+memory utilization with spread balance penalty
   - `best-fit` — minimize leftover capacity after placement
   - `external:<cmd>` — spawn external binary via stdin/stdout JSON contract; falls back to built-in on error or timeout
3. Select lowest-score candidate

Supports affinity (`affinity.<key>=<value>` labels) and anti-affinity (`anti-affinity.<key>=<value>` labels).

### State Store (`internal/store`)

BadgerDB-backed persistent store with a typed interface. All resources have:
- Prefixed key layout (`deployment:`, `workload:`, `node:`, `pv:`, `pvc:`, `job:`, `transition:`, etc.)
- Mutex-protected writes and reads
- Snapshot API for atomic cluster state export

### Load Balancer (`internal/loadbalancer`)

In-process load balancer managing endpoint selection per service:
- **Round-robin** — cyclic index advancement
- **Least-connections** — tracks in-flight count per endpoint; atomically increments/decrements
- Health-aware filtering (only `Ready` endpoints considered)

### DNS Server (`internal/dnsserver`)

Standalone UDP DNS server resolving service A-records in the format `<name>.default.svc.cluster.local`. Supports multiple bind addresses for HA deployment.

### Auth (`internal/auth`)

Four verification modes, each returning a `Principal{Subject, Roles, Source}`:

| Mode | Token format | Key source |
|---|---|---|
| `static` | Exact match | `ORCH_API_TOKEN` env |
| `jwt` | HMAC-HS256 | `ORCH_JWT_SECRET` env |
| `oidc` | RSA-RS256 | JWKS URL with kid resolution + 5-min cache |
| `hybrid` | Any of the above | Tries static → jwt → oidc in order |

### Worker Agent (`cmd/worker`)

Polls assignments, executes workloads, and reports status. Features:
- Retry with exponential backoff per workload
- Runtime drift reconciliation (detect actual vs desired workloads)
- Secret file materialization/cleanup on assignment lifecycle
- Optional TLS/mTLS transport for HTTPS control-plane communication
- Prometheus metrics endpoint for used CPU/memory/identity

### Runtime Drivers (`internal/runtime`)

Pluggable `ContainerRuntime` interface with three drivers:
- `docker` — Docker Engine API integration
- `inmemory` — in-process map for testing
- `remote-http` — HTTP client delegating to external execution backends

### Plugin Framework (`internal/plugin`)

Defines `SchedulerPlugin` and `ControllerPlugin` interfaces plus `Manager` for plugin registration/enable/disable lifecycle.

---

## Data Flow: Deployment Lifecycle

```
User: POST /v1/deployments
         │
         ▼
API Server → admission check (external plugin, if configured)
         │   → namespace existence + quota enforcement
         │   → volume claim bound check
         ▼
Orchestrator.CreateDeployment()
         │
         ▼
BadgerDB: store deployment record
         │
         ▼
Orchestrator.reconcileDeployment()
         │
         ▼
Scheduler.SelectNode()  ──(preempt if priority needed)──►  Node selected
         │
         ▼
BadgerDB: store workload record, update node Used resources
         │
         ▼
BadgerDB: enqueue Assignment for target worker
         │
         ▼
Worker polls GET /v1/workers/{id}/assignments
         │
         ▼
Worker.ProcessAssignments() → materialize secret files
         │                  → Runtime.RunWorkload()
         │                  → POST /v1/workloads/{id}/status  (Running)
         ▼
Orchestrator.ReportWorkloadStatus()
         │
         ▼
Controller reconcile loop → update rollout status → emit events
```

---

## Data Flow: Service Discovery

```
POST /v1/services  →  Service stored  →  EndpointController reconciles
                                              │
                         namespace-scoped label-selector match against
                         running workloads  →  ServiceEndpoint records
                                              │
GET /v1/dns/resolve?name=<svc>          DNS UDP query
GET /v1/services/{id}/proxy-target          │
GET /v1/network/services/{name}/connect  ◄──┘
         │
         Network policy evaluation
         │
         Load balancer (round-robin or least-connections)
         │
         Endpoint selected → circuit breaker check → upstream proxy
```

---

## Directory Layout

```
.
├── api/proto/                   # Protobuf definitions
├── cmd/
│   ├── apiserver/main.go        # Control plane entrypoint
│   └── worker/main.go           # Worker agent entrypoint
├── deploy/
│   ├── docker/                  # Dockerfiles
│   └── observability/           # Prometheus + Grafana configs
├── docs/                        # Architecture, testing, roadmap docs
├── internal/
│   ├── agent/                   # Worker HTTP client
│   ├── api/                     # HTTP server, gRPC server, validation
│   ├── auth/                    # Token verification
│   ├── config/                  # Env-driven config
│   ├── controller/              # Reconcile loop + shard/lease management
│   ├── dnsserver/               # UDP DNS server
│   ├── e2e/                     # End-to-end integration tests
│   ├── loadbalancer/            # Endpoint selection strategies
│   ├── model/                   # All domain types
│   ├── plugin/                  # Plugin SDK + manager
│   ├── runtime/                 # Container runtime drivers
│   ├── scheduler/               # Node placement engine
│   ├── service/                 # Orchestrator core + feature extensions
│   ├── store/                   # BadgerDB persistence layer
│   └── worker/                  # Assignment execution + drift reconcile
├── docker-compose.yml           # Local multi-node demo stack
├── go.mod
└── Makefile
```

---

## Key Design Decisions

| Decision | Rationale |
|---|---|
| BadgerDB as state store | Embedded, zero-dependency persistence; easy to swap for etcd via the `StateStore` interface |
| Interface-driven everything | `StateStore`, `ContainerRuntime`, `SchedulerPlugin`, `ControllerPlugin` are all interfaces; all logic is testable with in-memory fakes |
| Lease-based leader election | Eliminates external dependency for HA while providing correct standby semantics |
| Shard-aware reconciliation | Horizontal controller scaling without a distributed lock manager |
| AES-GCM for secrets | Standard authenticated encryption; key source is pluggable (env, file, HTTP KMS endpoint) |
| Admission via external command | Fully decoupled policy engine without embedded policy language |
