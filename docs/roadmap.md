# Roadmap Mapping To Requested Phases

## Phase 1: Cluster Foundation

- Worker registration and heartbeats implemented
- Cluster membership and resource reporting implemented

## Phase 2: API Server

- REST API implemented
- gRPC server implemented (JSON transport)
- Deploy/Delete/List/Cluster state endpoints implemented
- Token-based authentication implemented

## Phase 3: Scheduler

- Resource-aware scheduler implemented
- Least-loaded best-fit policy implemented
- Affinity/anti-affinity via labels implemented

## Phase 4: Worker Agent

- Docker image pull implemented
- Container create/start implemented
- Status reporting implemented

## Phase 5: Controller Manager

- Reconciliation loop implemented
- Replica drift reconciliation implemented
- Node health TTL checks implemented

## Remaining Phases (Planned)

- Service discovery and DNS
- Load balancing and service exposure
- Probe-based health checks
- Rolling updates and rollback
- Autoscaling controller
- HA with Raft leader election
- Dashboard + observability
- Volumes, policy, secrets, quotas, plugin framework, multi-tenancy
