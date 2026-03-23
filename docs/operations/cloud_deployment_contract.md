# Purpose

This document defines the canonical first-cloud deployment contract for FleetGraph Core.

It standardizes:

- the deployable runtime artifact
- the canonical startup surface
- the canonical health surface
- the pass-through environment boundary

It does not implement infrastructure provisioning, deployment automation, or platform-specific orchestration.

# Deployment Baseline

FleetGraph uses a managed container service baseline.

The deployment target class is a single container-hosted HTTP service where the platform:

- runs one container image
- injects environment variables
- routes HTTP traffic to the container port
- performs health checks against the runtime health endpoint

This baseline is intentionally general to avoid infrastructure sprawl while remaining concrete for implementation planning.

# Runtime Artifact

The canonical deployable artifact is the FleetGraph runtime Docker image.

Canonical startup surface inside that image:

python -m fleetgraph_core.runtime.runtime_server_entrypoint

This startup surface is shared across Docker, local execution, and future cloud/process-manager wiring.

# Startup Contract

Service startup contract:

- The deployment platform starts the container process using the image command.
- The runtime process starts through the canonical entrypoint module.
- The entrypoint launches the existing runtime HTTP app with deterministic defaults host=0.0.0.0 and port=8000.
- The container exposes port 8000 for inbound HTTP routing.

Deployment configuration must host this startup contract and must not replace it with alternate runtime logic.

# Health Contract

Canonical health surface:

/runtime/health

Cloud platform health checks should target this endpoint.

Contract distinction:

- Startup success means the process launched.
- Service health means /runtime/health returns HTTP 200.

# Environment Contract

Environment variables are deployment inputs and are passed through to the runtime process.

Rules:

- deployment layer injects values
- runtime reads values
- deployment and hosting layers do not reinterpret, transform, or mutate runtime payload/config semantics

# Deployment Boundary Rules

The deployment layer is hosting-only.

It must not:

- transform requests or responses
- modify runtime API contracts
- introduce business logic in deployment configuration
- add hidden orchestration behavior that changes runtime semantics

# Non-Goals / Deferred Items

This contract block does not implement:

- autoscaling
- secrets integration
- metrics/logging rollout
- auth/security rollout
- multi-region or high availability
- release automation
- package publishing
