# Purpose

This document defines the canonical runtime logging contract for FleetGraph Core.

This block standardizes deterministic runtime logging policy so logs are structurally consistent, operationally predictable, and aligned to the runtime boundary.

This block does not implement metrics, tracing, external telemetry shipping, dashboards, or vendor-specific observability integrations.

# Logging Scope

Current scope covers runtime operations surfaces that already exist:

- runtime server entrypoint startup path
- runtime startup failure surface
- runtime health/runtime boundary behavior via existing runtime logger context

Events in scope for this baseline are operational lifecycle and runtime failure events, not request/response payload tracing.

Deferred from this block:

- external aggregation pipelines
- distributed trace correlation
- metric-log correlation workflows
- alert routing

# Canonical Log Structure

Canonical logger naming:

- Runtime logger namespace uses fleetgraph.runtime.*
- Entrypoint lifecycle events use fleetgraph.runtime.entrypoint

Severity levels:

- INFO for startup/lifecycle events
- ERROR/EXCEPTION for startup/runtime launch failures

Message expectations:

- Event-style, deterministic messages
- Stable key fields in message context where relevant

Structured context fields (when present):

- event
- host
- port
- entrypoint

Determinism expectations:

- Logging setup path is explicit and repeatable
- Logger naming is stable
- Startup logging path is unambiguous through the canonical entrypoint

# Required Logging Events

Minimum expected runtime logging categories:

- runtime startup initiated
- runtime server launched/ready path initiated
- runtime startup failure with exception context
- runtime logging initialization fallback event when canonical runtime logger cannot be initialized

Runtime shutdown logging is optional in this baseline and may be expanded in a later block.

# Boundary Rules

Logging must not:

- mutate runtime contracts
- expose secrets
- add business logic
- alter request/response semantics
- become a side-channel for hidden state changes

# Deferred Items

This block does not implement:

- external aggregation
- distributed tracing
- metrics correlation
- alerting
- vendor-specific log routing
