# Purpose

This document defines the deterministic runtime container contract for FleetGraph Core using a single canonical startup surface. The runtime image is the canonical deployable artifact, and the runtime boundary remains pass-through only: configuration is provided to the process, and container/entrypoint wiring does not transform requests, responses, or runtime payloads.

# Build Command

docker build -t fleetgraph-core:base .

# Run Command

docker run --rm -p 8000:8000 --env-file .env fleetgraph-core:base

# Startup Behavior

Container startup executes the canonical runtime entrypoint module:

python -m fleetgraph_core.runtime.runtime_server_entrypoint

Entrypoint behavior is deterministic:

- The entrypoint imports the canonical FastAPI app target from fleetgraph_core.runtime.runtime_http_api.
- The entrypoint launches Uvicorn with centralized defaults host=0.0.0.0 and port=8000.
- Launch wiring is thin and contains no business logic.

# Port Exposure

The container exposes port 8000.

At runtime, map host port to container port as needed, for example -p 8000:8000.

# Environment Variables

Environment variables are passed through directly to the application process.

- The container sets PYTHONPATH=/app/src for runtime module resolution.
- Additional variables may be injected at runtime using --env or --env-file.
- The runtime boundary is pass-through only and does not reinterpret or mutate provided values.

# Health Check

Health verification endpoint:

/runtime/health

Example:

curl -f http://localhost:8000/runtime/health

# Entrypoint Notes

The canonical startup surface for Docker, local execution, and future deployment/process-manager wiring is:

python -m fleetgraph_core.runtime.runtime_server_entrypoint

The entrypoint internally launches the existing runtime app via Uvicorn and preserves current runtime behavior without route or contract changes.

This startup surface aligns with the cloud deployment contract and remains the single authoritative runtime launch path.