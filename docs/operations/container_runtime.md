# Purpose

This document defines the deterministic container runtime contract for FleetGraph Core. The container boundary is pass-through only: configuration is provided via environment variables, and runtime behavior is not transformed by container logic.

# Build Command

docker build -t fleetgraph-core:base .

# Run Command

docker run --rm -p 8000:8000 --env-file .env fleetgraph-core:base

# Startup Behavior

Container startup executes the fixed Uvicorn command:

python -m uvicorn fleetgraph_core.runtime.runtime_http_api:app --host 0.0.0.0 --port 8000

Behavior at startup is deterministic:

- The Python process starts Uvicorn.
- Uvicorn imports and serves `fleetgraph_core.runtime.runtime_http_api:app`.
- No container-side logic transformation occurs.

# Port Exposure

The container exposes port 8000.

At runtime, map host port to container port as needed, for example `-p 8000:8000`.

# Environment Variables

Environment variables are passed through directly to the application process.

- The container sets `PYTHONPATH=/app/src`.
- Additional variables may be injected at runtime using `--env` or `--env-file`.
- The container does not reinterpret, transform, or mutate provided configuration values.

# Health Check

Health verification endpoint:

/runtime/health

Example:

curl -f http://localhost:8000/runtime/health