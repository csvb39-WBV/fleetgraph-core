# Purpose

This document defines the deterministic runtime image assembly contract for FleetGraph Core. The container boundary remains pass-through only: configuration is provided via environment variables, and runtime behavior is not transformed by container logic.

# Build Command

docker build -t fleetgraph-core:base .

# Run Command

docker run --rm -p 8000:8000 --env-file .env fleetgraph-core:base

# Startup Behavior

Container startup executes the fixed Uvicorn command:

python -m uvicorn fleetgraph_core.runtime.runtime_http_api:app --host 0.0.0.0 --port 8000

Behavior at startup is deterministic:

- The Python process starts Uvicorn.
- Uvicorn imports and serves fleetgraph_core.runtime.runtime_http_api:app.
- No container-side logic transformation occurs.

# Port Exposure

The container exposes port 8000.

At runtime, map host port to container port as needed, for example `-p 8000:8000`.

# Environment Variables

Environment variables are passed through directly to the application process.

- The container sets PYTHONPATH=/app/src for runtime module resolution.
- Additional variables may be injected at runtime using --env or --env-file.
- The container does not reinterpret, transform, or mutate provided configuration values.

# Health Check

Health verification endpoint:

/runtime/health

Example:

curl -f http://localhost:8000/runtime/health

# Image Assembly Notes

Runtime image assembly is deliberate and bounded:

- Base image authority: python:3.11.9-slim is pinned for reproducible build behavior.
- Dependency authority: the image installs an explicit pinned runtime dependency set in Dockerfile: fastapi==0.115.12 and uvicorn==0.30.6.
- Layering discipline: source is copied as src/ to /app/src, and only runtime dependencies needed to serve the API are installed.
- Runtime cleanliness: pip cache is disabled and the container runs as a deterministic non-root user (uid 10001).
- Startup surface: the launch target is fixed to the uvicorn command documented above with no entrypoint transformation.