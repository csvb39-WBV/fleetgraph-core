from __future__ import annotations

from fastapi import FastAPI
from fastapi.responses import JSONResponse

from fleetgraph_core.runtime.runtime_bootstrap import (
    RuntimeBootstrap,
    build_runtime_bootstrap_from_environment,
    build_runtime_bootstrap_summary,
)
from fleetgraph_core.runtime.runtime_external_api import (
    build_runtime_external_api_response,
)
from fleetgraph_core.runtime.runtime_health_api import (
    build_runtime_health_response,
)
from fleetgraph_core.runtime.runtime_metrics_layer import (
    build_runtime_metrics_response,
)
from fleetgraph_core.runtime.runtime_readiness_layer import (
    build_runtime_readiness_response,
)


app = FastAPI()


def _build_runtime_metrics_http_response(bootstrap: RuntimeBootstrap) -> dict:
    response = build_runtime_metrics_response(bootstrap)
    request_metrics = dict(response["request_metrics"])
    request_metrics.pop("execution_time_ms", None)
    return {
        **response,
        "request_metrics": request_metrics,
    }


def _runtime_http_error_response(error: Exception) -> JSONResponse:
    return JSONResponse(
        status_code=500,
        content={
            "error": "runtime_http_api_error",
            "message": str(error),
        },
    )


@app.get("/runtime/summary")
def get_runtime_summary() -> dict:
    try:
        bootstrap = build_runtime_bootstrap_from_environment()
        return build_runtime_bootstrap_summary(bootstrap)
    except Exception as error:  # pragma: no cover
        return _runtime_http_error_response(error)


@app.get("/runtime/external")
def get_runtime_external() -> dict:
    try:
        bootstrap = build_runtime_bootstrap_from_environment()
        return build_runtime_external_api_response(bootstrap)
    except Exception as error:  # pragma: no cover
        return _runtime_http_error_response(error)


@app.get("/runtime/health")
def get_runtime_health() -> dict:
    try:
        bootstrap = build_runtime_bootstrap_from_environment()
        return build_runtime_health_response(bootstrap)
    except Exception as error:  # pragma: no cover
        return _runtime_http_error_response(error)


@app.get("/runtime/metrics")
def get_runtime_metrics() -> dict:
    try:
        bootstrap = build_runtime_bootstrap_from_environment()
        return _build_runtime_metrics_http_response(bootstrap)
    except Exception as error:  # pragma: no cover
        return _runtime_http_error_response(error)


@app.get("/runtime/readiness")
def get_runtime_readiness() -> dict:
    try:
        bootstrap = build_runtime_bootstrap_from_environment()
        readiness_state = {
            "config_loaded": bootstrap.config is not None,
            "bootstrap_complete": isinstance(bootstrap, RuntimeBootstrap),
        }
        return build_runtime_readiness_response(readiness_state)
    except Exception as error:  # pragma: no cover
        return _runtime_http_error_response(error)
