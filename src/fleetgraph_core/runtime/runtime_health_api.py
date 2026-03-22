from __future__ import annotations

from typing import Any

from fleetgraph_core.runtime.runtime_bootstrap import RuntimeBootstrap
from fleetgraph_core.runtime.runtime_external_api import build_runtime_external_api_response


_EXPECTED_RUNTIME_KEYS = (
    "environment",
    "api_host",
    "api_port",
    "debug",
    "log_level",
    "logger_name",
    "logger_level",
)
_ALLOWED_ENVIRONMENTS = {"development", "staging", "production"}
_ALLOWED_LOG_LEVELS = {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}


def _is_config_valid(runtime_payload: dict[str, Any]) -> bool:
    environment = runtime_payload["environment"]
    api_port = runtime_payload["api_port"]
    log_level = runtime_payload["log_level"]

    return (
        environment in _ALLOWED_ENVIRONMENTS
        and isinstance(api_port, int)
        and 1 <= api_port <= 65535
        and log_level in _ALLOWED_LOG_LEVELS
    )


def _is_logger_ready(runtime_payload: dict[str, Any]) -> bool:
    logger_name = runtime_payload["logger_name"]
    logger_level = runtime_payload["logger_level"]

    return (
        isinstance(logger_name, str)
        and bool(logger_name.strip())
        and logger_level in _ALLOWED_LOG_LEVELS
    )


def build_runtime_health_response(bootstrap: RuntimeBootstrap) -> dict[str, Any]:
    if not isinstance(bootstrap, RuntimeBootstrap):
        raise ValueError("bootstrap must be a RuntimeBootstrap instance")

    external_response = build_runtime_external_api_response(bootstrap)
    runtime_payload = external_response.get("runtime")
    if not isinstance(runtime_payload, dict):
        raise ValueError("Runtime external API response must contain a runtime dict")
    if tuple(runtime_payload.keys()) != _EXPECTED_RUNTIME_KEYS:
        raise ValueError("Runtime payload does not match health API contract")

    checks = {
        "config_valid": _is_config_valid(runtime_payload),
        "logger_ready": _is_logger_ready(runtime_payload),
    }
    status = "healthy" if all(checks.values()) else "degraded"

    return {
        "response_type": "runtime_health_response",
        "response_schema_version": "1.0",
        "status": status,
        "checks": checks,
        "runtime": {
            "environment": runtime_payload["environment"],
            "api_host": runtime_payload["api_host"],
            "api_port": runtime_payload["api_port"],
            "debug": runtime_payload["debug"],
            "log_level": runtime_payload["log_level"],
            "logger_name": runtime_payload["logger_name"],
            "logger_level": runtime_payload["logger_level"],
        },
    }
