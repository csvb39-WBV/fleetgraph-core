from __future__ import annotations

from typing import Any

from fleetgraph_core.runtime.runtime_bootstrap import (
    RuntimeBootstrap,
    build_runtime_bootstrap_summary,
)


_EXPECTED_RUNTIME_SUMMARY_KEYS = (
    "environment",
    "api_host",
    "api_port",
    "debug",
    "log_level",
    "logger_name",
    "logger_level",
)


def build_runtime_external_api_response(bootstrap: RuntimeBootstrap) -> dict[str, Any]:
    if not isinstance(bootstrap, RuntimeBootstrap):
        raise ValueError("bootstrap must be a RuntimeBootstrap instance")

    runtime_summary = build_runtime_bootstrap_summary(bootstrap)
    if tuple(runtime_summary.keys()) != _EXPECTED_RUNTIME_SUMMARY_KEYS:
        raise ValueError("Runtime bootstrap summary does not match external API contract")

    return {
        "response_type": "runtime_external_api_response",
        "response_schema_version": "1.0",
        "runtime": {
            "environment": runtime_summary["environment"],
            "api_host": runtime_summary["api_host"],
            "api_port": runtime_summary["api_port"],
            "debug": runtime_summary["debug"],
            "log_level": runtime_summary["log_level"],
            "logger_name": runtime_summary["logger_name"],
            "logger_level": runtime_summary["logger_level"],
        },
    }
