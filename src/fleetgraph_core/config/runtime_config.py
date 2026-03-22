from __future__ import annotations

from dataclasses import dataclass
import os
from typing import Any, Mapping


SUPPORTED_ENVIRONMENTS = ("development", "staging", "production")
SUPPORTED_LOG_LEVELS = ("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL")
DEFAULT_LOG_LEVEL = "INFO"
DEFAULT_ENVIRONMENT = "development"
DEFAULT_API_HOST = "127.0.0.1"
DEFAULT_API_PORT = 8000
DEFAULT_DEBUG = False

ENV_RUNTIME_ENVIRONMENT = "FLEETGRAPH_RUNTIME_ENVIRONMENT"
ENV_API_HOST = "FLEETGRAPH_API_HOST"
ENV_API_PORT = "FLEETGRAPH_API_PORT"
ENV_DEBUG = "FLEETGRAPH_DEBUG"
ENV_LOG_LEVEL = "FLEETGRAPH_LOG_LEVEL"


@dataclass(frozen=True)
class RuntimeConfig:
    environment: str
    api_host: str
    api_port: int
    debug: bool
    log_level: str


def _validate_mapping(config_input: Any) -> Mapping[str, Any]:
    if not isinstance(config_input, Mapping):
        raise ValueError("config_input must be a mapping")
    return config_input


def _normalize_non_empty_string(value: Any, field_name: str) -> str:
    if not isinstance(value, str):
        raise ValueError(f"{field_name} must be a string")

    normalized_value = value.strip()
    if not normalized_value:
        raise ValueError(f"{field_name} must be a non-empty string")

    return normalized_value


def _normalize_environment(environment_value: str) -> str:
    normalized_environment = _normalize_non_empty_string(environment_value, "environment").lower()
    if normalized_environment not in SUPPORTED_ENVIRONMENTS:
        raise ValueError(
            "Unsupported environment: "
            f"{environment_value}. Supported environments: "
            + ", ".join(SUPPORTED_ENVIRONMENTS)
        )
    return normalized_environment


def _normalize_host(host_value: Any) -> str:
    return _normalize_non_empty_string(host_value, "api_host")


def _normalize_port(port_value: Any) -> int:
    if isinstance(port_value, bool):
        raise ValueError("api_port must be an integer between 1 and 65535")

    if isinstance(port_value, int):
        normalized_port = port_value
    elif isinstance(port_value, str):
        raw_port = port_value.strip()
        if not raw_port:
            raise ValueError("api_port must be an integer between 1 and 65535")
        try:
            normalized_port = int(raw_port)
        except ValueError as exc:
            raise ValueError("api_port must be an integer between 1 and 65535") from exc
    else:
        raise ValueError("api_port must be an integer between 1 and 65535")

    if normalized_port < 1 or normalized_port > 65535:
        raise ValueError("api_port must be an integer between 1 and 65535")

    return normalized_port


def _normalize_debug(debug_value: Any) -> bool:
    if isinstance(debug_value, bool):
        return debug_value

    if not isinstance(debug_value, str):
        raise ValueError(
            "debug must be one of: 1, true, yes, on, 0, false, no, off"
        )

    normalized_debug = debug_value.strip().lower()
    truthy_values = {"1", "true", "yes", "on"}
    falsey_values = {"0", "false", "no", "off"}

    if normalized_debug in truthy_values:
        return True
    if normalized_debug in falsey_values:
        return False

    raise ValueError("debug must be one of: 1, true, yes, on, 0, false, no, off")


def _normalize_log_level(log_level_value: Any) -> str:
    normalized_log_level = _normalize_non_empty_string(log_level_value, "log_level").upper()

    if normalized_log_level not in SUPPORTED_LOG_LEVELS:
        raise ValueError(
            "Unsupported log_level: "
            f"{log_level_value}. Supported log levels: "
            + ", ".join(SUPPORTED_LOG_LEVELS)
        )

    return normalized_log_level


def _build_runtime_config(
    environment_value: Any,
    host_value: Any,
    port_value: Any,
    debug_value: Any,
    log_level_value: Any,
) -> RuntimeConfig:
    return RuntimeConfig(
        environment=_normalize_environment(environment_value),
        api_host=_normalize_host(host_value),
        api_port=_normalize_port(port_value),
        debug=_normalize_debug(debug_value),
        log_level=_normalize_log_level(log_level_value),
    )


def load_runtime_config(environ: Mapping[str, str] | None = None) -> RuntimeConfig:
    source_environ = environ if environ is not None else os.environ

    return _build_runtime_config(
        environment_value=source_environ.get(ENV_RUNTIME_ENVIRONMENT, DEFAULT_ENVIRONMENT),
        host_value=source_environ.get(ENV_API_HOST, DEFAULT_API_HOST),
        port_value=source_environ.get(ENV_API_PORT, str(DEFAULT_API_PORT)),
        debug_value=source_environ.get(ENV_DEBUG, "false"),
        log_level_value=source_environ.get(ENV_LOG_LEVEL, DEFAULT_LOG_LEVEL),
    )


def build_runtime_config(config_input: Mapping[str, Any]) -> RuntimeConfig:
    validated_mapping = _validate_mapping(config_input)

    return _build_runtime_config(
        environment_value=validated_mapping.get("environment", DEFAULT_ENVIRONMENT),
        host_value=validated_mapping.get("api_host", DEFAULT_API_HOST),
        port_value=validated_mapping.get("api_port", DEFAULT_API_PORT),
        debug_value=validated_mapping.get("debug", DEFAULT_DEBUG),
        log_level_value=validated_mapping.get("log_level", DEFAULT_LOG_LEVEL),
    )
