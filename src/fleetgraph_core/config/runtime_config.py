from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping


SUPPORTED_ENVIRONMENTS = ("dev", "test", "prod")
DEFAULT_LOG_LEVEL = "INFO"

_REQUIRED_KEYS = (
    "environment",
    "aws_region",
    "storage_bucket",
    "signal_topic",
)


@dataclass(frozen=True)
class RuntimeConfig:
    environment: str
    aws_region: str
    storage_bucket: str
    signal_topic: str
    log_level: str


def _validate_mapping(config_input: Any) -> Mapping[str, Any]:
    if not isinstance(config_input, Mapping):
        raise ValueError("config_input must be a mapping")
    return config_input


def _validate_required_string(
    config_input: Mapping[str, Any],
    key_name: str,
) -> str:
    if key_name not in config_input:
        raise ValueError(f"Missing required config key: {key_name}")

    value = config_input[key_name]
    if not isinstance(value, str):
        raise ValueError(f"Config key must be a string: {key_name}")

    normalized_value = value.strip()
    if not normalized_value:
        raise ValueError(f"Config key must be a non-empty string: {key_name}")

    return normalized_value


def _normalize_environment(environment_value: str) -> str:
    normalized_environment = environment_value.strip().lower()
    if normalized_environment not in SUPPORTED_ENVIRONMENTS:
        raise ValueError(
            "Unsupported environment: "
            f"{environment_value}. Supported environments: "
            + ", ".join(SUPPORTED_ENVIRONMENTS)
        )
    return normalized_environment


def _normalize_log_level(log_level_value: str | None) -> str:
    if log_level_value is None:
        return DEFAULT_LOG_LEVEL

    if not isinstance(log_level_value, str):
        raise ValueError("Config key must be a string: log_level")

    normalized_log_level = log_level_value.strip().upper()
    if not normalized_log_level:
        raise ValueError("Config key must be a non-empty string: log_level")

    return normalized_log_level


def build_runtime_config(config_input: Mapping[str, Any]) -> RuntimeConfig:
    validated_mapping = _validate_mapping(config_input)

    required_values = {
        key_name: _validate_required_string(validated_mapping, key_name)
        for key_name in _REQUIRED_KEYS
    }

    return RuntimeConfig(
        environment=_normalize_environment(required_values["environment"]),
        aws_region=required_values["aws_region"],
        storage_bucket=required_values["storage_bucket"],
        signal_topic=required_values["signal_topic"],
        log_level=_normalize_log_level(validated_mapping.get("log_level")),
    )
