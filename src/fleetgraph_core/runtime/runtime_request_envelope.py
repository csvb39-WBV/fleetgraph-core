from __future__ import annotations

from copy import deepcopy
from typing import Any


REQUIRED_KEYS: tuple[str, ...] = (
    "request_id",
    "client_id",
    "api_key",
    "operation_type",
    "payload",
    "runtime_limits",
    "billing_enabled",
)

ALLOWED_OPERATION_TYPES: tuple[str, ...] = (
    "ingest",
    "retrieve",
    "reprocess",
    "status",
)


def _validate_non_empty_string(value: Any, field_name: str) -> str:
    if not isinstance(value, str):
        raise ValueError(f"{field_name} must be a string")
    if not value:
        raise ValueError(f"{field_name} must be a non-empty string")
    return value


def _validate_string(value: Any, field_name: str) -> str:
    if not isinstance(value, str):
        raise ValueError(f"{field_name} must be a string")
    return value


def _validate_dict(value: Any, field_name: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ValueError(f"{field_name} must be a dict")
    return value


def _validate_keys(envelope: dict[str, Any]) -> None:
    actual_keys = set(envelope.keys())
    required_keys = set(REQUIRED_KEYS)

    missing_keys = required_keys - actual_keys
    if missing_keys:
        raise ValueError(
            "Missing required keys: " + ", ".join(sorted(missing_keys))
        )

    extra_keys = actual_keys - required_keys
    if extra_keys:
        raise ValueError("Unexpected keys: " + ", ".join(sorted(extra_keys)))


def build_runtime_request_envelope(envelope: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(envelope, dict):
        raise ValueError("envelope must be a dict")

    _validate_keys(envelope)

    request_id = _validate_non_empty_string(envelope["request_id"], "request_id")
    client_id = _validate_non_empty_string(envelope["client_id"], "client_id")
    api_key = _validate_string(envelope["api_key"], "api_key")

    operation_type = envelope["operation_type"]
    if operation_type not in ALLOWED_OPERATION_TYPES:
        raise ValueError(
            "operation_type must be one of: "
            + ", ".join(ALLOWED_OPERATION_TYPES)
        )

    payload = _validate_dict(envelope["payload"], "payload")
    runtime_limits = _validate_dict(envelope["runtime_limits"], "runtime_limits")

    billing_enabled = envelope["billing_enabled"]
    if not isinstance(billing_enabled, bool):
        raise ValueError("billing_enabled must be a bool")

    # Preserve nested structure and ordering while returning a detached envelope.
    return {
        "request_id": request_id,
        "client_id": client_id,
        "api_key": api_key,
        "operation_type": operation_type,
        "payload": deepcopy(payload),
        "runtime_limits": deepcopy(runtime_limits),
        "billing_enabled": billing_enabled,
    }