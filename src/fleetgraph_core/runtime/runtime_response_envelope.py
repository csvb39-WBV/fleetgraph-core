from __future__ import annotations

from copy import deepcopy
from typing import Any


REQUIRED_KEYS: tuple[str, ...] = (
    "request_id",
    "client_id",
    "operation_type",
    "status",
    "result",
    "errors",
    "billing_enabled",
)

ALLOWED_OPERATION_TYPES: tuple[str, ...] = (
    "ingest",
    "retrieve",
    "reprocess",
    "status",
)

ALLOWED_RESPONSE_STATUSES: tuple[str, ...] = (
    "accepted",
    "completed",
    "failed",
)


def _validate_non_empty_string(value: Any, field_name: str) -> str:
    if not isinstance(value, str):
        raise ValueError(f"{field_name} must be a string")
    if not value:
        raise ValueError(f"{field_name} must be a non-empty string")
    return value


def _validate_dict(value: Any, field_name: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ValueError(f"{field_name} must be a dict")
    return value


def _validate_list(value: Any, field_name: str) -> list[Any]:
    if not isinstance(value, list):
        raise ValueError(f"{field_name} must be a list")
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


def build_runtime_response_envelope(envelope: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(envelope, dict):
        raise ValueError("envelope must be a dict")

    _validate_keys(envelope)

    request_id = _validate_non_empty_string(envelope["request_id"], "request_id")
    client_id = _validate_non_empty_string(envelope["client_id"], "client_id")

    operation_type = envelope["operation_type"]
    if operation_type not in ALLOWED_OPERATION_TYPES:
        raise ValueError(
            "operation_type must be one of: "
            + ", ".join(ALLOWED_OPERATION_TYPES)
        )

    status = envelope["status"]
    if status not in ALLOWED_RESPONSE_STATUSES:
        raise ValueError(
            "status must be one of: " + ", ".join(ALLOWED_RESPONSE_STATUSES)
        )

    result = _validate_dict(envelope["result"], "result")
    errors = _validate_list(envelope["errors"], "errors")

    billing_enabled = envelope["billing_enabled"]
    if not isinstance(billing_enabled, bool):
        raise ValueError("billing_enabled must be a bool")

    # Return a detached deterministic envelope while preserving nested ordering.
    return {
        "request_id": request_id,
        "client_id": client_id,
        "operation_type": operation_type,
        "status": status,
        "result": deepcopy(result),
        "errors": deepcopy(errors),
        "billing_enabled": billing_enabled,
    }