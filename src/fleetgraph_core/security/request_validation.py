"""
D15-MB2 Request Validation Evaluator.

Deterministically validates inbound request envelopes against bounded
request constraints.

Pure in-memory Python with strict closed-schema validation.
No timestamps, no filesystem access, no external dependencies, no mutation.
"""

from __future__ import annotations

from typing import Any

_REQUIRED_FIELDS: frozenset[str] = frozenset({
    "request_id",
    "endpoint",
    "content_type",
    "payload_size_bytes",
    "max_payload_size_bytes",
    "has_body",
})

_EXPECTED_OUTPUT_KEYS: tuple[str, ...] = (
    "status",
    "reasons",
)

_REASON_ORDER: tuple[str, ...] = (
    "request_id_missing",
    "endpoint_missing",
    "content_type_missing",
    "payload_size_limit_exceeded",
)


def _require_closed_schema(obj: dict[str, Any], required_fields: frozenset[str], label: str) -> None:
    present = set(obj.keys())

    missing = required_fields - present
    if missing:
        raise ValueError(f"{label} is missing required fields: {', '.join(sorted(missing))}")

    extra = present - required_fields
    if extra:
        raise ValueError(f"{label} contains unexpected fields: {', '.join(sorted(extra))}")


def evaluate_request_validation(validation_input: dict[str, Any]) -> dict[str, Any]:
    """Evaluate deterministic validity of inbound request envelope."""
    if not isinstance(validation_input, dict):
        raise TypeError("validation_input must be a dict")

    _require_closed_schema(validation_input, _REQUIRED_FIELDS, "validation_input")

    request_id = validation_input["request_id"]
    if not isinstance(request_id, str):
        raise TypeError("validation_input field 'request_id' must be a str")

    endpoint = validation_input["endpoint"]
    if not isinstance(endpoint, str):
        raise TypeError("validation_input field 'endpoint' must be a str")

    content_type = validation_input["content_type"]
    if not isinstance(content_type, str):
        raise TypeError("validation_input field 'content_type' must be a str")

    payload_size_bytes = validation_input["payload_size_bytes"]
    if not isinstance(payload_size_bytes, int) or isinstance(payload_size_bytes, bool):
        raise TypeError("validation_input field 'payload_size_bytes' must be an int")
    if payload_size_bytes < 0:
        raise ValueError("validation_input field 'payload_size_bytes' must not be negative")

    max_payload_size_bytes = validation_input["max_payload_size_bytes"]
    if not isinstance(max_payload_size_bytes, int) or isinstance(max_payload_size_bytes, bool):
        raise TypeError("validation_input field 'max_payload_size_bytes' must be an int")
    if max_payload_size_bytes < 0:
        raise ValueError("validation_input field 'max_payload_size_bytes' must not be negative")

    has_body = validation_input["has_body"]
    if not isinstance(has_body, bool):
        raise TypeError("validation_input field 'has_body' must be a bool")

    reasons: list[str] = []

    if request_id == "":
        reasons.append("request_id_missing")

    if endpoint == "":
        reasons.append("endpoint_missing")

    if content_type == "":
        reasons.append("content_type_missing")

    if payload_size_bytes > max_payload_size_bytes:
        reasons.append("payload_size_limit_exceeded")

    if reasons:
        status = "invalid"
        reasons = [reason for reason in _REASON_ORDER if reason in reasons]
    else:
        status = "valid"
        reasons = ["request_valid"]

    response: dict[str, Any] = {
        "status": status,
        "reasons": reasons,
    }

    if tuple(response.keys()) != _EXPECTED_OUTPUT_KEYS:
        raise RuntimeError("internal error: request validation response schema mismatch")

    return response
