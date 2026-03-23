"""
D12-MB2 CI Schema Validator.

Validates runtime endpoint response payloads against canonical schemas.

Pure validation logic — no runtime calls, no network, no filesystem writes,
no external dependencies, no randomness, no side effects.
"""

from __future__ import annotations

from typing import Any

# ---------------------------------------------------------------------------
# Canonical endpoint schemas
# ---------------------------------------------------------------------------
# Each schema entry is a tuple of (key, expected_type).
# Ordering is intentional — it defines the required key order.
# Nested sub-schemas are marked with type=dict and a corresponding entry
# in _NESTED_SCHEMAS keyed as "<endpoint>.<key>".

_ENDPOINT_SCHEMAS: dict[str, tuple[tuple[str, type], ...]] = {
    "runtime_summary": (
        ("environment", str),
        ("api_host", str),
        ("api_port", int),
        ("debug", bool),
        ("log_level", str),
        ("logger_name", str),
        ("logger_level", str),
    ),
    "runtime_external": (
        ("response_type", str),
        ("response_schema_version", str),
        ("runtime", dict),
    ),
    "runtime_health": (
        ("response_type", str),
        ("response_schema_version", str),
        ("status", str),
        ("checks", dict),
        ("runtime", dict),
    ),
    "runtime_metrics": (
        ("response_type", str),
        ("response_schema_version", str),
        ("runtime_metrics", dict),
        ("request_metrics", dict),
        ("error_metrics", dict),
        ("health_alignment", dict),
    ),
    "runtime_readiness": (
        ("status", str),
        ("checks", dict),
    ),
}

# Nested sub-schemas: key = "<endpoint>.<field_name>"
_NESTED_SCHEMAS: dict[str, tuple[tuple[str, type], ...]] = {
    "runtime_external.runtime": (
        ("environment", str),
        ("api_host", str),
        ("api_port", int),
        ("debug", bool),
        ("log_level", str),
        ("logger_name", str),
        ("logger_level", str),
    ),
    "runtime_health.checks": (
        ("config_valid", bool),
        ("logger_ready", bool),
    ),
    "runtime_health.runtime": (
        ("environment", str),
        ("api_host", str),
        ("api_port", int),
        ("debug", bool),
        ("log_level", str),
        ("logger_name", str),
        ("logger_level", str),
    ),
    "runtime_metrics.runtime_metrics": (
        ("startup_success", bool),
        ("runtime_status", str),
    ),
    "runtime_metrics.request_metrics": (
        ("request_count_total", int),
        ("request_success_count", int),
        ("request_failure_count", int),
    ),
    "runtime_metrics.error_metrics": (
        ("exception_count", int),
        ("failure_event_count", int),
    ),
    "runtime_metrics.health_alignment": (
        ("health_endpoint_status", str),
        ("health_is_healthy", bool),
    ),
    "runtime_readiness.checks": (
        ("config_loaded", bool),
        ("bootstrap_complete", bool),
    ),
}

SUPPORTED_ENDPOINTS: frozenset[str] = frozenset(_ENDPOINT_SCHEMAS.keys())


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _validate_against_schema(
    obj: dict[str, Any],
    schema: tuple[tuple[str, type], ...],
    context: str,
    errors: list[str],
) -> None:
    """Validate obj keys (presence, absence, order, types) against schema."""
    expected_keys: tuple[str, ...] = tuple(k for k, _ in schema)
    expected_key_set: frozenset[str] = frozenset(expected_keys)
    present_keys: tuple[str, ...] = tuple(obj.keys())
    present_key_set: frozenset[str] = frozenset(present_keys)

    # Missing keys
    for key in expected_keys:
        if key not in present_key_set:
            errors.append(f"{context}: missing required key '{key}'")

    # Extra keys
    for key in sorted(present_key_set - expected_key_set):
        errors.append(f"{context}: unexpected key '{key}'")

    # Key ordering — only checked when all expected keys are present
    if expected_key_set <= present_key_set:
        # Filter present_keys down to just the expected ones, preserving order
        actual_order = tuple(k for k in present_keys if k in expected_key_set)
        if actual_order != expected_keys:
            errors.append(
                f"{context}: wrong key order — "
                f"expected {list(expected_keys)}, got {list(actual_order)}"
            )

    # Value types — only for keys that are present
    for key, expected_type in schema:
        if key not in present_key_set:
            continue
        value = obj[key]
        # bool is a subclass of int; distinguish them explicitly
        if expected_type is int and isinstance(value, bool):
            errors.append(
                f"{context}: key '{key}' has wrong type — "
                f"expected int, got bool"
            )
        elif not isinstance(value, expected_type):
            errors.append(
                f"{context}: key '{key}' has wrong type — "
                f"expected {expected_type.__name__}, got {type(value).__name__}"
            )


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def validate_endpoint_schema(validation_input: dict[str, Any]) -> dict[str, Any]:
    """Validate a runtime endpoint response payload against its canonical schema.

    Args:
        validation_input: {"endpoint": <str>, "payload": <dict>}

    Returns:
        {"status": "pass" | "fail", "errors": [<message>, ...]}
        Key order is fixed; errors list is deterministic.
    """
    if not isinstance(validation_input, dict):
        raise TypeError("validation_input must be a dict")

    if "endpoint" not in validation_input:
        raise ValueError("validation_input is missing required key 'endpoint'")
    if "payload" not in validation_input:
        raise ValueError("validation_input is missing required key 'payload'")

    endpoint = validation_input["endpoint"]
    payload = validation_input["payload"]

    if not isinstance(endpoint, str):
        raise TypeError("validation_input 'endpoint' must be a str")
    if not isinstance(payload, dict):
        raise TypeError("validation_input 'payload' must be a dict")

    if endpoint not in _ENDPOINT_SCHEMAS:
        raise ValueError(
            f"unknown endpoint '{endpoint}'; supported: {sorted(SUPPORTED_ENDPOINTS)}"
        )

    errors: list[str] = []

    # Validate top-level schema
    top_schema = _ENDPOINT_SCHEMAS[endpoint]
    _validate_against_schema(payload, top_schema, f"{endpoint}", errors)

    # Validate nested sub-schemas where the key is present and of correct type
    for field_key, field_type in top_schema:
        if field_type is not dict:
            continue
        nested_schema_key = f"{endpoint}.{field_key}"
        if nested_schema_key not in _NESTED_SCHEMAS:
            continue
        nested_value = payload.get(field_key)
        if not isinstance(nested_value, dict):
            continue  # type error already recorded above
        _validate_against_schema(
            nested_value,
            _NESTED_SCHEMAS[nested_schema_key],
            f"{endpoint}.{field_key}",
            errors,
        )

    status = "pass" if not errors else "fail"
    result: dict[str, Any] = {
        "status": status,
        "errors": errors,
    }

    assert tuple(result.keys()) == ("status", "errors"), (
        "internal error: validator response schema mismatch"
    )

    return result
