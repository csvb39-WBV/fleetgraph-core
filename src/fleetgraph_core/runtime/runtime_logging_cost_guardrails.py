"""
D13-MB7 Runtime Logging Cost Guardrails Evaluator.

Deterministically evaluates production logging eligibility for bounded
logging events.

Pure in-memory Python with strict closed-schema validation.
No timestamps, no filesystem access, no external dependencies, no mutation.
"""

from __future__ import annotations

from typing import Any

_REQUIRED_FIELDS: frozenset[str] = frozenset({
    "log_level",
    "event_category",
    "payload_size_bytes",
    "max_payload_size_bytes",
    "production_mode",
})

_ALLOWED_LOG_LEVELS: frozenset[str] = frozenset({
    "DEBUG",
    "INFO",
    "WARNING",
    "ERROR",
})

_ALLOWED_EVENT_CATEGORIES: frozenset[str] = frozenset({
    "startup",
    "request",
    "metrics",
    "health",
    "diagnostic",
})

_INT_FIELDS: tuple[str, ...] = (
    "payload_size_bytes",
    "max_payload_size_bytes",
)

_REASON_ORDER: tuple[str, ...] = (
    "payload_size_limit_exceeded",
    "debug_logging_blocked_in_production",
    "diagnostic_logging_blocked_in_production",
)

_EXPECTED_OUTPUT_KEYS: tuple[str, ...] = (
    "status",
    "reasons",
)


def _require_closed_schema(obj: dict[str, Any], required_fields: frozenset[str], label: str) -> None:
    present = set(obj.keys())

    missing = required_fields - present
    if missing:
        raise ValueError(f"{label} is missing required fields: {', '.join(sorted(missing))}")

    extra = present - required_fields
    if extra:
        raise ValueError(f"{label} contains unexpected fields: {', '.join(sorted(extra))}")


def evaluate_runtime_logging_cost_guardrails(
    guardrail_input: dict[str, Any],
) -> dict[str, Any]:
    """Build deterministic allow/reject logging decision for runtime events."""
    if not isinstance(guardrail_input, dict):
        raise TypeError("guardrail_input must be a dict")

    _require_closed_schema(guardrail_input, _REQUIRED_FIELDS, "guardrail_input")

    log_level = guardrail_input["log_level"]
    if not isinstance(log_level, str):
        raise TypeError("guardrail_input field 'log_level' must be a str")
    if log_level not in _ALLOWED_LOG_LEVELS:
        raise ValueError(
            "guardrail_input field 'log_level' must be one of "
            f"{sorted(_ALLOWED_LOG_LEVELS)}"
        )

    event_category = guardrail_input["event_category"]
    if not isinstance(event_category, str):
        raise TypeError("guardrail_input field 'event_category' must be a str")
    if event_category not in _ALLOWED_EVENT_CATEGORIES:
        raise ValueError(
            "guardrail_input field 'event_category' must be one of "
            f"{sorted(_ALLOWED_EVENT_CATEGORIES)}"
        )

    for field in _INT_FIELDS:
        value = guardrail_input[field]
        if not isinstance(value, int) or isinstance(value, bool):
            raise TypeError(f"guardrail_input field '{field}' must be an int")
        if value < 0:
            raise ValueError(f"guardrail_input field '{field}' must not be negative")

    production_mode = guardrail_input["production_mode"]
    if not isinstance(production_mode, bool):
        raise TypeError("guardrail_input field 'production_mode' must be a bool")

    reasons: list[str] = []

    if guardrail_input["payload_size_bytes"] > guardrail_input["max_payload_size_bytes"]:
        reasons.append("payload_size_limit_exceeded")

    if production_mode and log_level == "DEBUG":
        reasons.append("debug_logging_blocked_in_production")

    if production_mode and event_category == "diagnostic":
        reasons.append("diagnostic_logging_blocked_in_production")

    if reasons:
        status = "reject"
        reasons = [reason for reason in _REASON_ORDER if reason in reasons]
    else:
        status = "allow"
        reasons = ["within_logging_cost_limits"]

    response: dict[str, Any] = {
        "status": status,
        "reasons": reasons,
    }

    if tuple(response.keys()) != _EXPECTED_OUTPUT_KEYS:
        raise RuntimeError("internal error: logging guardrails response schema mismatch")

    return response
