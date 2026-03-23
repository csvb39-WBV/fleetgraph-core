"""
D3-MB4 Runtime Failure Policy Layer.

Accepts a bounded policy input payload and returns a deterministic
closed-schema retry/failure decision.

Pure in-memory Python, no side effects.
"""

from __future__ import annotations

from typing import Any

CANONICAL_FAILURE_TYPES: frozenset[str] = frozenset({
    "VALIDATION_ERROR",
    "EXECUTION_ERROR",
    "TIMEOUT_ERROR",
    "DEPENDENCY_ERROR",
})

_POLICY_INPUT_REQUIRED_FIELDS: frozenset[str] = frozenset({
    "failure_type",
    "attempt_count",
    "max_retries",
})

_EXPECTED_RESPONSE_KEYS: tuple[str, ...] = (
    "failure_type",
    "should_retry",
    "retry_decision",
)

_NO_RETRY_TYPES: frozenset[str] = frozenset({
    "VALIDATION_ERROR",
    "EXECUTION_ERROR",
})

_CONDITIONAL_RETRY_TYPES: frozenset[str] = frozenset({
    "TIMEOUT_ERROR",
    "DEPENDENCY_ERROR",
})


def _require_closed_schema(obj: dict, required_fields: frozenset[str], label: str) -> None:
    present = set(obj.keys())
    missing = required_fields - present
    if missing:
        raise ValueError(f"{label} is missing required fields: {', '.join(sorted(missing))}")
    extra = present - required_fields
    if extra:
        raise ValueError(f"{label} contains unexpected fields: {', '.join(sorted(extra))}")


def build_failure_policy_response(
    policy_input: dict[str, Any],
) -> dict[str, Any]:
    """Build a deterministic failure policy decision from a bounded input payload."""
    if not isinstance(policy_input, dict):
        raise TypeError("policy_input must be a dict")

    _require_closed_schema(policy_input, _POLICY_INPUT_REQUIRED_FIELDS, "policy_input")

    failure_type = policy_input["failure_type"]
    if not isinstance(failure_type, str):
        raise TypeError("policy_input field 'failure_type' must be a str")
    if failure_type not in CANONICAL_FAILURE_TYPES:
        raise ValueError(
            f"policy_input field 'failure_type' must be one of "
            f"{sorted(CANONICAL_FAILURE_TYPES)}, got: {failure_type!r}"
        )

    attempt_count = policy_input["attempt_count"]
    if not isinstance(attempt_count, int) or isinstance(attempt_count, bool):
        raise TypeError("policy_input field 'attempt_count' must be an int")
    if attempt_count < 0:
        raise ValueError("policy_input field 'attempt_count' must not be negative")

    max_retries = policy_input["max_retries"]
    if not isinstance(max_retries, int) or isinstance(max_retries, bool):
        raise TypeError("policy_input field 'max_retries' must be an int")
    if max_retries < 0:
        raise ValueError("policy_input field 'max_retries' must not be negative")

    if failure_type in _NO_RETRY_TYPES:
        should_retry = False
    else:
        should_retry = attempt_count < max_retries

    retry_decision = "retry" if should_retry else "do_not_retry"

    response: dict[str, Any] = {
        "failure_type": failure_type,
        "should_retry": should_retry,
        "retry_decision": retry_decision,
    }

    if tuple(response.keys()) != _EXPECTED_RESPONSE_KEYS:
        raise RuntimeError("internal error: policy response schema mismatch")

    return response
