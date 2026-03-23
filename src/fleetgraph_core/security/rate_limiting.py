"""
D15-MB3 Rate Limiting Evaluator.

Deterministically evaluates client request eligibility under a rate-limit
window policy.

Pure in-memory Python with strict closed-schema validation.
No timestamps, no filesystem access, no external dependencies, no mutation.
"""

from __future__ import annotations

from typing import Any

_REQUIRED_FIELDS: frozenset[str] = frozenset({
    "client_id",
    "request_count_in_window",
    "max_requests_per_window",
    "window_active",
})

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


def evaluate_rate_limiting(rate_limit_input: dict[str, Any]) -> dict[str, Any]:
    """Evaluate deterministic rate limiting decision."""
    if not isinstance(rate_limit_input, dict):
        raise TypeError("rate_limit_input must be a dict")

    _require_closed_schema(rate_limit_input, _REQUIRED_FIELDS, "rate_limit_input")

    client_id = rate_limit_input["client_id"]
    if not isinstance(client_id, str):
        raise TypeError("rate_limit_input field 'client_id' must be a str")

    request_count_in_window = rate_limit_input["request_count_in_window"]
    if not isinstance(request_count_in_window, int) or isinstance(request_count_in_window, bool):
        raise TypeError("rate_limit_input field 'request_count_in_window' must be an int")
    if request_count_in_window < 0:
        raise ValueError("rate_limit_input field 'request_count_in_window' must not be negative")

    max_requests_per_window = rate_limit_input["max_requests_per_window"]
    if not isinstance(max_requests_per_window, int) or isinstance(max_requests_per_window, bool):
        raise TypeError("rate_limit_input field 'max_requests_per_window' must be an int")
    if max_requests_per_window < 0:
        raise ValueError("rate_limit_input field 'max_requests_per_window' must not be negative")

    window_active = rate_limit_input["window_active"]
    if not isinstance(window_active, bool):
        raise TypeError("rate_limit_input field 'window_active' must be a bool")

    if client_id == "":
        status = "reject"
        reasons = ["client_id_missing"]
    elif not window_active:
        status = "allow"
        reasons = ["window_not_active"]
    elif request_count_in_window < max_requests_per_window:
        status = "allow"
        reasons = ["within_rate_limit"]
    else:
        status = "reject"
        reasons = ["rate_limit_exceeded"]

    response: dict[str, Any] = {
        "status": status,
        "reasons": reasons,
    }

    if tuple(response.keys()) != _EXPECTED_OUTPUT_KEYS:
        raise RuntimeError("internal error: rate limiting response schema mismatch")

    return response
