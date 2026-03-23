"""
D3-MB3 Runtime Readiness Layer.

Accepts a bounded internal state payload and produces a deterministic
closed-schema readiness response.

Pure in-memory Python, no side effects.
"""

from __future__ import annotations

from typing import Any

_READINESS_INPUT_REQUIRED_FIELDS: frozenset[str] = frozenset({
    "config_loaded",
    "bootstrap_complete",
})

_EXPECTED_RESPONSE_KEYS: tuple[str, ...] = (
    "status",
    "checks",
)

_EXPECTED_CHECKS_KEYS: tuple[str, ...] = (
    "config_loaded",
    "bootstrap_complete",
)


def _require_closed_schema(obj: dict, required_fields: frozenset[str], label: str) -> None:
    present = set(obj.keys())
    missing = required_fields - present
    if missing:
        raise ValueError(f"{label} is missing required fields: {', '.join(sorted(missing))}")
    extra = present - required_fields
    if extra:
        raise ValueError(f"{label} contains unexpected fields: {', '.join(sorted(extra))}")


def build_runtime_readiness_response(
    readiness_state: dict[str, Any],
) -> dict[str, Any]:
    """Build a deterministic runtime readiness response from a bounded state payload."""
    if not isinstance(readiness_state, dict):
        raise TypeError("readiness_state must be a dict")

    _require_closed_schema(readiness_state, _READINESS_INPUT_REQUIRED_FIELDS, "readiness_state")

    config_loaded = readiness_state["config_loaded"]
    if not isinstance(config_loaded, bool):
        raise TypeError("readiness_state field 'config_loaded' must be a bool")

    bootstrap_complete = readiness_state["bootstrap_complete"]
    if not isinstance(bootstrap_complete, bool):
        raise TypeError("readiness_state field 'bootstrap_complete' must be a bool")

    is_ready = config_loaded and bootstrap_complete
    status = "ready" if is_ready else "not_ready"

    response: dict[str, Any] = {
        "status": status,
        "checks": {
            "config_loaded": config_loaded,
            "bootstrap_complete": bootstrap_complete,
        },
    }

    if tuple(response.keys()) != _EXPECTED_RESPONSE_KEYS:
        raise RuntimeError("internal error: readiness response schema mismatch")
    if tuple(response["checks"].keys()) != _EXPECTED_CHECKS_KEYS:
        raise RuntimeError("internal error: readiness checks schema mismatch")

    return response
