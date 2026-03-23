"""
D15-MB1 API Key Authentication Evaluator.

Deterministically evaluates provided API key authorization against a bounded
authorized key set.

Pure in-memory Python with strict closed-schema validation.
No timestamps, no filesystem access, no external dependencies, no mutation.
"""

from __future__ import annotations

from typing import Any

_REQUIRED_FIELDS: frozenset[str] = frozenset({
    "provided_api_key",
    "authorized_api_keys",
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


def evaluate_api_key_auth(auth_input: dict[str, Any]) -> dict[str, Any]:
    """Evaluate deterministic API key authorization response."""
    if not isinstance(auth_input, dict):
        raise TypeError("auth_input must be a dict")

    _require_closed_schema(auth_input, _REQUIRED_FIELDS, "auth_input")

    provided_api_key = auth_input["provided_api_key"]
    if not isinstance(provided_api_key, str):
        raise TypeError("auth_input field 'provided_api_key' must be a str")

    authorized_api_keys = auth_input["authorized_api_keys"]
    if not isinstance(authorized_api_keys, list):
        raise TypeError("auth_input field 'authorized_api_keys' must be a list")

    normalized_authorized_api_keys: list[str] = []
    for index, key in enumerate(authorized_api_keys):
        if not isinstance(key, str):
            raise TypeError(
                "auth_input field 'authorized_api_keys' entry at index "
                f"{index} must be a str"
            )
        if not key:
            raise ValueError(
                "auth_input field 'authorized_api_keys' entry at index "
                f"{index} must be a non-empty string"
            )
        normalized_authorized_api_keys.append(key)

    if provided_api_key == "":
        status = "unauthorized"
        reasons = ["api_key_missing"]
    elif not normalized_authorized_api_keys:
        status = "unauthorized"
        reasons = ["authorized_key_set_empty"]
    elif provided_api_key in normalized_authorized_api_keys:
        status = "authorized"
        reasons = ["api_key_authorized"]
    else:
        status = "unauthorized"
        reasons = ["api_key_not_authorized"]

    response: dict[str, Any] = {
        "status": status,
        "reasons": reasons,
    }

    if tuple(response.keys()) != _EXPECTED_OUTPUT_KEYS:
        raise RuntimeError("internal error: api key auth response schema mismatch")

    return response
