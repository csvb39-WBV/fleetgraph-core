"""Deterministic audit signal normalization for construction intelligence."""

from __future__ import annotations

from typing import Any

_REQUIRED_FIELDS: tuple[str, ...] = (
    "audit_id",
    "company_name",
    "agency",
    "issue_type",
    "severity",
    "opened_date",
    "status",
    "penalty_amount",
    "source_name",
)

_LOWERCASE_FIELDS: frozenset[str] = frozenset({"issue_type", "severity", "status"})
_SUPPORTED_SOURCES: frozenset[str] = frozenset({"regulatory_enforcement", "osha_citations"})


def _validate_required_string(value: object, field_name: str) -> str:
    if not isinstance(value, str):
        raise ValueError(f"{field_name} must be a string")

    normalized_value = value.strip()
    if normalized_value == "":
        raise ValueError(f"{field_name} cannot be empty or whitespace-only")

    if field_name in _LOWERCASE_FIELDS:
        return normalized_value.lower()

    return normalized_value


def _validate_penalty_amount(value: object) -> int | float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError("penalty_amount must be an int or float")

    return value


def parse_audit_signal(payload: dict[str, Any]) -> dict[str, Any]:
    """Normalize a raw audit or enforcement payload into a stable audit record."""
    if not isinstance(payload, dict):
        raise ValueError("payload must be a dictionary")

    normalized_output: dict[str, Any] = {}

    for field_name in _REQUIRED_FIELDS:
        if field_name not in payload:
            raise ValueError(f"{field_name} is required")

        field_value = payload[field_name]
        if field_name == "penalty_amount":
            normalized_output[field_name] = _validate_penalty_amount(field_value)
            continue

        normalized_output[field_name] = _validate_required_string(field_value, field_name)

    source_name = normalized_output["source_name"]
    if source_name not in _SUPPORTED_SOURCES:
        raise ValueError(f"source_name '{source_name}' is not supported")

    return normalized_output


__all__ = ["parse_audit_signal"]
