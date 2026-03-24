"""Deterministic litigation signal parser for normalized case records."""

from __future__ import annotations

from typing import Any


_REQUIRED_FIELDS: tuple[str, ...] = (
    "case_id",
    "company_name",
    "jurisdiction",
    "case_type",
    "filing_date",
    "status",
    "alleged_amount",
    "plaintiff_role",
    "defendant_role",
    "source_name",
)

_LOWERCASE_FIELDS: set[str] = {
    "case_type",
    "status",
    "plaintiff_role",
    "defendant_role",
}


def _require_payload_dict(payload: Any) -> dict[str, Any]:
    if not isinstance(payload, dict):
        raise ValueError("payload must be a dictionary")
    return payload


def _require_field(payload: dict[str, Any], field_name: str) -> Any:
    if field_name not in payload:
        raise ValueError(f"missing required field: {field_name}")
    return payload[field_name]


def _normalize_required_string(value: Any, field_name: str, *, lowercase: bool) -> str:
    if not isinstance(value, str):
        raise ValueError(f"{field_name} must be a non-empty string")

    normalized = value.strip()
    if not normalized:
        raise ValueError(f"{field_name} must be a non-empty string")

    if lowercase:
        return normalized.lower()
    return normalized


def _normalize_alleged_amount(value: Any) -> int | float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError("alleged_amount must be a numeric value")
    return value


def parse_litigation_signal(payload: dict[str, Any]) -> dict[str, Any]:
    """Parse a raw litigation payload into a normalized case-record contract."""
    payload_dict = _require_payload_dict(payload)

    output: dict[str, Any] = {}

    for field_name in _REQUIRED_FIELDS:
        raw_value = _require_field(payload_dict, field_name)

        if field_name == "alleged_amount":
            output[field_name] = _normalize_alleged_amount(raw_value)
            continue

        output[field_name] = _normalize_required_string(
            raw_value,
            field_name,
            lowercase=field_name in _LOWERCASE_FIELDS,
        )

    if output["source_name"] != "court_dockets":
        raise ValueError("source_name must be 'court_dockets'")

    return output


__all__ = ["parse_litigation_signal"]
