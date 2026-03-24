"""Canonical deterministic schema for construction unified event records."""

from __future__ import annotations

from copy import deepcopy
from typing import Any


_SUPPORTED_EVENT_TYPES: tuple[str, ...] = (
    "litigation",
    "audit",
    "enforcement",
    "lien",
    "bond_claim",
)

_SUPPORTED_STATUSES: set[str] = {
    "open",
    "active",
    "pending",
    "resolved",
    "closed",
}

_SUPPORTED_SEVERITIES: set[str] = {
    "low",
    "medium",
    "high",
    "critical",
}

_CANONICAL_TOP_LEVEL_KEYS: tuple[str, ...] = (
    "event_id",
    "event_type",
    "company_name",
    "source_name",
    "status",
    "event_date",
    "jurisdiction",
    "project_name",
    "agency_or_court",
    "severity",
    "amount",
    "currency",
    "service_fit",
    "trigger_tags",
    "evidence",
    "event_details",
)

_REQUIRED_NON_EMPTY_STRING_FIELDS: tuple[str, ...] = (
    "event_id",
    "event_type",
    "company_name",
    "source_name",
    "status",
)

_OPTIONAL_NULLABLE_STRING_FIELDS: tuple[str, ...] = (
    "event_date",
    "jurisdiction",
    "project_name",
    "agency_or_court",
    "severity",
    "currency",
)

_EVENT_DETAIL_REQUIRED_FIELDS: dict[str, tuple[str, ...]] = {
    "litigation": (
        "case_id",
        "case_type",
        "filing_date",
        "plaintiff_role",
        "defendant_role",
    ),
    "audit": (
        "audit_id",
        "issue_type",
        "opened_date",
        "agency",
    ),
    "enforcement": (
        "action_id",
        "issue_type",
        "opened_date",
        "agency",
    ),
    "lien": (
        "lien_id",
        "filing_date",
        "claimant_role",
    ),
    "bond_claim": (
        "bond_claim_id",
        "filing_date",
        "claimant_role",
    ),
}


def get_supported_event_types() -> tuple[str, ...]:
    """Return deterministic tuple of supported unified event types."""
    return _SUPPORTED_EVENT_TYPES


def _require_dict(value: Any, field_name: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ValueError(f"{field_name} must be a dictionary.")
    return value


def _require_non_empty_string(value: Any, field_name: str) -> str:
    if not isinstance(value, str):
        raise ValueError(f"{field_name} must be a non-empty string.")

    normalized = value.strip()
    if not normalized:
        raise ValueError(f"{field_name} must be a non-empty string.")

    return normalized


def _normalize_nullable_string(value: Any, field_name: str) -> str | None:
    if value is None:
        return None

    if not isinstance(value, str):
        raise ValueError(f"{field_name} must be a non-empty string or None.")

    normalized = value.strip()
    if not normalized:
        raise ValueError(f"{field_name} must be a non-empty string or None.")

    return normalized


def _normalize_amount(value: Any) -> int | float | None:
    if value is None:
        return None

    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError("amount must be a non-negative number.")

    if value < 0:
        raise ValueError("amount must be a non-negative number.")

    return value


def _normalize_string_list(value: Any, field_name: str) -> list[str]:
    if not isinstance(value, list):
        raise ValueError(f"{field_name} must be a list.")

    normalized_items: list[str] = []
    for index, item in enumerate(value):
        if not isinstance(item, str):
            raise ValueError(f"{field_name}[{index}] must be a non-empty string.")

        normalized_item = item.strip()
        if not normalized_item:
            raise ValueError(f"{field_name}[{index}] must be a non-empty string.")

        normalized_items.append(normalized_item)

    return normalized_items


def _normalize_evidence(value: Any) -> dict[str, str | None]:
    evidence = _require_dict(value, "evidence")

    expected_keys = {"summary", "source_record_id"}
    if set(evidence.keys()) != expected_keys:
        raise ValueError("evidence must contain exactly: summary, source_record_id.")

    return {
        "summary": _normalize_nullable_string(evidence.get("summary"), "evidence.summary"),
        "source_record_id": _normalize_nullable_string(
            evidence.get("source_record_id"),
            "evidence.source_record_id",
        ),
    }


def _normalize_event_details(value: Any, event_type: str) -> dict[str, Any]:
    event_details = _require_dict(value, "event_details")

    required_fields = _EVENT_DETAIL_REQUIRED_FIELDS[event_type]
    for field_name in required_fields:
        if field_name not in event_details:
            raise ValueError(f"event_details.{field_name} is required.")
        _require_non_empty_string(event_details[field_name], f"event_details.{field_name}")

    normalized_details = deepcopy(event_details)
    for field_name in required_fields:
        normalized_details[field_name] = str(normalized_details[field_name]).strip()

    return normalized_details


def _validate_top_level_required_fields(record: dict[str, Any]) -> None:
    expected_keys = set(_CANONICAL_TOP_LEVEL_KEYS)
    actual_keys = set(record.keys())
    if actual_keys != expected_keys:
        raise ValueError("record must contain exactly the canonical top-level keys.")


def _normalize_unified_event_record(record: dict[str, Any]) -> dict[str, Any]:
    _validate_top_level_required_fields(record)

    normalized: dict[str, Any] = {}

    for field_name in _REQUIRED_NON_EMPTY_STRING_FIELDS:
        normalized[field_name] = _require_non_empty_string(record[field_name], field_name)

    event_type = normalized["event_type"]
    if event_type not in _SUPPORTED_EVENT_TYPES:
        raise ValueError("event_type must be one of the supported event types.")

    status = normalized["status"]
    if status not in _SUPPORTED_STATUSES:
        raise ValueError("status must be one of the supported statuses.")

    for field_name in _OPTIONAL_NULLABLE_STRING_FIELDS:
        normalized[field_name] = _normalize_nullable_string(record[field_name], field_name)

    severity = normalized["severity"]
    if severity is not None and severity not in _SUPPORTED_SEVERITIES:
        raise ValueError("severity must be one of the supported severity levels.")

    normalized["amount"] = _normalize_amount(record["amount"])
    normalized["service_fit"] = _normalize_string_list(record["service_fit"], "service_fit")
    normalized["trigger_tags"] = _normalize_string_list(record["trigger_tags"], "trigger_tags")
    normalized["evidence"] = _normalize_evidence(record["evidence"])
    normalized["event_details"] = _normalize_event_details(record["event_details"], event_type)

    return {
        "event_id": normalized["event_id"],
        "event_type": normalized["event_type"],
        "company_name": normalized["company_name"],
        "source_name": normalized["source_name"],
        "status": normalized["status"],
        "event_date": normalized["event_date"],
        "jurisdiction": normalized["jurisdiction"],
        "project_name": normalized["project_name"],
        "agency_or_court": normalized["agency_or_court"],
        "severity": normalized["severity"],
        "amount": normalized["amount"],
        "currency": normalized["currency"],
        "service_fit": normalized["service_fit"],
        "trigger_tags": normalized["trigger_tags"],
        "evidence": normalized["evidence"],
        "event_details": normalized["event_details"],
    }


def build_unified_event_record(record: dict[str, Any]) -> dict[str, Any]:
    """Build a canonical normalized unified event record from input data."""
    record_dict = _require_dict(record, "record")
    return _normalize_unified_event_record(record_dict)


def validate_unified_event_record(record: dict[str, Any]) -> bool:
    """Validate a candidate unified event record against canonical schema rules."""
    record_dict = _require_dict(record, "record")

    expected_keys = set(_CANONICAL_TOP_LEVEL_KEYS)
    actual_keys = set(record_dict.keys())
    if actual_keys != expected_keys:
        raise ValueError("record must contain exactly the canonical top-level keys.")

    _normalize_unified_event_record(record_dict)
    return True


__all__ = [
    "build_unified_event_record",
    "validate_unified_event_record",
    "get_supported_event_types",
]
