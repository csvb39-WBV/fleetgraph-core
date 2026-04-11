from __future__ import annotations

import hashlib
import re
from datetime import date, datetime
from typing import Any

__all__ = [
    "normalize_signal_batch",
    "normalize_signal_record",
]

_DATE_ONLY_PATTERN = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_DATETIME_PREFIX_PATTERN = re.compile(r"^\d{4}-\d{2}-\d{2}T")


def _normalize_optional_text(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    normalized_value = value.strip()
    if normalized_value == "":
        return None
    return normalized_value


def _resolve_company_name(record: dict[str, Any]) -> str | None:
    for field_name in ("company_name", "company"):
        company_name = _normalize_optional_text(record.get(field_name))
        if company_name is not None:
            return company_name
    return None


def _stable_company_id(company_name: str) -> str:
    slug_base = re.sub(r"[^a-z0-9]+", "-", company_name.lower()).strip("-")
    if slug_base == "":
        slug_base = "company"
    digest = hashlib.sha256(company_name.strip().lower().encode("utf-8")).hexdigest()[:8]
    return f"{slug_base}--signal--{digest}"


def _resolve_company_id(record: dict[str, Any], company_name: str) -> str:
    explicit_company_id = _normalize_optional_text(record.get("company_id"))
    if explicit_company_id is not None:
        return explicit_company_id
    return _stable_company_id(company_name)


def _resolve_signal_detail(record: dict[str, Any]) -> str | None:
    for field_name in ("signal_detail", "event_summary", "raw_text"):
        signal_detail = _normalize_optional_text(record.get(field_name))
        if signal_detail is not None:
            return signal_detail
    source_event_id = _normalize_optional_text(record.get("source_event_id"))
    source_event_type = _normalize_optional_text(record.get("source_event_type"))
    if source_event_id is not None and source_event_type is not None:
        return f"{source_event_type}:{source_event_id}"
    return None


def _parse_event_date(value: object) -> date | datetime | None:
    if isinstance(value, datetime):
        return value
    if isinstance(value, date):
        return value
    if not isinstance(value, str):
        return None

    normalized_value = value.strip()
    if normalized_value == "" or normalized_value.lower() == "unknown":
        return None
    if normalized_value.endswith("Z"):
        normalized_value = normalized_value[:-1] + "+00:00"
    if _DATE_ONLY_PATTERN.match(normalized_value):
        return date.fromisoformat(normalized_value)
    if _DATETIME_PREFIX_PATTERN.match(normalized_value):
        return datetime.fromisoformat(normalized_value)
    try:
        return datetime.fromisoformat(normalized_value)
    except ValueError:
        return None


def _resolve_event_date(record: dict[str, Any]) -> date | datetime | None:
    for field_name in ("event_date", "date_detected", "date"):
        event_date = _parse_event_date(record.get(field_name))
        if event_date is not None:
            return event_date
    return None


def _resolve_signal_type(record: dict[str, Any]) -> str | None:
    signal_type = _normalize_optional_text(record.get("signal_type"))
    if signal_type is None:
        return None
    return signal_type.lower()


def _resolve_source_url(record: dict[str, Any]) -> str | None:
    for field_name in ("source_url", "source", "url"):
        source_url = _normalize_optional_text(record.get(field_name))
        if source_url is None:
            continue
        if source_url.lower() == "unknown":
            return None
        return source_url
    return None


def normalize_signal_record(record: object) -> dict[str, Any] | None:
    if not isinstance(record, dict):
        return None

    company_name = _resolve_company_name(record)
    signal_type = _resolve_signal_type(record)
    signal_detail = _resolve_signal_detail(record)
    event_date = _resolve_event_date(record)

    if company_name is None:
        return None
    if signal_type is None:
        return None
    if signal_detail is None:
        return None
    if event_date is None:
        return None

    return {
        "company_id": _resolve_company_id(record, company_name),
        "company_name": company_name,
        "signal_type": signal_type,
        "signal_detail": signal_detail,
        "event_date": event_date,
        "source_url": _resolve_source_url(record),
    }


def normalize_signal_batch(records: list[object]) -> list[dict[str, Any]]:
    if not isinstance(records, list):
        raise ValueError("records must be a list")

    normalized_records: list[dict[str, Any]] = []
    for record in records:
        normalized_record = normalize_signal_record(record)
        if normalized_record is None:
            continue
        normalized_records.append(normalized_record)
    return normalized_records
