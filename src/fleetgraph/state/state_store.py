from __future__ import annotations

from copy import deepcopy
from datetime import datetime
from typing import Any

__all__ = [
    "build_state_store",
    "normalize_state_records",
]

_ALLOWED_STATUSES = {
    "PENDING",
    "SENT",
    "REPLIED",
    "BOUNCED",
    "UNSUBSCRIBED",
    "SUPPRESSED",
}
_REQUIRED_STATE_FIELDS = (
    "draft_id",
    "prospect_id",
    "company_id",
    "contact_email",
    "sequence_step",
    "status",
    "last_event_at",
    "next_scheduled_at",
)
_REQUIRED_PLAN_FIELDS = (
    "draft_id",
    "prospect_id",
    "company_id",
    "contact_email",
    "sequence_step",
    "scheduled_send_at",
)


def _normalize_non_empty_string(value: object, *, field_name: str) -> str:
    if not isinstance(value, str):
        raise ValueError(f"{field_name} must be a non-empty string")
    normalized_value = value.strip()
    if normalized_value == "":
        raise ValueError(f"{field_name} must be a non-empty string")
    return normalized_value


def _normalize_sequence_step(value: object, *, field_name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"{field_name} must be an int")
    if value <= 0:
        raise ValueError(f"{field_name} must be greater than 0")
    return value


def _coerce_datetime(value: object, *, field_name: str, allow_none: bool = False) -> datetime | None:
    if value is None:
        if allow_none:
            return None
        raise ValueError(f"{field_name} must be a datetime or ISO-8601 string")
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        normalized_value = value.strip()
        if normalized_value == "":
            if allow_none:
                return None
            raise ValueError(f"{field_name} must be a datetime or ISO-8601 string")
        return datetime.fromisoformat(normalized_value)
    raise ValueError(f"{field_name} must be a datetime or ISO-8601 string")


def _state_sort_key(record: dict[str, Any]) -> tuple[str, str, str, int, str]:
    next_scheduled_at = record["next_scheduled_at"]
    if next_scheduled_at is None:
        next_scheduled_component = "9999-12-31T23:59:59"
    else:
        next_scheduled_component = next_scheduled_at.isoformat(timespec="seconds")
    return (
        next_scheduled_component,
        str(record["prospect_id"]),
        str(record["contact_email"]).lower(),
        int(record["sequence_step"]),
        str(record["draft_id"]),
    )


def normalize_state_records(state_records: list[object]) -> list[dict[str, Any]]:
    if not isinstance(state_records, list):
        raise ValueError("state_records must be a list")

    normalized_records_by_draft_id: dict[str, dict[str, Any]] = {}
    for index, record in enumerate(state_records):
        if not isinstance(record, dict):
            raise ValueError(f"state_records[{index}] must be a dict")
        for field_name in _REQUIRED_STATE_FIELDS:
            if field_name not in record:
                raise ValueError(f"state_records[{index}] missing required field: {field_name}")

        normalized_record = {
            "draft_id": _normalize_non_empty_string(
                record.get("draft_id"),
                field_name=f"state_records[{index}].draft_id",
            ),
            "prospect_id": _normalize_non_empty_string(
                record.get("prospect_id"),
                field_name=f"state_records[{index}].prospect_id",
            ),
            "company_id": _normalize_non_empty_string(
                record.get("company_id"),
                field_name=f"state_records[{index}].company_id",
            ),
            "contact_email": _normalize_non_empty_string(
                record.get("contact_email"),
                field_name=f"state_records[{index}].contact_email",
            ),
            "sequence_step": _normalize_sequence_step(
                record.get("sequence_step"),
                field_name=f"state_records[{index}].sequence_step",
            ),
            "status": _normalize_non_empty_string(
                record.get("status"),
                field_name=f"state_records[{index}].status",
            ),
            "last_event_at": _coerce_datetime(
                record.get("last_event_at"),
                field_name=f"state_records[{index}].last_event_at",
            ),
            "next_scheduled_at": _coerce_datetime(
                record.get("next_scheduled_at"),
                field_name=f"state_records[{index}].next_scheduled_at",
                allow_none=True,
            ),
        }
        if normalized_record["status"] not in _ALLOWED_STATUSES:
            raise ValueError(f"state_records[{index}].status is invalid")
        normalized_records_by_draft_id[normalized_record["draft_id"]] = normalized_record

    normalized_records = list(normalized_records_by_draft_id.values())
    normalized_records.sort(key=_state_sort_key)
    return deepcopy(normalized_records)


def build_state_store(execution_plan_records: list[object]) -> list[dict[str, Any]]:
    if not isinstance(execution_plan_records, list):
        raise ValueError("execution_plan_records must be a list")

    state_records: list[dict[str, Any]] = []
    for index, record in enumerate(execution_plan_records):
        if not isinstance(record, dict):
            raise ValueError(f"execution_plan_records[{index}] must be a dict")
        for field_name in _REQUIRED_PLAN_FIELDS:
            if field_name not in record:
                raise ValueError(f"execution_plan_records[{index}] missing required field: {field_name}")

        scheduled_send_at = _coerce_datetime(
            record.get("scheduled_send_at"),
            field_name=f"execution_plan_records[{index}].scheduled_send_at",
        )
        state_records.append(
            {
                "draft_id": _normalize_non_empty_string(
                    record.get("draft_id"),
                    field_name=f"execution_plan_records[{index}].draft_id",
                ),
                "prospect_id": _normalize_non_empty_string(
                    record.get("prospect_id"),
                    field_name=f"execution_plan_records[{index}].prospect_id",
                ),
                "company_id": _normalize_non_empty_string(
                    record.get("company_id"),
                    field_name=f"execution_plan_records[{index}].company_id",
                ),
                "contact_email": _normalize_non_empty_string(
                    record.get("contact_email"),
                    field_name=f"execution_plan_records[{index}].contact_email",
                ),
                "sequence_step": _normalize_sequence_step(
                    record.get("sequence_step"),
                    field_name=f"execution_plan_records[{index}].sequence_step",
                ),
                "status": "PENDING",
                "last_event_at": scheduled_send_at,
                "next_scheduled_at": scheduled_send_at,
            }
        )

    return normalize_state_records(state_records)
