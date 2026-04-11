from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any

from fleetgraph.state.state_store import normalize_state_records

__all__ = [
    "is_in_cooldown",
]

_TERMINAL_REPLY_STATUSES = {
    "REPLIED",
    "BOUNCED",
    "UNSUBSCRIBED",
}


def _coerce_datetime(value: object, *, field_name: str) -> datetime:
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        normalized_value = value.strip()
        if normalized_value == "":
            raise ValueError(f"{field_name} must be a datetime or ISO-8601 string")
        return datetime.fromisoformat(normalized_value)
    raise ValueError(f"{field_name} must be a datetime or ISO-8601 string")


def _latest_completed_sequence_event(company_state_records: list[dict[str, Any]]) -> datetime | None:
    grouped_records: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for record in company_state_records:
        group_key = (str(record["prospect_id"]), str(record["contact_email"]).lower())
        grouped_records.setdefault(group_key, []).append(record)

    completed_at: datetime | None = None
    for grouped_state_records in grouped_records.values():
        grouped_state_records.sort(key=lambda row: int(row["sequence_step"]))
        final_record = grouped_state_records[-1]
        if str(final_record["status"]) == "SENT":
            final_last_event_at = final_record["last_event_at"]
            if completed_at is None or final_last_event_at > completed_at:
                completed_at = final_last_event_at
    return completed_at


def is_in_cooldown(
    company_id: str,
    reference_datetime: datetime,
    state_records: list[object],
    *,
    cooldown_days: int = 45,
) -> bool:
    if not isinstance(company_id, str) or company_id.strip() == "":
        raise ValueError("company_id must be a non-empty string")
    if cooldown_days < 0:
        raise ValueError("cooldown_days must be greater than or equal to 0")

    normalized_reference = _coerce_datetime(reference_datetime, field_name="reference_datetime")
    normalized_state_records = normalize_state_records(state_records)
    company_state_records = [
        record for record in normalized_state_records if str(record["company_id"]) == company_id.strip()
    ]
    if company_state_records == []:
        return False

    latest_blocking_event_at: datetime | None = None
    for record in company_state_records:
        if str(record["status"]) in _TERMINAL_REPLY_STATUSES:
            last_event_at = record["last_event_at"]
            if latest_blocking_event_at is None or last_event_at > latest_blocking_event_at:
                latest_blocking_event_at = last_event_at

    completed_sequence_at = _latest_completed_sequence_event(company_state_records)
    if completed_sequence_at is not None:
        if latest_blocking_event_at is None or completed_sequence_at > latest_blocking_event_at:
            latest_blocking_event_at = completed_sequence_at

    if latest_blocking_event_at is None:
        return False
    return normalized_reference < (latest_blocking_event_at + timedelta(days=cooldown_days))
