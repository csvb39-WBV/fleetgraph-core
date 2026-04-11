from __future__ import annotations

from copy import deepcopy
from datetime import datetime
from typing import Any

__all__ = [
    "process_response_events",
]

_ALLOWED_EVENT_TYPES = {
    "REPLIED",
    "BOUNCED",
    "UNSUBSCRIBED",
}


def _normalize_non_empty_string(value: object, *, field_name: str) -> str:
    if not isinstance(value, str):
        raise ValueError(f"{field_name} must be a non-empty string")
    normalized_value = value.strip()
    if normalized_value == "":
        raise ValueError(f"{field_name} must be a non-empty string")
    return normalized_value


def _coerce_datetime(value: object, *, field_name: str) -> datetime:
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        normalized_value = value.strip()
        if normalized_value == "":
            raise ValueError(f"{field_name} must be a datetime or ISO-8601 string")
        return datetime.fromisoformat(normalized_value)
    raise ValueError(f"{field_name} must be a datetime or ISO-8601 string")


def process_response_events(events: list[dict[str, object]]) -> list[dict[str, Any]]:
    if not isinstance(events, list):
        raise ValueError("events must be a list")

    updates: list[dict[str, Any]] = []
    for index, event in enumerate(events):
        if not isinstance(event, dict):
            raise ValueError(f"events[{index}] must be a dict")
        draft_id = _normalize_non_empty_string(
            event.get("draft_id"),
            field_name=f"events[{index}].draft_id",
        )
        event_type = _normalize_non_empty_string(
            event.get("event_type"),
            field_name=f"events[{index}].event_type",
        )
        if event_type not in _ALLOWED_EVENT_TYPES:
            raise ValueError(f"events[{index}].event_type is invalid")
        timestamp = _coerce_datetime(
            event.get("timestamp"),
            field_name=f"events[{index}].timestamp",
        )
        updates.append(
            {
                "draft_id": draft_id,
                "status": event_type,
                "last_event_at": timestamp,
                "next_scheduled_at": None,
            }
        )

    updates.sort(
        key=lambda row: (
            row["last_event_at"].isoformat(timespec="seconds"),
            str(row["draft_id"]),
            str(row["status"]),
        )
    )
    return deepcopy(updates)
