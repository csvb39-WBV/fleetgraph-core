from __future__ import annotations

from copy import deepcopy
from datetime import datetime
from typing import Any

from fleetgraph.state.cooldown import is_in_cooldown
from fleetgraph.state.state_store import normalize_state_records

__all__ = [
    "apply_state_updates",
    "detect_conversion_signals",
    "filter_execution_plan",
]

_ALLOWED_STATUSES = {
    "PENDING",
    "SENT",
    "REPLIED",
    "BOUNCED",
    "UNSUBSCRIBED",
    "SUPPRESSED",
}
_BLOCKED_EXECUTION_STATUSES = {
    "SENT",
    "REPLIED",
    "BOUNCED",
    "UNSUBSCRIBED",
    "SUPPRESSED",
}
_TERMINAL_SUPPRESSION_STATUSES = {
    "REPLIED",
    "BOUNCED",
    "UNSUBSCRIBED",
}
_ALLOWED_TRANSITIONS = {
    "PENDING": {"PENDING", "SENT", "SUPPRESSED"},
    "SENT": {"SENT", "REPLIED", "BOUNCED", "UNSUBSCRIBED", "SUPPRESSED"},
    "REPLIED": {"REPLIED", "SUPPRESSED"},
    "BOUNCED": {"BOUNCED", "SUPPRESSED"},
    "UNSUBSCRIBED": {"UNSUBSCRIBED", "SUPPRESSED"},
    "SUPPRESSED": {"SUPPRESSED"},
}


def _normalize_non_empty_string(value: object, *, field_name: str) -> str:
    if not isinstance(value, str):
        raise ValueError(f"{field_name} must be a non-empty string")
    normalized_value = value.strip()
    if normalized_value == "":
        raise ValueError(f"{field_name} must be a non-empty string")
    return normalized_value


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


def _normalize_update(update: object, *, index: int) -> dict[str, Any]:
    if not isinstance(update, dict):
        raise ValueError(f"updates[{index}] must be a dict")
    draft_id = _normalize_non_empty_string(update.get("draft_id"), field_name=f"updates[{index}].draft_id")
    status = _normalize_non_empty_string(update.get("status"), field_name=f"updates[{index}].status")
    if status not in _ALLOWED_STATUSES:
        raise ValueError(f"updates[{index}].status is invalid")
    last_event_at = _coerce_datetime(update.get("last_event_at"), field_name=f"updates[{index}].last_event_at")
    next_scheduled_at = _coerce_datetime(
        update.get("next_scheduled_at"),
        field_name=f"updates[{index}].next_scheduled_at",
        allow_none=True,
    )
    return {
        "draft_id": draft_id,
        "status": status,
        "last_event_at": last_event_at,
        "next_scheduled_at": next_scheduled_at,
    }


def _transition_status(current_status: str, next_status: str) -> str:
    allowed_targets = _ALLOWED_TRANSITIONS.get(current_status)
    if allowed_targets is None or next_status not in allowed_targets:
        raise ValueError(f"invalid transition: {current_status} -> {next_status}")
    return next_status


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


def _suppress_future_sequence_steps(state_records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    updated_records = deepcopy(state_records)
    trigger_records = [record for record in updated_records if str(record["status"]) in _TERMINAL_SUPPRESSION_STATUSES]
    if trigger_records == []:
        updated_records.sort(key=_state_sort_key)
        return updated_records

    for trigger_record in trigger_records:
        for candidate_record in updated_records:
            if candidate_record["draft_id"] == trigger_record["draft_id"]:
                continue
            if str(candidate_record["prospect_id"]) != str(trigger_record["prospect_id"]):
                continue
            if str(candidate_record["contact_email"]).lower() != str(trigger_record["contact_email"]).lower():
                continue
            if int(candidate_record["sequence_step"]) <= int(trigger_record["sequence_step"]):
                continue
            if candidate_record["next_scheduled_at"] is None:
                continue
            if candidate_record["next_scheduled_at"] <= trigger_record["last_event_at"]:
                continue
            if str(candidate_record["status"]) in _TERMINAL_SUPPRESSION_STATUSES:
                continue
            candidate_record["status"] = "SUPPRESSED"
            candidate_record["last_event_at"] = trigger_record["last_event_at"]
            candidate_record["next_scheduled_at"] = None

    updated_records.sort(key=_state_sort_key)
    return updated_records


def apply_state_updates(state_records: list[object], updates: list[object]) -> list[dict[str, Any]]:
    normalized_state_records = normalize_state_records(state_records)
    if not isinstance(updates, list):
        raise ValueError("updates must be a list")

    state_records_by_draft_id = {
        str(record["draft_id"]): deepcopy(record)
        for record in normalized_state_records
    }
    normalized_updates = [_normalize_update(update, index=index) for index, update in enumerate(updates)]
    normalized_updates.sort(
        key=lambda row: (
            row["last_event_at"].isoformat(timespec="seconds"),
            str(row["draft_id"]),
            str(row["status"]),
        )
    )

    for update in normalized_updates:
        draft_id = str(update["draft_id"])
        if draft_id not in state_records_by_draft_id:
            raise ValueError(f"unknown draft_id: {draft_id}")
        current_record = state_records_by_draft_id[draft_id]
        current_status = str(current_record["status"])
        next_status = str(update["status"])
        if current_status == next_status:
            continue
        current_record["status"] = _transition_status(current_status, next_status)
        current_record["last_event_at"] = update["last_event_at"]
        current_record["next_scheduled_at"] = update["next_scheduled_at"]

    updated_state_records = list(state_records_by_draft_id.values())
    updated_state_records = _suppress_future_sequence_steps(updated_state_records)
    updated_state_records.sort(key=_state_sort_key)
    return deepcopy(updated_state_records)


def detect_conversion_signals(events: list[object]) -> list[dict[str, Any]]:
    if not isinstance(events, list):
        raise ValueError("events must be a list")

    normalized_events: list[dict[str, Any]] = []
    for index, event in enumerate(events):
        if not isinstance(event, dict):
            raise ValueError(f"events[{index}] must be a dict")
        prospect_id = _normalize_non_empty_string(
            event.get("prospect_id"),
            field_name=f"events[{index}].prospect_id",
        )
        event_type = _normalize_non_empty_string(
            event.get("event_type"),
            field_name=f"events[{index}].event_type",
        )
        timestamp = _coerce_datetime(
            event.get("timestamp"),
            field_name=f"events[{index}].timestamp",
        )
        normalized_events.append(
            {
                "prospect_id": prospect_id,
                "event_type": event_type,
                "timestamp": timestamp,
            }
        )

    normalized_events.sort(
        key=lambda row: (
            str(row["prospect_id"]),
            row["timestamp"].isoformat(timespec="seconds"),
            str(row["event_type"]),
        )
    )

    grouped_events: dict[str, list[dict[str, Any]]] = {}
    for event in normalized_events:
        grouped_events.setdefault(str(event["prospect_id"]), []).append(event)

    conversion_signals: list[dict[str, Any]] = []
    for prospect_id in sorted(grouped_events):
        grouped_prospect_events = grouped_events[prospect_id]
        event_types = [str(event["event_type"]) for event in grouped_prospect_events]
        if "REPLIED" in event_types:
            reason = "reply_detected"
            conversion_flag = True
        elif event_types.count("OPENED") >= 2:
            reason = "multiple_email_opens"
            conversion_flag = True
        elif event_types.count("ENGAGED") >= 2:
            reason = "repeated_engagement_signals"
            conversion_flag = True
        else:
            reason = "no_conversion_signal"
            conversion_flag = False
        conversion_signals.append(
            {
                "prospect_id": prospect_id,
                "conversion_flag": conversion_flag,
                "reason": reason,
            }
        )

    return deepcopy(conversion_signals)


def filter_execution_plan(
    execution_plan_records: list[object],
    state_records: list[object],
    *,
    cooldown_days: int = 45,
) -> list[dict[str, Any]]:
    if not isinstance(execution_plan_records, list):
        raise ValueError("execution_plan_records must be a list")

    normalized_state_records = normalize_state_records(state_records)
    state_records_by_draft_id = {
        str(record["draft_id"]): record
        for record in normalized_state_records
    }

    filtered_plan: list[dict[str, Any]] = []
    for index, plan_record in enumerate(execution_plan_records):
        if not isinstance(plan_record, dict):
            raise ValueError(f"execution_plan_records[{index}] must be a dict")
        draft_id = _normalize_non_empty_string(
            plan_record.get("draft_id"),
            field_name=f"execution_plan_records[{index}].draft_id",
        )
        company_id = _normalize_non_empty_string(
            plan_record.get("company_id"),
            field_name=f"execution_plan_records[{index}].company_id",
        )
        scheduled_send_at = _coerce_datetime(
            plan_record.get("scheduled_send_at"),
            field_name=f"execution_plan_records[{index}].scheduled_send_at",
        )
        state_record = state_records_by_draft_id.get(draft_id)
        if state_record is not None and str(state_record["status"]) in _BLOCKED_EXECUTION_STATUSES:
            continue
        if is_in_cooldown(
            company_id,
            scheduled_send_at,
            normalized_state_records,
            cooldown_days=cooldown_days,
        ):
            continue
        filtered_plan.append(deepcopy(plan_record))

    filtered_plan.sort(
        key=lambda row: (
            str(row["scheduled_send_at"]),
            str(row["draft_id"]),
        )
    )
    return filtered_plan
