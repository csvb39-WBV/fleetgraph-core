from __future__ import annotations

import hashlib
import json
from copy import deepcopy
from datetime import datetime, timedelta
from typing import Any

__all__ = [
    "build_outreach_execution_plan",
    "build_sender_payloads",
    "resolve_next_send_window",
]

_REQUIRED_DRAFT_FIELDS = (
    "prospect_id",
    "company_id",
    "company_name",
    "contact",
    "selected_bucket",
    "signal_type",
    "signal_detail",
    "subject",
    "body",
)
_REQUIRED_CONTACT_FIELDS = (
    "name",
    "email",
    "priority_rank",
)
_DEFAULT_MAX_SEQUENCE_LENGTH = 4
_DEFAULT_MAX_EMAILS_PER_SEND_WINDOW = 50
_APPROVED_WINDOWS = (
    (1, "TUESDAY_0915"),
    (4, "FRIDAY_0915"),
)
_APPROVED_HOUR = 9
_APPROVED_MINUTE = 15


def _normalize_non_empty_string(value: object, *, field_name: str) -> str:
    if not isinstance(value, str):
        raise ValueError(f"{field_name} must be a non-empty string")
    normalized_value = value.strip()
    if normalized_value == "":
        raise ValueError(f"{field_name} must be a non-empty string")
    return normalized_value


def _normalize_priority_rank(value: object, *, field_name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"{field_name} must be an int")
    return value


def _coerce_datetime(value: object, *, field_name: str) -> datetime:
    if not isinstance(value, datetime):
        raise ValueError(f"{field_name} must be a datetime")
    return value


def _normalize_contact(contact: object, *, draft_index: int) -> dict[str, Any]:
    if not isinstance(contact, dict):
        raise ValueError(f"message_drafts[{draft_index}].contact must be a dict")

    normalized_contact: dict[str, Any] = {}
    for field_name in _REQUIRED_CONTACT_FIELDS:
        if field_name not in contact:
            raise ValueError(f"message_drafts[{draft_index}].contact missing required field: {field_name}")

    normalized_contact["name"] = _normalize_non_empty_string(
        contact.get("name"),
        field_name=f"message_drafts[{draft_index}].contact.name",
    )
    normalized_contact["email"] = _normalize_non_empty_string(
        contact.get("email"),
        field_name=f"message_drafts[{draft_index}].contact.email",
    )
    normalized_contact["priority_rank"] = _normalize_priority_rank(
        contact.get("priority_rank"),
        field_name=f"message_drafts[{draft_index}].contact.priority_rank",
    )
    if "title" in contact and contact.get("title") is not None:
        normalized_contact["title"] = _normalize_non_empty_string(
            contact.get("title"),
            field_name=f"message_drafts[{draft_index}].contact.title",
        )
    return normalized_contact


def _normalize_message_draft(draft: object, *, draft_index: int) -> dict[str, Any]:
    if not isinstance(draft, dict):
        raise ValueError(f"message_drafts[{draft_index}] must be a dict")

    normalized_draft: dict[str, Any] = {}
    for field_name in _REQUIRED_DRAFT_FIELDS:
        if field_name not in draft:
            raise ValueError(f"message_drafts[{draft_index}] missing required field: {field_name}")

    for field_name in (
        "prospect_id",
        "company_id",
        "company_name",
        "selected_bucket",
        "signal_type",
        "signal_detail",
        "subject",
        "body",
    ):
        normalized_draft[field_name] = _normalize_non_empty_string(
            draft.get(field_name),
            field_name=f"message_drafts[{draft_index}].{field_name}",
        )

    normalized_draft["contact"] = _normalize_contact(draft.get("contact"), draft_index=draft_index)
    return normalized_draft


def _format_scheduled_send_at(value: datetime) -> str:
    return value.isoformat(timespec="seconds")


def resolve_next_send_window(reference_datetime: datetime) -> dict[str, Any]:
    normalized_reference = _coerce_datetime(reference_datetime, field_name="reference_datetime")
    candidate_windows: list[tuple[datetime, str]] = []
    for day_offset in range(0, 15):
        candidate_day = normalized_reference + timedelta(days=day_offset)
        weekday = candidate_day.weekday()
        for approved_weekday, window_name in _APPROVED_WINDOWS:
            if weekday != approved_weekday:
                continue
            candidate_window = candidate_day.replace(
                hour=_APPROVED_HOUR,
                minute=_APPROVED_MINUTE,
                second=0,
                microsecond=0,
            )
            if candidate_window >= normalized_reference:
                candidate_windows.append((candidate_window, window_name))
    if candidate_windows == []:
        raise ValueError("unable to resolve next approved send window")
    candidate_windows.sort(key=lambda item: (item[0], item[1]))
    next_window_at, next_window_name = candidate_windows[0]
    return {
        "send_window": next_window_name,
        "scheduled_send_at": _format_scheduled_send_at(next_window_at),
        "scheduled_send_datetime": next_window_at,
    }


def _deterministic_jitter_minutes(
    draft: dict[str, Any],
    *,
    sequence_step: int,
    max_jitter_minutes: int,
) -> int:
    if max_jitter_minutes <= 0:
        return 0
    digest_source = json.dumps(
        [
            draft["prospect_id"],
            draft["company_id"],
            draft["contact"]["email"],
            sequence_step,
        ],
        ensure_ascii=True,
        separators=(",", ":"),
    )
    digest = hashlib.sha256(digest_source.encode("utf-8")).hexdigest()
    return int(digest[:8], 16) % (max_jitter_minutes + 1)


def _scheduled_window_for_step(
    draft: dict[str, Any],
    *,
    reference_datetime: datetime,
    sequence_step: int,
    max_jitter_minutes: int,
) -> dict[str, Any]:
    candidate_reference = reference_datetime
    for _ in range(1, sequence_step):
        previous_window = resolve_next_send_window(candidate_reference)
        candidate_reference = previous_window["scheduled_send_datetime"] + timedelta(seconds=1)

    resolved_window = resolve_next_send_window(candidate_reference)
    jitter_minutes = _deterministic_jitter_minutes(
        draft,
        sequence_step=sequence_step,
        max_jitter_minutes=max_jitter_minutes,
    )
    scheduled_datetime = resolved_window["scheduled_send_datetime"] + timedelta(minutes=jitter_minutes)
    return {
        "send_window": resolved_window["send_window"],
        "scheduled_send_at": _format_scheduled_send_at(scheduled_datetime),
        "scheduled_send_datetime": scheduled_datetime,
    }


def _draft_id(plan_record: dict[str, Any]) -> str:
    digest_source = json.dumps(
        {
            "prospect_id": plan_record["prospect_id"],
            "company_id": plan_record["company_id"],
            "contact_email": plan_record["contact_email"],
            "sequence_step": plan_record["sequence_step"],
            "scheduled_send_at": plan_record["scheduled_send_at"],
            "subject": plan_record["subject"],
            "body": plan_record["body"],
        },
        ensure_ascii=True,
        separators=(",", ":"),
    )
    digest = hashlib.sha256(digest_source.encode("utf-8")).hexdigest()[:16]
    return f"draft:{plan_record['prospect_id']}:{digest}"


def _candidate_sort_key(candidate: dict[str, Any]) -> tuple[str, int, int, str]:
    return (
        str(candidate["scheduled_send_at"]),
        int(candidate["input_order"]),
        int(candidate["sequence_step"]),
        str(candidate["contact_email"]).lower(),
    )


def build_outreach_execution_plan(
    message_drafts: list[object],
    *,
    reference_datetime: datetime,
    max_sequence_length: int = _DEFAULT_MAX_SEQUENCE_LENGTH,
    max_emails_per_send_window: int = _DEFAULT_MAX_EMAILS_PER_SEND_WINDOW,
    max_jitter_minutes: int = 0,
) -> dict[str, Any]:
    if not isinstance(message_drafts, list):
        raise ValueError("message_drafts must be a list")
    if max_sequence_length <= 0:
        raise ValueError("max_sequence_length must be greater than 0")
    if max_emails_per_send_window <= 0:
        raise ValueError("max_emails_per_send_window must be greater than 0")
    if max_jitter_minutes < 0:
        raise ValueError("max_jitter_minutes must be greater than or equal to 0")

    normalized_reference = _coerce_datetime(reference_datetime, field_name="reference_datetime")
    normalized_drafts = [
        _normalize_message_draft(draft, draft_index=index)
        for index, draft in enumerate(message_drafts)
    ]

    candidate_records: list[dict[str, Any]] = []
    for input_order, draft in enumerate(normalized_drafts):
        for sequence_step in range(1, max_sequence_length + 1):
            scheduled_window = _scheduled_window_for_step(
                draft,
                reference_datetime=normalized_reference,
                sequence_step=sequence_step,
                max_jitter_minutes=max_jitter_minutes,
            )
            candidate_record = {
                "prospect_id": draft["prospect_id"],
                "company_id": draft["company_id"],
                "company_name": draft["company_name"],
                "contact_email": draft["contact"]["email"],
                "contact_name": draft["contact"]["name"],
                "sequence_step": sequence_step,
                "send_window": scheduled_window["send_window"],
                "scheduled_send_at": scheduled_window["scheduled_send_at"],
                "subject": draft["subject"],
                "body": draft["body"],
                "input_order": input_order,
                "scheduled_send_datetime": scheduled_window["scheduled_send_datetime"],
            }
            candidate_record["draft_id"] = _draft_id(candidate_record)
            candidate_records.append(candidate_record)

    candidate_records.sort(key=_candidate_sort_key)

    planned_sends: list[dict[str, Any]] = []
    overflow: list[dict[str, Any]] = []
    counts_by_window: dict[str, int] = {}
    for candidate in candidate_records:
        window_name = str(candidate["send_window"])
        current_count = counts_by_window.get(window_name, 0)
        if current_count >= max_emails_per_send_window:
            overflow.append(
                {
                    "prospect_id": candidate["prospect_id"],
                    "company_id": candidate["company_id"],
                    "company_name": candidate["company_name"],
                    "contact_email": candidate["contact_email"],
                    "contact_name": candidate["contact_name"],
                    "sequence_step": candidate["sequence_step"],
                    "send_window": candidate["send_window"],
                    "scheduled_send_at": candidate["scheduled_send_at"],
                    "subject": candidate["subject"],
                    "body": candidate["body"],
                    "draft_id": candidate["draft_id"],
                    "overflow_reason": "send_window_cap_reached",
                }
            )
            continue

        counts_by_window[window_name] = current_count + 1
        planned_sends.append(
            {
                "draft_id": candidate["draft_id"],
                "prospect_id": candidate["prospect_id"],
                "company_id": candidate["company_id"],
                "company_name": candidate["company_name"],
                "contact_email": candidate["contact_email"],
                "contact_name": candidate["contact_name"],
                "sequence_step": candidate["sequence_step"],
                "send_window": candidate["send_window"],
                "scheduled_send_at": candidate["scheduled_send_at"],
                "subject": candidate["subject"],
                "body": candidate["body"],
            }
        )

    return {
        "planned_sends": planned_sends,
        "overflow": overflow,
        "window_counts": {window: counts_by_window[window] for window in sorted(counts_by_window)},
        "max_sequence_length": max_sequence_length,
        "max_emails_per_send_window": max_emails_per_send_window,
    }


def build_sender_payloads(plan_records: list[object]) -> list[dict[str, Any]]:
    if not isinstance(plan_records, list):
        raise ValueError("plan_records must be a list")

    sender_payloads: list[dict[str, Any]] = []
    for index, plan_record in enumerate(plan_records):
        if not isinstance(plan_record, dict):
            raise ValueError(f"plan_records[{index}] must be a dict")
        required_fields = (
            "draft_id",
            "prospect_id",
            "company_id",
            "company_name",
            "contact_email",
            "contact_name",
            "sequence_step",
            "send_window",
            "scheduled_send_at",
            "subject",
            "body",
        )
        for field_name in required_fields:
            if field_name not in plan_record:
                raise ValueError(f"plan_records[{index}] missing required field: {field_name}")

        sender_payloads.append(
            {
                "draft_id": _normalize_non_empty_string(
                    plan_record.get("draft_id"),
                    field_name=f"plan_records[{index}].draft_id",
                ),
                "recipient": {
                    "email": _normalize_non_empty_string(
                        plan_record.get("contact_email"),
                        field_name=f"plan_records[{index}].contact_email",
                    ),
                    "name": _normalize_non_empty_string(
                        plan_record.get("contact_name"),
                        field_name=f"plan_records[{index}].contact_name",
                    ),
                },
                "message": {
                    "subject": _normalize_non_empty_string(
                        plan_record.get("subject"),
                        field_name=f"plan_records[{index}].subject",
                    ),
                    "body": _normalize_non_empty_string(
                        plan_record.get("body"),
                        field_name=f"plan_records[{index}].body",
                    ),
                },
                "metadata": {
                    "prospect_id": _normalize_non_empty_string(
                        plan_record.get("prospect_id"),
                        field_name=f"plan_records[{index}].prospect_id",
                    ),
                    "company_id": _normalize_non_empty_string(
                        plan_record.get("company_id"),
                        field_name=f"plan_records[{index}].company_id",
                    ),
                    "company_name": _normalize_non_empty_string(
                        plan_record.get("company_name"),
                        field_name=f"plan_records[{index}].company_name",
                    ),
                    "sequence_step": int(plan_record["sequence_step"]),
                    "send_window": _normalize_non_empty_string(
                        plan_record.get("send_window"),
                        field_name=f"plan_records[{index}].send_window",
                    ),
                    "scheduled_send_at": _normalize_non_empty_string(
                        plan_record.get("scheduled_send_at"),
                        field_name=f"plan_records[{index}].scheduled_send_at",
                    ),
                },
            }
        )

    return deepcopy(sender_payloads)
