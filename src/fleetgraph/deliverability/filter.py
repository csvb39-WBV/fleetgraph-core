from __future__ import annotations

from copy import deepcopy
from datetime import datetime
from typing import Any

from fleetgraph.deliverability.domain_policy import evaluate_domain_policy
from fleetgraph.deliverability.rate_control import evaluate_bounce_protection
from fleetgraph.deliverability.suppression import is_suppressed, is_valid_contact_email
from fleetgraph.state.state_store import normalize_state_records

__all__ = [
    "evaluate_send_safety",
    "filter_execution_plan_for_deliverability",
]

_BLOCKED_STATE_STATUSES = {
    "SENT",
    "REPLIED",
    "BOUNCED",
    "UNSUBSCRIBED",
    "SUPPRESSED",
}
_REQUIRED_PLAN_FIELDS = (
    "draft_id",
    "company_id",
    "contact_email",
    "send_window",
    "scheduled_send_at",
)


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


def _extract_sender_domain(sender_domain: object) -> str:
    if not isinstance(sender_domain, str):
        raise ValueError("sender_domain must be a non-empty string")
    normalized_sender_domain = sender_domain.strip().lower()
    if normalized_sender_domain == "":
        raise ValueError("sender_domain must be a non-empty string")
    return normalized_sender_domain


def _normalize_plan_record(plan_record: object, *, index: int | None = None) -> dict[str, Any]:
    location = "plan_record" if index is None else f"execution_plan[{index}]"
    if not isinstance(plan_record, dict):
        raise ValueError(f"{location} must be a dict")
    for field_name in _REQUIRED_PLAN_FIELDS:
        if field_name not in plan_record:
            raise ValueError(f"{location} missing required field: {field_name}")
    normalized_record = deepcopy(plan_record)
    normalized_record["draft_id"] = _normalize_non_empty_string(
        plan_record.get("draft_id"),
        field_name=f"{location}.draft_id",
    )
    normalized_record["company_id"] = _normalize_non_empty_string(
        plan_record.get("company_id"),
        field_name=f"{location}.company_id",
    )
    normalized_record["contact_email"] = _normalize_non_empty_string(
        plan_record.get("contact_email"),
        field_name=f"{location}.contact_email",
    ).lower()
    normalized_record["send_window"] = _normalize_non_empty_string(
        plan_record.get("send_window"),
        field_name=f"{location}.send_window",
    )
    normalized_record["scheduled_send_at"] = _coerce_datetime(
        plan_record.get("scheduled_send_at"),
        field_name=f"{location}.scheduled_send_at",
    )
    return normalized_record


def _decision_details(plan_record: dict[str, Any], context: dict[str, Any]) -> dict[str, Any]:
    sender_domain = _extract_sender_domain(context.get("sender_domain"))
    suppression_records = list(context.get("suppression_list") or [])
    state_records = list(context.get("state_records") or [])
    domain_policies = list(context.get("domain_policies") or [])
    domain_metrics = list(context.get("domain_metrics") or [])
    send_counts_by_day = dict(context.get("send_counts_by_day") or {})
    send_counts_by_window = dict(context.get("send_counts_by_window") or {})
    stage_limits = context.get("stage_limits")
    bounce_threshold = float(context.get("bounce_threshold", 0.05))

    if not is_valid_contact_email(plan_record["contact_email"]):
        return {"allow_send": False, "reason": "invalid_contact_email"}
    if is_suppressed(plan_record["contact_email"], suppression_records):
        return {"allow_send": False, "reason": "suppressed_email"}

    normalized_state_records = normalize_state_records(state_records)
    state_records_by_draft_id = {
        str(record["draft_id"]): record
        for record in normalized_state_records
    }
    state_record = state_records_by_draft_id.get(str(plan_record["draft_id"]))
    if state_record is not None and str(state_record["status"]) in _BLOCKED_STATE_STATUSES:
        return {"allow_send": False, "reason": "blocked_by_state"}

    bounce_evaluation = evaluate_bounce_protection(
        sender_domain,
        domain_metrics,
        bounce_threshold=bounce_threshold,
    )
    if bounce_evaluation["allow_send"] is not True:
        return {"allow_send": False, "reason": "domain_degraded"}

    domain_policy_evaluation = evaluate_domain_policy(
        sender_domain,
        domain_policies,
        scheduled_send_at=plan_record["scheduled_send_at"],
        send_window=str(plan_record["send_window"]),
        send_counts_by_day=send_counts_by_day,
        send_counts_by_window=send_counts_by_window,
        stage_limits=stage_limits,
    )
    if domain_policy_evaluation["allow_send"] is not True:
        return {"allow_send": False, "reason": str(domain_policy_evaluation["reason"])}

    return {"allow_send": True, "reason": "safe_to_send"}


def evaluate_send_safety(plan_record: object, context: dict[str, Any]) -> bool:
    normalized_plan_record = _normalize_plan_record(plan_record)
    decision = _decision_details(normalized_plan_record, context)
    return bool(decision["allow_send"])


def filter_execution_plan_for_deliverability(
    execution_plan: list[object],
    *,
    sender_domain: str,
    state_records: list[object],
    domain_policies: list[object],
    suppression_list: list[object],
    domain_metrics: list[object],
    send_counts_by_day: dict[str, int] | None = None,
    send_counts_by_window: dict[str, int] | None = None,
    stage_limits: dict[int, int] | None = None,
    bounce_threshold: float = 0.05,
) -> list[dict[str, Any]]:
    if not isinstance(execution_plan, list):
        raise ValueError("execution_plan must be a list")

    normalized_sender_domain = _extract_sender_domain(sender_domain)
    running_daily_counts = dict(send_counts_by_day or {})
    running_window_counts = dict(send_counts_by_window or {})
    filtered_execution_plan: list[dict[str, Any]] = []

    for index, plan_record in enumerate(execution_plan):
        normalized_plan_record = _normalize_plan_record(plan_record, index=index)
        context = {
            "sender_domain": normalized_sender_domain,
            "state_records": state_records,
            "domain_policies": domain_policies,
            "suppression_list": suppression_list,
            "domain_metrics": domain_metrics,
            "send_counts_by_day": running_daily_counts,
            "send_counts_by_window": running_window_counts,
            "stage_limits": stage_limits,
            "bounce_threshold": bounce_threshold,
        }
        if evaluate_send_safety(normalized_plan_record, context) is not True:
            continue

        day_key = f"{normalized_sender_domain}|{normalized_plan_record['scheduled_send_at'].date().isoformat()}"
        window_key = (
            f"{normalized_sender_domain}|{normalized_plan_record['send_window']}|"
            f"{normalized_plan_record['scheduled_send_at'].isoformat(timespec='seconds')}"
        )
        running_daily_counts[day_key] = int(running_daily_counts.get(day_key, 0)) + 1
        running_window_counts[window_key] = int(running_window_counts.get(window_key, 0)) + 1
        accepted_record = deepcopy(normalized_plan_record)
        accepted_record["scheduled_send_at"] = accepted_record["scheduled_send_at"].isoformat(timespec="seconds")
        filtered_execution_plan.append(accepted_record)

    return filtered_execution_plan
