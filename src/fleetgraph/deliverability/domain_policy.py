from __future__ import annotations

from copy import deepcopy
from datetime import datetime
from typing import Any

__all__ = [
    "evaluate_domain_policy",
    "normalize_domain_policies",
    "resolve_domain_policy",
]

_REQUIRED_POLICY_FIELDS = (
    "domain",
    "max_daily_sends",
    "max_per_window",
    "warmup_stage",
    "reputation_score",
)


def _normalize_non_empty_string(value: object, *, field_name: str) -> str:
    if not isinstance(value, str):
        raise ValueError(f"{field_name} must be a non-empty string")
    normalized_value = value.strip()
    if normalized_value == "":
        raise ValueError(f"{field_name} must be a non-empty string")
    return normalized_value


def _normalize_domain(value: object, *, field_name: str) -> str:
    return _normalize_non_empty_string(value, field_name=field_name).lower()


def _normalize_positive_int(value: object, *, field_name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"{field_name} must be an int")
    if value <= 0:
        raise ValueError(f"{field_name} must be greater than 0")
    return value


def _normalize_float(value: object, *, field_name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"{field_name} must be numeric")
    numeric_value = float(value)
    if numeric_value < 0.0:
        raise ValueError(f"{field_name} must be greater than or equal to 0")
    return numeric_value


def _coerce_datetime(value: object, *, field_name: str) -> datetime:
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        normalized_value = value.strip()
        if normalized_value == "":
            raise ValueError(f"{field_name} must be a datetime or ISO-8601 string")
        return datetime.fromisoformat(normalized_value)
    raise ValueError(f"{field_name} must be a datetime or ISO-8601 string")


def normalize_domain_policies(policy_records: list[object]) -> list[dict[str, Any]]:
    if not isinstance(policy_records, list):
        raise ValueError("policy_records must be a list")

    normalized_by_domain: dict[str, dict[str, Any]] = {}
    for index, record in enumerate(policy_records):
        if not isinstance(record, dict):
            raise ValueError(f"policy_records[{index}] must be a dict")
        for field_name in _REQUIRED_POLICY_FIELDS:
            if field_name not in record:
                raise ValueError(f"policy_records[{index}] missing required field: {field_name}")
        normalized_record = {
            "domain": _normalize_domain(
                record.get("domain"),
                field_name=f"policy_records[{index}].domain",
            ),
            "max_daily_sends": _normalize_positive_int(
                record.get("max_daily_sends"),
                field_name=f"policy_records[{index}].max_daily_sends",
            ),
            "max_per_window": _normalize_positive_int(
                record.get("max_per_window"),
                field_name=f"policy_records[{index}].max_per_window",
            ),
            "warmup_stage": _normalize_positive_int(
                record.get("warmup_stage"),
                field_name=f"policy_records[{index}].warmup_stage",
            ),
            "reputation_score": _normalize_float(
                record.get("reputation_score"),
                field_name=f"policy_records[{index}].reputation_score",
            ),
        }
        normalized_by_domain[normalized_record["domain"]] = normalized_record

    normalized_records = list(normalized_by_domain.values())
    normalized_records.sort(key=lambda row: str(row["domain"]))
    return deepcopy(normalized_records)


def resolve_domain_policy(domain: str, policy_records: list[object]) -> dict[str, Any] | None:
    normalized_domain = _normalize_domain(domain, field_name="domain")
    normalized_policy_records = normalize_domain_policies(policy_records)
    for record in normalized_policy_records:
        if str(record["domain"]) == normalized_domain:
            return deepcopy(record)
    return None


def evaluate_domain_policy(
    domain: str,
    policy_records: list[object],
    *,
    scheduled_send_at: object,
    send_window: str,
    send_counts_by_day: dict[str, int] | None = None,
    send_counts_by_window: dict[str, int] | None = None,
    stage_limits: dict[int, int] | None = None,
) -> dict[str, Any]:
    from fleetgraph.deliverability.rate_control import resolve_stage_max_per_window

    normalized_domain = _normalize_domain(domain, field_name="domain")
    normalized_send_window = _normalize_non_empty_string(send_window, field_name="send_window")
    normalized_scheduled_send_at = _coerce_datetime(
        scheduled_send_at,
        field_name="scheduled_send_at",
    )
    policy_record = resolve_domain_policy(normalized_domain, policy_records)
    if policy_record is None:
        return {
            "domain": normalized_domain,
            "allow_send": False,
            "reason": "missing_domain_policy",
            "effective_max_per_window": 0,
            "max_daily_sends": 0,
            "daily_count": 0,
            "window_count": 0,
        }

    effective_stage_limit = resolve_stage_max_per_window(
        int(policy_record["warmup_stage"]),
        stage_limits=stage_limits,
    )
    effective_max_per_window = min(int(policy_record["max_per_window"]), effective_stage_limit)

    normalized_send_counts_by_day = dict(send_counts_by_day or {})
    normalized_send_counts_by_window = dict(send_counts_by_window or {})
    day_key = f"{normalized_domain}|{normalized_scheduled_send_at.date().isoformat()}"
    window_key = (
        f"{normalized_domain}|{normalized_send_window}|"
        f"{normalized_scheduled_send_at.isoformat(timespec='seconds')}"
    )
    daily_count = int(normalized_send_counts_by_day.get(day_key, 0))
    window_count = int(normalized_send_counts_by_window.get(window_key, 0))

    if daily_count >= int(policy_record["max_daily_sends"]):
        reason = "domain_daily_cap_reached"
        allow_send = False
    elif window_count >= effective_max_per_window:
        reason = "domain_window_cap_reached"
        allow_send = False
    else:
        reason = "domain_policy_clear"
        allow_send = True

    return {
        "domain": normalized_domain,
        "allow_send": allow_send,
        "reason": reason,
        "effective_max_per_window": effective_max_per_window,
        "max_daily_sends": int(policy_record["max_daily_sends"]),
        "daily_count": daily_count,
        "window_count": window_count,
    }
