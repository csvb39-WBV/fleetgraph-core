from __future__ import annotations

from copy import deepcopy
from typing import Any

__all__ = [
    "evaluate_bounce_protection",
    "resolve_stage_max_per_window",
    "resolve_warmup_stage",
]

_DEFAULT_STAGE_LIMITS = {
    1: 10,
    2: 20,
    3: 30,
    4: 50,
}


def _normalize_non_empty_string(value: object, *, field_name: str) -> str:
    if not isinstance(value, str):
        raise ValueError(f"{field_name} must be a non-empty string")
    normalized_value = value.strip().lower()
    if normalized_value == "":
        raise ValueError(f"{field_name} must be a non-empty string")
    return normalized_value


def _normalize_non_negative_int(value: object, *, field_name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"{field_name} must be an int")
    if value < 0:
        raise ValueError(f"{field_name} must be greater than or equal to 0")
    return value


def _normalize_stage_limits(stage_limits: dict[int, int] | None) -> dict[int, int]:
    normalized_stage_limits = dict(_DEFAULT_STAGE_LIMITS)
    if stage_limits is None:
        return normalized_stage_limits
    for stage, limit in stage_limits.items():
        normalized_stage = _normalize_non_negative_int(stage, field_name="stage_limits.stage")
        if normalized_stage <= 0:
            raise ValueError("stage_limits.stage must be greater than 0")
        normalized_stage_limits[normalized_stage] = _normalize_non_negative_int(
            limit,
            field_name=f"stage_limits[{normalized_stage}]",
        )
    return normalized_stage_limits


def resolve_stage_max_per_window(warmup_stage: int, *, stage_limits: dict[int, int] | None = None) -> int:
    normalized_stage_limits = _normalize_stage_limits(stage_limits)
    normalized_stage = _normalize_non_negative_int(warmup_stage, field_name="warmup_stage")
    if normalized_stage <= 0:
        raise ValueError("warmup_stage must be greater than 0")
    max_defined_stage = max(normalized_stage_limits)
    resolved_stage = min(normalized_stage, max_defined_stage)
    return int(normalized_stage_limits[resolved_stage])


def resolve_warmup_stage(warmup_stage: int, *, stage_limits: dict[int, int] | None = None) -> dict[str, Any]:
    resolved_max = resolve_stage_max_per_window(warmup_stage, stage_limits=stage_limits)
    normalized_stage_limits = _normalize_stage_limits(stage_limits)
    max_defined_stage = max(normalized_stage_limits)
    resolved_stage = min(_normalize_non_negative_int(warmup_stage, field_name="warmup_stage"), max_defined_stage)
    if resolved_stage <= 0:
        raise ValueError("warmup_stage must be greater than 0")
    return {
        "warmup_stage": resolved_stage,
        "max_per_window": resolved_max,
    }


def evaluate_bounce_protection(
    domain: str,
    domain_metrics: list[object],
    *,
    bounce_threshold: float = 0.05,
) -> dict[str, Any]:
    if isinstance(bounce_threshold, bool) or not isinstance(bounce_threshold, (int, float)):
        raise ValueError("bounce_threshold must be numeric")
    numeric_threshold = float(bounce_threshold)
    if numeric_threshold < 0.0:
        raise ValueError("bounce_threshold must be greater than or equal to 0")

    normalized_domain = _normalize_non_empty_string(domain, field_name="domain")
    sent = 0
    bounced = 0
    for index, metric in enumerate(domain_metrics):
        if not isinstance(metric, dict):
            raise ValueError(f"domain_metrics[{index}] must be a dict")
        metric_domain = _normalize_non_empty_string(
            metric.get("domain"),
            field_name=f"domain_metrics[{index}].domain",
        )
        if metric_domain != normalized_domain:
            continue
        sent = _normalize_non_negative_int(metric.get("sent"), field_name=f"domain_metrics[{index}].sent")
        bounced = _normalize_non_negative_int(metric.get("bounced"), field_name=f"domain_metrics[{index}].bounced")
        break

    if sent == 0:
        bounce_rate = 0.0
    else:
        bounce_rate = bounced / sent

    if bounce_rate > numeric_threshold:
        status = "DEGRADED"
        allow_send = False
        reason = "bounce_threshold_exceeded"
    else:
        status = "HEALTHY"
        allow_send = True
        reason = "bounce_rate_within_threshold"

    return deepcopy(
        {
            "domain": normalized_domain,
            "sent": sent,
            "bounced": bounced,
            "bounce_rate": bounce_rate,
            "status": status,
            "allow_send": allow_send,
            "reason": reason,
        }
    )
