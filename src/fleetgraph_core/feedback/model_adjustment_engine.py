"""Deterministic feedback-based model adjustment aggregation."""

from __future__ import annotations

from collections.abc import Mapping


_VALID_PRIORITIES = {"HIGH", "MEDIUM", "LOW"}
_VALID_TIMINGS = {"IMMEDIATE", "7_DAYS", "30_DAYS"}
_VALID_OUTCOME_STATUSES = {"WON", "LOST", "NO_RESPONSE", "PENDING"}

_ADJUSTMENT_BY_OUTCOME = {
    "WON": 1.0,
    "LOST": -1.0,
    "NO_RESPONSE": -0.5,
    "PENDING": 0.0,
}


def _validate_non_empty_string(value: object, field_name: str) -> str:
    if not isinstance(value, str):
        raise ValueError(f"{field_name} must be a non-empty string")

    normalized = value.strip()
    if not normalized:
        raise ValueError(f"{field_name} must be a non-empty string")

    return normalized


def _validate_numeric(value: object, field_name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"{field_name} must be numeric")

    return float(value)


def _validate_tracked_outcome(
    item: object,
    index: int,
    company_id: str,
) -> Mapping[str, object]:
    if not isinstance(item, Mapping):
        raise ValueError(f"tracked_outcomes[{index}] must be a mapping")

    required_fields = (
        "company_id",
        "icp",
        "priority",
        "opportunity_score",
        "reason",
        "timing",
        "stage",
        "pipeline_status",
        "outcome_status",
        "notes",
    )
    for field_name in required_fields:
        if field_name not in item:
            raise ValueError(f"tracked_outcomes[{index}] missing required field: {field_name}")

    tracked_company_id = _validate_non_empty_string(
        item.get("company_id"),
        f"tracked_outcomes[{index}].company_id",
    )
    if tracked_company_id != company_id:
        raise ValueError(f"tracked_outcomes[{index}].company_id must match top-level company_id")

    _validate_non_empty_string(item.get("icp"), f"tracked_outcomes[{index}].icp")

    priority = _validate_non_empty_string(
        item.get("priority"),
        f"tracked_outcomes[{index}].priority",
    ).upper()
    if priority not in _VALID_PRIORITIES:
        raise ValueError(f"tracked_outcomes[{index}].priority must be one of: HIGH, MEDIUM, LOW")

    _validate_numeric(
        item.get("opportunity_score"),
        f"tracked_outcomes[{index}].opportunity_score",
    )
    _validate_non_empty_string(item.get("reason"), f"tracked_outcomes[{index}].reason")

    timing = _validate_non_empty_string(
        item.get("timing"),
        f"tracked_outcomes[{index}].timing",
    ).upper()
    if timing not in _VALID_TIMINGS:
        raise ValueError(
            f"tracked_outcomes[{index}].timing must be one of: IMMEDIATE, 7_DAYS, 30_DAYS"
        )

    stage = _validate_non_empty_string(item.get("stage"), f"tracked_outcomes[{index}].stage")
    if stage != "READY":
        raise ValueError(f"tracked_outcomes[{index}].stage must be READY")

    pipeline_status = _validate_non_empty_string(
        item.get("pipeline_status"),
        f"tracked_outcomes[{index}].pipeline_status",
    )
    if pipeline_status != "OPEN":
        raise ValueError(f"tracked_outcomes[{index}].pipeline_status must be OPEN")

    outcome_status = _validate_non_empty_string(
        item.get("outcome_status"),
        f"tracked_outcomes[{index}].outcome_status",
    ).upper()
    if outcome_status not in _VALID_OUTCOME_STATUSES:
        raise ValueError(
            "tracked_outcomes"
            f"[{index}].outcome_status must be one of: LOST, NO_RESPONSE, PENDING, WON"
        )

    notes = item.get("notes")
    if not isinstance(notes, str):
        raise ValueError(f"tracked_outcomes[{index}].notes must be a string")

    return item


def build_model_adjustments(payload: object) -> dict[str, object]:
    """Build deterministic ICP adjustment map from tracked outcomes."""
    if not isinstance(payload, Mapping):
        raise ValueError("input must be a mapping")

    company_id = _validate_non_empty_string(payload.get("company_id"), "company_id")

    tracked_outcomes_in = payload.get("tracked_outcomes")
    if not isinstance(tracked_outcomes_in, list):
        raise ValueError("tracked_outcomes must be a list")

    icp_adjustments: dict[str, float] = {}
    total_adjustments = 0.0

    for index, item in enumerate(tracked_outcomes_in):
        parsed = _validate_tracked_outcome(item, index, company_id)
        icp = _validate_non_empty_string(parsed["icp"], f"tracked_outcomes[{index}].icp")
        outcome_status = _validate_non_empty_string(
            parsed["outcome_status"],
            f"tracked_outcomes[{index}].outcome_status",
        ).upper()

        adjustment = _ADJUSTMENT_BY_OUTCOME[outcome_status]
        icp_adjustments[icp] = icp_adjustments.get(icp, 0.0) + adjustment
        total_adjustments += adjustment

    ordered_icp_adjustments = {
        icp: icp_adjustments[icp]
        for icp in sorted(icp_adjustments.keys())
    }

    return {
        "company_id": company_id,
        "model_adjustments": {
            "icp_adjustments": ordered_icp_adjustments,
            "total_adjustments": total_adjustments,
        },
    }


__all__ = ["build_model_adjustments"]