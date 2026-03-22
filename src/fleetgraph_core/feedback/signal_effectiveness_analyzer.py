"""Deterministic signal effectiveness analysis from tracked outcomes."""

from __future__ import annotations

from collections.abc import Mapping


_VALID_PRIORITIES = {"HIGH", "MEDIUM", "LOW"}
_VALID_TIMINGS = {"IMMEDIATE", "7_DAYS", "30_DAYS"}
_VALID_OUTCOME_STATUSES = {"WON", "LOST", "NO_RESPONSE", "PENDING"}

_EFFECTIVENESS_CONTRIBUTIONS = {
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


def analyze_signal_effectiveness(payload: object) -> dict[str, object]:
    """Compute deterministic signal effectiveness metrics by ICP."""
    if not isinstance(payload, Mapping):
        raise ValueError("input must be a mapping")

    company_id = _validate_non_empty_string(payload.get("company_id"), "company_id")

    tracked_outcomes_in = payload.get("tracked_outcomes")
    if not isinstance(tracked_outcomes_in, list):
        raise ValueError("tracked_outcomes must be a list")

    icp_stats: dict[str, dict[str, object]] = {}
    total_records = 0
    total_effectiveness_score = 0.0

    for index, item in enumerate(tracked_outcomes_in):
        parsed = _validate_tracked_outcome(item, index, company_id)

        icp = _validate_non_empty_string(parsed["icp"], f"tracked_outcomes[{index}].icp")
        outcome_status = _validate_non_empty_string(
            parsed["outcome_status"],
            f"tracked_outcomes[{index}].outcome_status",
        ).upper()

        contribution = _EFFECTIVENESS_CONTRIBUTIONS[outcome_status]
        total_records += 1
        total_effectiveness_score += contribution

        if icp not in icp_stats:
            icp_stats[icp] = {
                "record_count": 0,
                "won_count": 0,
                "lost_count": 0,
                "no_response_count": 0,
                "pending_count": 0,
                "effectiveness_score": 0.0,
            }

        current = icp_stats[icp]
        current["record_count"] = int(current["record_count"]) + 1
        if outcome_status == "WON":
            current["won_count"] = int(current["won_count"]) + 1
        elif outcome_status == "LOST":
            current["lost_count"] = int(current["lost_count"]) + 1
        elif outcome_status == "NO_RESPONSE":
            current["no_response_count"] = int(current["no_response_count"]) + 1
        else:
            current["pending_count"] = int(current["pending_count"]) + 1
        current["effectiveness_score"] = float(current["effectiveness_score"]) + contribution

    ordered_icp_effectiveness = {
        icp: {
            "record_count": int(icp_stats[icp]["record_count"]),
            "won_count": int(icp_stats[icp]["won_count"]),
            "lost_count": int(icp_stats[icp]["lost_count"]),
            "no_response_count": int(icp_stats[icp]["no_response_count"]),
            "pending_count": int(icp_stats[icp]["pending_count"]),
            "effectiveness_score": float(icp_stats[icp]["effectiveness_score"]),
        }
        for icp in sorted(icp_stats.keys())
    }

    return {
        "company_id": company_id,
        "signal_effectiveness": {
            "total_records": total_records,
            "total_effectiveness_score": total_effectiveness_score,
            "icp_effectiveness": ordered_icp_effectiveness,
        },
    }


__all__ = ["analyze_signal_effectiveness"]