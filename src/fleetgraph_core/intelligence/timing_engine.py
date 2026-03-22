"""Deterministic timing assignment for validated prospect records."""

from __future__ import annotations

from collections.abc import Mapping


_VALID_PRIORITIES = {"HIGH", "MEDIUM", "LOW"}
_TIMING_BY_PRIORITY = {
    "HIGH": "IMMEDIATE",
    "MEDIUM": "7_DAYS",
    "LOW": "30_DAYS",
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


def _validate_prospect(
    prospect: object,
    *,
    index: int,
    company_id: str,
) -> Mapping[str, object]:
    if not isinstance(prospect, Mapping):
        raise ValueError(f"prospects[{index}] must be a mapping")

    required_fields = ("company_id", "icp", "priority", "opportunity_score", "reason")
    for field_name in required_fields:
        if field_name not in prospect:
            raise ValueError(f"prospects[{index}] missing required field: {field_name}")

    prospect_company_id = _validate_non_empty_string(
        prospect.get("company_id"),
        f"prospects[{index}].company_id",
    )
    if prospect_company_id != company_id:
        raise ValueError(f"prospects[{index}].company_id must match top-level company_id")

    _validate_non_empty_string(prospect.get("icp"), f"prospects[{index}].icp")

    priority = _validate_non_empty_string(
        prospect.get("priority"),
        f"prospects[{index}].priority",
    ).upper()
    if priority not in _VALID_PRIORITIES:
        raise ValueError(f"prospects[{index}].priority must be one of: HIGH, MEDIUM, LOW")

    _validate_numeric(
        prospect.get("opportunity_score"),
        f"prospects[{index}].opportunity_score",
    )
    _validate_non_empty_string(prospect.get("reason"), f"prospects[{index}].reason")

    return prospect


def assign_timing(payload: object) -> dict[str, object]:
    """Append deterministic timing windows to each validated prospect."""
    if not isinstance(payload, Mapping):
        raise ValueError("input must be a mapping")

    company_id = _validate_non_empty_string(payload.get("company_id"), "company_id")

    prospects = payload.get("prospects")
    if not isinstance(prospects, list):
        raise ValueError("prospects must be a list")

    timed_prospects: list[dict[str, object]] = []
    for index, prospect in enumerate(prospects):
        parsed = _validate_prospect(prospect, index=index, company_id=company_id)

        priority = _validate_non_empty_string(
            parsed["priority"],
            f"prospects[{index}].priority",
        ).upper()
        timed_prospects.append(
            {
                "company_id": _validate_non_empty_string(
                    parsed["company_id"],
                    f"prospects[{index}].company_id",
                ),
                "icp": _validate_non_empty_string(parsed["icp"], f"prospects[{index}].icp"),
                "priority": priority,
                "opportunity_score": _validate_numeric(
                    parsed["opportunity_score"],
                    f"prospects[{index}].opportunity_score",
                ),
                "reason": _validate_non_empty_string(
                    parsed["reason"],
                    f"prospects[{index}].reason",
                ),
                "timing": _TIMING_BY_PRIORITY[priority],
            }
        )

    return {
        "company_id": company_id,
        "timed_prospects": timed_prospects,
    }


__all__ = ["assign_timing"]