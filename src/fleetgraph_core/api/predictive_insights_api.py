"""Deterministic API-facing predictive insights formatter."""

from __future__ import annotations

from collections.abc import Mapping


_VALID_PRIORITIES = {"HIGH", "MEDIUM", "LOW"}
_VALID_TIMINGS = {"IMMEDIATE", "7_DAYS", "30_DAYS"}


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


def _validate_opportunity(item: object, index: int) -> Mapping[str, object]:
    if not isinstance(item, Mapping):
        raise ValueError(f"opportunities[{index}] must be a mapping")

    required_fields = ("icp", "opportunity_score", "reason")
    for field_name in required_fields:
        if field_name not in item:
            raise ValueError(f"opportunities[{index}] missing required field: {field_name}")

    _validate_non_empty_string(item.get("icp"), f"opportunities[{index}].icp")
    _validate_numeric(
        item.get("opportunity_score"),
        f"opportunities[{index}].opportunity_score",
    )
    _validate_non_empty_string(item.get("reason"), f"opportunities[{index}].reason")

    return item


def _validate_timed_prospect(
    item: object,
    index: int,
    company_id: str,
) -> Mapping[str, object]:
    if not isinstance(item, Mapping):
        raise ValueError(f"timed_prospects[{index}] must be a mapping")

    required_fields = (
        "company_id",
        "icp",
        "priority",
        "opportunity_score",
        "reason",
        "timing",
    )
    for field_name in required_fields:
        if field_name not in item:
            raise ValueError(f"timed_prospects[{index}] missing required field: {field_name}")

    prospect_company_id = _validate_non_empty_string(
        item.get("company_id"),
        f"timed_prospects[{index}].company_id",
    )
    if prospect_company_id != company_id:
        raise ValueError(f"timed_prospects[{index}].company_id must match top-level company_id")

    _validate_non_empty_string(item.get("icp"), f"timed_prospects[{index}].icp")

    priority = _validate_non_empty_string(
        item.get("priority"),
        f"timed_prospects[{index}].priority",
    ).upper()
    if priority not in _VALID_PRIORITIES:
        raise ValueError(f"timed_prospects[{index}].priority must be one of: HIGH, MEDIUM, LOW")

    timing = _validate_non_empty_string(
        item.get("timing"),
        f"timed_prospects[{index}].timing",
    ).upper()
    if timing not in _VALID_TIMINGS:
        raise ValueError(
            f"timed_prospects[{index}].timing must be one of: IMMEDIATE, 7_DAYS, 30_DAYS"
        )

    _validate_numeric(
        item.get("opportunity_score"),
        f"timed_prospects[{index}].opportunity_score",
    )
    _validate_non_empty_string(item.get("reason"), f"timed_prospects[{index}].reason")

    return item


def _compute_next_best_icp(opportunities: list[dict[str, object]]) -> str:
    if not opportunities:
        return "NONE"

    best_index = 0
    best_score = float(opportunities[0]["opportunity_score"])
    for index, item in enumerate(opportunities):
        score = float(item["opportunity_score"])
        if score > best_score:
            best_index = index
            best_score = score

    return str(opportunities[best_index]["icp"])


def build_predictive_insights(payload: object) -> dict[str, object]:
    """Build deterministic predictive insights response for one company."""
    if not isinstance(payload, Mapping):
        raise ValueError("input must be a mapping")

    company_id = _validate_non_empty_string(payload.get("company_id"), "company_id")

    opportunities_in = payload.get("opportunities")
    timed_prospects_in = payload.get("timed_prospects")

    if not isinstance(opportunities_in, list):
        raise ValueError("opportunities must be a list")
    if not isinstance(timed_prospects_in, list):
        raise ValueError("timed_prospects must be a list")

    opportunities: list[dict[str, object]] = []
    timed_prospects: list[dict[str, object]] = []

    for index, item in enumerate(opportunities_in):
        parsed = _validate_opportunity(item, index)
        opportunities.append(
            {
                "icp": _validate_non_empty_string(parsed["icp"], f"opportunities[{index}].icp"),
                "opportunity_score": _validate_numeric(
                    parsed["opportunity_score"],
                    f"opportunities[{index}].opportunity_score",
                ),
                "reason": _validate_non_empty_string(
                    parsed["reason"],
                    f"opportunities[{index}].reason",
                ),
            }
        )

    for index, item in enumerate(timed_prospects_in):
        parsed = _validate_timed_prospect(item, index, company_id)
        timed_prospects.append(
            {
                "company_id": _validate_non_empty_string(
                    parsed["company_id"],
                    f"timed_prospects[{index}].company_id",
                ),
                "icp": _validate_non_empty_string(parsed["icp"], f"timed_prospects[{index}].icp"),
                "priority": _validate_non_empty_string(
                    parsed["priority"],
                    f"timed_prospects[{index}].priority",
                ).upper(),
                "opportunity_score": _validate_numeric(
                    parsed["opportunity_score"],
                    f"timed_prospects[{index}].opportunity_score",
                ),
                "reason": _validate_non_empty_string(
                    parsed["reason"],
                    f"timed_prospects[{index}].reason",
                ),
                "timing": _validate_non_empty_string(
                    parsed["timing"],
                    f"timed_prospects[{index}].timing",
                ).upper(),
            }
        )

    highest_score = 0.0
    if opportunities:
        highest_score = max(float(item["opportunity_score"]) for item in opportunities)

    immediate_action_count = sum(
        1
        for item in timed_prospects
        if str(item["timing"]) == "IMMEDIATE"
    )

    return {
        "company_id": company_id,
        "predictive_insights": {
            "opportunity_count": len(opportunities),
            "timed_prospect_count": len(timed_prospects),
            "highest_opportunity_score": highest_score,
            "immediate_action_count": immediate_action_count,
            "next_best_icp": _compute_next_best_icp(opportunities),
        },
        "opportunities": opportunities,
        "timed_prospects": timed_prospects,
    }


__all__ = ["build_predictive_insights"]