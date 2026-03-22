"""Deterministic API-facing RFP panel formatter."""

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

    required = ("icp", "opportunity_score", "reason")
    for field_name in required:
        if field_name not in item:
            raise ValueError(f"opportunities[{index}] missing required field: {field_name}")

    _validate_non_empty_string(item.get("icp"), f"opportunities[{index}].icp")
    _validate_numeric(item.get("opportunity_score"), f"opportunities[{index}].opportunity_score")
    _validate_non_empty_string(item.get("reason"), f"opportunities[{index}].reason")

    return item


def _validate_pipeline_record(
    item: object,
    index: int,
    company_id: str,
) -> Mapping[str, object]:
    if not isinstance(item, Mapping):
        raise ValueError(f"pipeline_records[{index}] must be a mapping")

    required = (
        "company_id",
        "icp",
        "priority",
        "opportunity_score",
        "reason",
        "timing",
        "stage",
        "pipeline_status",
    )
    for field_name in required:
        if field_name not in item:
            raise ValueError(f"pipeline_records[{index}] missing required field: {field_name}")

    record_company_id = _validate_non_empty_string(
        item.get("company_id"),
        f"pipeline_records[{index}].company_id",
    )
    if record_company_id != company_id:
        raise ValueError(f"pipeline_records[{index}].company_id must match top-level company_id")

    _validate_non_empty_string(item.get("icp"), f"pipeline_records[{index}].icp")

    priority = _validate_non_empty_string(
        item.get("priority"),
        f"pipeline_records[{index}].priority",
    ).upper()
    if priority not in _VALID_PRIORITIES:
        raise ValueError(f"pipeline_records[{index}].priority must be one of: HIGH, MEDIUM, LOW")

    timing = _validate_non_empty_string(
        item.get("timing"),
        f"pipeline_records[{index}].timing",
    ).upper()
    if timing not in _VALID_TIMINGS:
        raise ValueError(
            f"pipeline_records[{index}].timing must be one of: IMMEDIATE, 7_DAYS, 30_DAYS"
        )

    stage = _validate_non_empty_string(item.get("stage"), f"pipeline_records[{index}].stage")
    if stage != "READY":
        raise ValueError(f"pipeline_records[{index}].stage must be READY")

    pipeline_status = _validate_non_empty_string(
        item.get("pipeline_status"),
        f"pipeline_records[{index}].pipeline_status",
    )
    if pipeline_status != "OPEN":
        raise ValueError(f"pipeline_records[{index}].pipeline_status must be OPEN")

    _validate_numeric(
        item.get("opportunity_score"),
        f"pipeline_records[{index}].opportunity_score",
    )
    _validate_non_empty_string(item.get("reason"), f"pipeline_records[{index}].reason")

    return item


def _compute_top_opportunity_icp(opportunities: list[dict[str, object]]) -> str:
    if not opportunities:
        return "NONE"

    best_index = 0
    best_score = float(opportunities[0]["opportunity_score"])

    for index, item in enumerate(opportunities):
        score = float(item["opportunity_score"])
        if score > best_score:
            best_score = score
            best_index = index

    return str(opportunities[best_index]["icp"])


def build_rfp_panel(payload: object) -> dict[str, object]:
    """Build deterministic RFP panel response for one company."""
    if not isinstance(payload, Mapping):
        raise ValueError("input must be a mapping")

    company_id = _validate_non_empty_string(payload.get("company_id"), "company_id")

    opportunities_in = payload.get("opportunities")
    pipeline_records_in = payload.get("pipeline_records")

    if not isinstance(opportunities_in, list):
        raise ValueError("opportunities must be a list")
    if not isinstance(pipeline_records_in, list):
        raise ValueError("pipeline_records must be a list")

    opportunities: list[dict[str, object]] = []
    pipeline_records: list[dict[str, object]] = []

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

    for index, item in enumerate(pipeline_records_in):
        parsed = _validate_pipeline_record(item, index, company_id)
        pipeline_records.append(
            {
                "company_id": _validate_non_empty_string(
                    parsed["company_id"],
                    f"pipeline_records[{index}].company_id",
                ),
                "icp": _validate_non_empty_string(parsed["icp"], f"pipeline_records[{index}].icp"),
                "priority": _validate_non_empty_string(
                    parsed["priority"],
                    f"pipeline_records[{index}].priority",
                ).upper(),
                "opportunity_score": _validate_numeric(
                    parsed["opportunity_score"],
                    f"pipeline_records[{index}].opportunity_score",
                ),
                "reason": _validate_non_empty_string(
                    parsed["reason"],
                    f"pipeline_records[{index}].reason",
                ),
                "timing": _validate_non_empty_string(
                    parsed["timing"],
                    f"pipeline_records[{index}].timing",
                ).upper(),
                "stage": _validate_non_empty_string(
                    parsed["stage"],
                    f"pipeline_records[{index}].stage",
                ),
                "pipeline_status": _validate_non_empty_string(
                    parsed["pipeline_status"],
                    f"pipeline_records[{index}].pipeline_status",
                ),
            }
        )

    high_priority_pipeline_count = sum(
        1 for item in pipeline_records if str(item["priority"]) == "HIGH"
    )

    return {
        "company_id": company_id,
        "rfp_panel": {
            "opportunity_count": len(opportunities),
            "pipeline_record_count": len(pipeline_records),
            "high_priority_pipeline_count": high_priority_pipeline_count,
            "top_opportunity_icp": _compute_top_opportunity_icp(opportunities),
        },
        "opportunities": opportunities,
        "pipeline_records": pipeline_records,
    }


__all__ = ["build_rfp_panel"]