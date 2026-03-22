"""Deterministic API-facing company intelligence aggregator."""

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


def _validate_prospect(item: object, index: int, company_id: str) -> Mapping[str, object]:
    if not isinstance(item, Mapping):
        raise ValueError(f"prospects[{index}] must be a mapping")

    required = ("company_id", "icp", "priority", "opportunity_score", "reason")
    for field_name in required:
        if field_name not in item:
            raise ValueError(f"prospects[{index}] missing required field: {field_name}")

    prospect_company_id = _validate_non_empty_string(
        item.get("company_id"),
        f"prospects[{index}].company_id",
    )
    if prospect_company_id != company_id:
        raise ValueError(f"prospects[{index}].company_id must match top-level company_id")

    _validate_non_empty_string(item.get("icp"), f"prospects[{index}].icp")

    priority = _validate_non_empty_string(
        item.get("priority"),
        f"prospects[{index}].priority",
    ).upper()
    if priority not in _VALID_PRIORITIES:
        raise ValueError(f"prospects[{index}].priority must be one of: HIGH, MEDIUM, LOW")

    _validate_numeric(item.get("opportunity_score"), f"prospects[{index}].opportunity_score")
    _validate_non_empty_string(item.get("reason"), f"prospects[{index}].reason")

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

    _validate_numeric(
        item.get("opportunity_score"),
        f"pipeline_records[{index}].opportunity_score",
    )
    _validate_non_empty_string(item.get("reason"), f"pipeline_records[{index}].reason")

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

    return item


def _determine_highest_priority(
    prospects: list[dict[str, object]],
    pipeline_records: list[dict[str, object]],
) -> str:
    priorities = [
        str(item["priority"]).upper()
        for item in prospects + pipeline_records
    ]
    if "HIGH" in priorities:
        return "HIGH"
    if "MEDIUM" in priorities:
        return "MEDIUM"
    if "LOW" in priorities:
        return "LOW"
    return "NONE"


def build_company_intelligence(payload: object) -> dict[str, object]:
    """Build deterministic company intelligence API response."""
    if not isinstance(payload, Mapping):
        raise ValueError("input must be a mapping")

    company_id = _validate_non_empty_string(payload.get("company_id"), "company_id")

    opportunities_in = payload.get("opportunities")
    prospects_in = payload.get("prospects")
    pipeline_records_in = payload.get("pipeline_records")

    if not isinstance(opportunities_in, list):
        raise ValueError("opportunities must be a list")
    if not isinstance(prospects_in, list):
        raise ValueError("prospects must be a list")
    if not isinstance(pipeline_records_in, list):
        raise ValueError("pipeline_records must be a list")

    opportunities: list[dict[str, object]] = []
    prospects: list[dict[str, object]] = []
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

    for index, item in enumerate(prospects_in):
        parsed = _validate_prospect(item, index, company_id)
        prospects.append(
            {
                "company_id": _validate_non_empty_string(
                    parsed["company_id"],
                    f"prospects[{index}].company_id",
                ),
                "icp": _validate_non_empty_string(parsed["icp"], f"prospects[{index}].icp"),
                "priority": _validate_non_empty_string(
                    parsed["priority"],
                    f"prospects[{index}].priority",
                ).upper(),
                "opportunity_score": _validate_numeric(
                    parsed["opportunity_score"],
                    f"prospects[{index}].opportunity_score",
                ),
                "reason": _validate_non_empty_string(
                    parsed["reason"],
                    f"prospects[{index}].reason",
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

    highest_score = 0.0
    if opportunities:
        highest_score = max(float(item["opportunity_score"]) for item in opportunities)

    return {
        "company_id": company_id,
        "company_intelligence": {
            "opportunity_count": len(opportunities),
            "prospect_count": len(prospects),
            "pipeline_record_count": len(pipeline_records),
            "highest_opportunity_score": highest_score,
            "highest_priority": _determine_highest_priority(prospects, pipeline_records),
        },
        "opportunities": opportunities,
        "prospects": prospects,
        "pipeline_records": pipeline_records,
    }


__all__ = ["build_company_intelligence"]