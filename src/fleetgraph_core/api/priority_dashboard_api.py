"""Deterministic API-facing formatter for priority dashboard records."""

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


def _validate_pipeline_record(
    record: object,
    *,
    index: int,
    company_id: str,
) -> Mapping[str, object]:
    if not isinstance(record, Mapping):
        raise ValueError(f"pipeline_records[{index}] must be a mapping")

    required_fields = (
        "company_id",
        "icp",
        "priority",
        "opportunity_score",
        "reason",
        "timing",
        "stage",
        "pipeline_status",
    )
    for field_name in required_fields:
        if field_name not in record:
            raise ValueError(f"pipeline_records[{index}] missing required field: {field_name}")

    record_company_id = _validate_non_empty_string(
        record.get("company_id"),
        f"pipeline_records[{index}].company_id",
    )
    if record_company_id != company_id:
        raise ValueError(f"pipeline_records[{index}].company_id must match top-level company_id")

    _validate_non_empty_string(record.get("icp"), f"pipeline_records[{index}].icp")

    priority = _validate_non_empty_string(
        record.get("priority"),
        f"pipeline_records[{index}].priority",
    ).upper()
    if priority not in _VALID_PRIORITIES:
        raise ValueError(f"pipeline_records[{index}].priority must be one of: HIGH, MEDIUM, LOW")

    _validate_numeric(
        record.get("opportunity_score"),
        f"pipeline_records[{index}].opportunity_score",
    )
    _validate_non_empty_string(record.get("reason"), f"pipeline_records[{index}].reason")

    timing = _validate_non_empty_string(
        record.get("timing"),
        f"pipeline_records[{index}].timing",
    ).upper()
    if timing not in _VALID_TIMINGS:
        raise ValueError(
            f"pipeline_records[{index}].timing must be one of: IMMEDIATE, 7_DAYS, 30_DAYS"
        )

    stage = _validate_non_empty_string(record.get("stage"), f"pipeline_records[{index}].stage")
    if stage != "READY":
        raise ValueError(f"pipeline_records[{index}].stage must be READY")

    pipeline_status = _validate_non_empty_string(
        record.get("pipeline_status"),
        f"pipeline_records[{index}].pipeline_status",
    )
    if pipeline_status != "OPEN":
        raise ValueError(f"pipeline_records[{index}].pipeline_status must be OPEN")

    return record


def build_priority_dashboard(payload: object) -> dict[str, object]:
    """Build deterministic dashboard response from pipeline records."""
    if not isinstance(payload, Mapping):
        raise ValueError("input must be a mapping")

    company_id = _validate_non_empty_string(payload.get("company_id"), "company_id")

    pipeline_records = payload.get("pipeline_records")
    if not isinstance(pipeline_records, list):
        raise ValueError("pipeline_records must be a list")

    records: list[dict[str, object]] = []
    high = 0
    medium = 0
    low = 0

    for index, record in enumerate(pipeline_records):
        parsed = _validate_pipeline_record(record, index=index, company_id=company_id)

        priority = _validate_non_empty_string(
            parsed["priority"],
            f"pipeline_records[{index}].priority",
        ).upper()
        if priority == "HIGH":
            high += 1
        elif priority == "MEDIUM":
            medium += 1
        else:
            low += 1

        records.append(
            {
                "company_id": _validate_non_empty_string(
                    parsed["company_id"],
                    f"pipeline_records[{index}].company_id",
                ),
                "icp": _validate_non_empty_string(parsed["icp"], f"pipeline_records[{index}].icp"),
                "priority": priority,
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

    return {
        "company_id": company_id,
        "dashboard_summary": {
            "total_records": len(records),
            "high_priority_count": high,
            "medium_priority_count": medium,
            "low_priority_count": low,
        },
        "records": records,
    }


__all__ = ["build_priority_dashboard"]