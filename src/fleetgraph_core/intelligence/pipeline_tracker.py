"""Deterministic pipeline record generation from timed prospects."""

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


def _validate_timed_prospect(
    timed_prospect: object,
    *,
    index: int,
    company_id: str,
) -> Mapping[str, object]:
    if not isinstance(timed_prospect, Mapping):
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
        if field_name not in timed_prospect:
            raise ValueError(f"timed_prospects[{index}] missing required field: {field_name}")

    timed_company_id = _validate_non_empty_string(
        timed_prospect.get("company_id"),
        f"timed_prospects[{index}].company_id",
    )
    if timed_company_id != company_id:
        raise ValueError(f"timed_prospects[{index}].company_id must match top-level company_id")

    _validate_non_empty_string(timed_prospect.get("icp"), f"timed_prospects[{index}].icp")

    priority = _validate_non_empty_string(
        timed_prospect.get("priority"),
        f"timed_prospects[{index}].priority",
    ).upper()
    if priority not in _VALID_PRIORITIES:
        raise ValueError(f"timed_prospects[{index}].priority must be one of: HIGH, MEDIUM, LOW")

    _validate_numeric(
        timed_prospect.get("opportunity_score"),
        f"timed_prospects[{index}].opportunity_score",
    )
    _validate_non_empty_string(
        timed_prospect.get("reason"),
        f"timed_prospects[{index}].reason",
    )

    timing = _validate_non_empty_string(
        timed_prospect.get("timing"),
        f"timed_prospects[{index}].timing",
    ).upper()
    if timing not in _VALID_TIMINGS:
        raise ValueError(f"timed_prospects[{index}].timing must be one of: IMMEDIATE, 7_DAYS, 30_DAYS")

    return timed_prospect


def build_pipeline_records(payload: object) -> dict[str, object]:
    """Convert timed prospects into deterministic pipeline records."""
    if not isinstance(payload, Mapping):
        raise ValueError("input must be a mapping")

    company_id = _validate_non_empty_string(payload.get("company_id"), "company_id")

    timed_prospects = payload.get("timed_prospects")
    if not isinstance(timed_prospects, list):
        raise ValueError("timed_prospects must be a list")

    pipeline_records: list[dict[str, object]] = []
    for index, timed_prospect in enumerate(timed_prospects):
        parsed = _validate_timed_prospect(timed_prospect, index=index, company_id=company_id)

        pipeline_records.append(
            {
                "company_id": _validate_non_empty_string(
                    parsed["company_id"],
                    f"timed_prospects[{index}].company_id",
                ),
                "icp": _validate_non_empty_string(
                    parsed["icp"],
                    f"timed_prospects[{index}].icp",
                ),
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
                "stage": "READY",
                "pipeline_status": "OPEN",
            }
        )

    return {
        "company_id": company_id,
        "pipeline_records": pipeline_records,
    }


__all__ = ["build_pipeline_records"]