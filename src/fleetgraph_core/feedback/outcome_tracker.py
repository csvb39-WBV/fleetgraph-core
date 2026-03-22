"""Deterministic outcome tracking for pipeline records."""

from __future__ import annotations

from collections.abc import Mapping


_VALID_PRIORITIES = {"HIGH", "MEDIUM", "LOW"}
_VALID_TIMINGS = {"IMMEDIATE", "7_DAYS", "30_DAYS"}
_VALID_OUTCOME_STATUSES = {"WON", "LOST", "NO_RESPONSE"}


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
    item: object,
    index: int,
    company_id: str,
) -> Mapping[str, object]:
    if not isinstance(item, Mapping):
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


def _validate_outcome(item: object, index: int) -> Mapping[str, object]:
    if not isinstance(item, Mapping):
        raise ValueError(f"outcomes[{index}] must be a mapping")

    required_fields = ("icp", "outcome_status", "notes")
    for field_name in required_fields:
        if field_name not in item:
            raise ValueError(f"outcomes[{index}] missing required field: {field_name}")

    _validate_non_empty_string(item.get("icp"), f"outcomes[{index}].icp")

    outcome_status = _validate_non_empty_string(
        item.get("outcome_status"),
        f"outcomes[{index}].outcome_status",
    ).upper()
    if outcome_status not in _VALID_OUTCOME_STATUSES:
        raise ValueError(
            f"outcomes[{index}].outcome_status must be one of: LOST, NO_RESPONSE, WON"
        )

    notes = item.get("notes")
    if not isinstance(notes, str):
        raise ValueError(f"outcomes[{index}].notes must be a string")

    return item


def track_outcomes(payload: object) -> dict[str, object]:
    """Attach deterministic outcome state to pipeline records by ICP."""
    if not isinstance(payload, Mapping):
        raise ValueError("input must be a mapping")

    company_id = _validate_non_empty_string(payload.get("company_id"), "company_id")

    pipeline_records_in = payload.get("pipeline_records")
    outcomes_in = payload.get("outcomes")

    if not isinstance(pipeline_records_in, list):
        raise ValueError("pipeline_records must be a list")
    if not isinstance(outcomes_in, list):
        raise ValueError("outcomes must be a list")

    pipeline_records: list[dict[str, object]] = []
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

    outcomes: list[dict[str, object]] = []
    for index, item in enumerate(outcomes_in):
        parsed = _validate_outcome(item, index)
        outcomes.append(
            {
                "icp": _validate_non_empty_string(parsed["icp"], f"outcomes[{index}].icp"),
                "outcome_status": _validate_non_empty_string(
                    parsed["outcome_status"],
                    f"outcomes[{index}].outcome_status",
                ).upper(),
                "notes": parsed["notes"],
            }
        )

    tracked_outcomes = [
        {
            **record,
            "outcome_status": "PENDING",
            "notes": "",
        }
        for record in pipeline_records
    ]

    consumed_record_indexes: set[int] = set()
    for outcome_index, outcome in enumerate(outcomes):
        outcome_icp = str(outcome["icp"])
        matched_index: int | None = None

        for record_index, record in enumerate(tracked_outcomes):
            if record_index in consumed_record_indexes:
                continue
            if str(record["icp"]) == outcome_icp:
                matched_index = record_index
                break

        if matched_index is None:
            raise ValueError(f"outcomes[{outcome_index}] has no matching pipeline record for icp: {outcome_icp}")

        tracked_outcomes[matched_index]["outcome_status"] = outcome["outcome_status"]
        tracked_outcomes[matched_index]["notes"] = outcome["notes"]
        consumed_record_indexes.add(matched_index)

    return {
        "company_id": company_id,
        "tracked_outcomes": tracked_outcomes,
    }


__all__ = ["track_outcomes"]