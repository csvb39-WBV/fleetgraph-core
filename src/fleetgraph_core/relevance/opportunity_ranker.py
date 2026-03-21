"""FG4-MB2 deterministic opportunity ranker."""

from copy import deepcopy
from typing import Any


FG4_MB1_FIELDS = (
    "canonical_organization_id",
    "organization_domain_candidate_id",
    "organization_candidate_id",
    "candidate_id",
    "seed_id",
    "source_id",
    "source_label",
    "canonical_organization_name",
    "canonical_organization_key",
    "domain_candidate",
    "candidate_state",
    "relevance_gate_outcome",
)

OPPORTUNITY_RANK_FIELD = "opportunity_rank"
RELEVANCE_ALLOWED_OUTCOMES = ("relevant",)


def _validate_fg4_mb1_record(record: dict[str, Any]) -> None:
    field_names = set(record.keys())
    required_field_names = set(FG4_MB1_FIELDS)

    missing_fields = sorted(required_field_names - field_names)
    extra_fields = sorted(field_names - required_field_names)

    if missing_fields:
        raise ValueError(
            "record is missing required fields: " + ", ".join(missing_fields)
        )

    if extra_fields:
        raise ValueError(
            "record contains unknown fields: " + ", ".join(extra_fields)
        )

    for field_name in FG4_MB1_FIELDS:
        field_value = record[field_name]
        if not isinstance(field_value, str):
            raise TypeError(f"{field_name} must be a non-empty string")
        if field_value.strip() == "":
            raise ValueError(f"{field_name} must be a non-empty string")

    if record["candidate_state"] != "canonicalized":
        raise ValueError("candidate_state must be exactly 'canonicalized'")

    if record["relevance_gate_outcome"] not in RELEVANCE_ALLOWED_OUTCOMES:
        raise ValueError("relevance_gate_outcome must be exactly 'relevant'")


def validate_fg4_mb1_records(records: list[dict[str, Any]]) -> None:
    """Validate exact FG4-MB1 records for ranking input."""
    if not isinstance(records, list):
        raise TypeError("records must be a list")

    for record in records:
        if not isinstance(record, dict):
            raise TypeError("each record must be a dictionary")
        _validate_fg4_mb1_record(record)


def _build_rank_from_position(position_index: int) -> int:
    if not isinstance(position_index, int):
        raise TypeError("position_index must be an integer")
    if position_index < 0:
        raise ValueError("position_index must be non-negative")
    return position_index + 1


def apply_opportunity_ranker(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Apply deterministic 1-based ranking in stable input order."""
    validate_fg4_mb1_records(records)

    output_records: list[dict[str, Any]] = []

    for index, record in enumerate(records):
        output_record = deepcopy(record)
        output_record[OPPORTUNITY_RANK_FIELD] = _build_rank_from_position(index)
        output_records.append(output_record)

    return output_records
