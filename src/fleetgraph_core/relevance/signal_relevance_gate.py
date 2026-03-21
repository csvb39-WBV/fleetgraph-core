"""FG4-MB1 deterministic signal relevance gate."""

from copy import deepcopy
from typing import Any


FG3_CANONICAL_FIELDS = (
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
)

RELEVANCE_GATE_ALLOWED_OUTCOMES = ("relevant",)


def _validate_fg3_record(record: dict[str, Any]) -> None:
    field_names = set(record.keys())
    required_field_names = set(FG3_CANONICAL_FIELDS)

    missing_fields = sorted(required_field_names - field_names)
    extra_fields = sorted(field_names - required_field_names)

    if missing_fields:
        raise ValueError(
            "canonical record is missing required fields: " + ", ".join(missing_fields)
        )

    if extra_fields:
        raise ValueError(
            "canonical record contains unknown fields: " + ", ".join(extra_fields)
        )

    for field_name in FG3_CANONICAL_FIELDS:
        field_value = record[field_name]
        if not isinstance(field_value, str):
            raise TypeError(f"{field_name} must be a non-empty string")
        if field_value.strip() == "":
            raise ValueError(f"{field_name} must be a non-empty string")

    if record["candidate_state"] != "canonicalized":
        raise ValueError("candidate_state must be exactly 'canonicalized'")


def validate_fg3_canonical_records(records: list[dict[str, Any]]) -> None:
    """Validate exact FG3 canonical organization schema records."""
    if not isinstance(records, list):
        raise TypeError("records must be a list")

    for record in records:
        if not isinstance(record, dict):
            raise TypeError("each record must be a dictionary")
        _validate_fg3_record(record)


def _build_relevance_gate_outcome(record: dict[str, Any]) -> str:
    # Bounded FG4-MB1 rule: any valid canonicalized FG3 record is relevant.
    _validate_fg3_record(record)
    return RELEVANCE_GATE_ALLOWED_OUTCOMES[0]


def apply_signal_relevance_gate(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Apply deterministic FG4-MB1 relevance gate in stable input order."""
    validate_fg3_canonical_records(records)

    output_records: list[dict[str, Any]] = []
    for record in records:
        output_record = deepcopy(record)
        output_record["relevance_gate_outcome"] = _build_relevance_gate_outcome(record)
        output_records.append(output_record)

    return output_records
