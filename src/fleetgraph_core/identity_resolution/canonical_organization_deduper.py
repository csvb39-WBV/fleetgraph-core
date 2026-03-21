"""Deterministic canonical organization duplicate suppression."""

from copy import deepcopy


CANONICAL_ORGANIZATION_FIELDS = (
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


def validate_canonical_organizations(canonical_records: list[dict]) -> None:
    """Validate FG3-MB1 canonical organization records for suppression input."""
    if not isinstance(canonical_records, list):
        raise TypeError("canonical_records must be a list")

    for record in canonical_records:
        if not isinstance(record, dict):
            raise TypeError("each canonical record must be a dictionary")

        field_names = set(record.keys())
        required_field_names = set(CANONICAL_ORGANIZATION_FIELDS)

        missing_fields = sorted(required_field_names - field_names)
        extra_fields = sorted(field_names - required_field_names)

        if missing_fields:
            raise ValueError(
                "canonical record is missing required fields: "
                + ", ".join(missing_fields)
            )

        if extra_fields:
            raise ValueError(
                "canonical record contains unknown fields: "
                + ", ".join(extra_fields)
            )

        for field_name in CANONICAL_ORGANIZATION_FIELDS:
            field_value = record[field_name]
            if not isinstance(field_value, str):
                raise TypeError(f"{field_name} must be a non-empty string")
            if field_value.strip() == "":
                raise ValueError(f"{field_name} must be a non-empty string")

        if record["candidate_state"].strip() != "canonicalized":
            raise ValueError("candidate_state must be exactly 'canonicalized'")


def build_deduplication_key(canonical_record: dict) -> tuple:
    """Build the deduplication key for a canonical record."""
    validate_canonical_organizations([canonical_record])
    return (canonical_record["canonical_organization_key"],)


def deduplicate_canonical_organizations(canonical_records: list[dict]) -> list[dict]:
    """Suppress duplicate canonical organizations by key with first-seen winner logic."""
    output_records = []
    seen_keys = set()

    for record in canonical_records:
        deduplication_key = build_deduplication_key(record)
        if deduplication_key in seen_keys:
            continue
        seen_keys.add(deduplication_key)
        output_records.append(deepcopy(record))

    return output_records


def suppress_duplicate_canonical_organizations(records: list[dict]) -> list[dict]:
    """Public entry point for deterministic canonical duplicate suppression."""
    validate_canonical_organizations(records)
    return deduplicate_canonical_organizations(records)