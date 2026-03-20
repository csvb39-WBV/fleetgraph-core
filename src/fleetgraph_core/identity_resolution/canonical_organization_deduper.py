"""Deterministic canonical organization deduplication."""

from collections import defaultdict
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

UNIFIED_ORGANIZATION_FIELDS = (
    "unified_organization_id",
    "canonical_organization_ids",
    "canonical_organization_name",
    "canonical_organization_key",
    "domain_candidate",
    "source_ids",
    "candidate_state",
)


def validate_canonical_organizations(canonical_records: list[dict]) -> None:
    """Validate FG3-MB2 canonical organization records for deduplication."""
    if not isinstance(canonical_records, list):
        raise TypeError("canonical_records must be a list")

    seen_canonical_ids = set()

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

        canonical_id = record["canonical_organization_id"].strip()
        if canonical_id in seen_canonical_ids:
            raise ValueError(
                "duplicate canonical_organization_id detected: " + canonical_id
            )

        seen_canonical_ids.add(canonical_id)


def build_deduplication_key(canonical_record: dict) -> tuple:
    """Build the deduplication key for a canonical record."""
    validate_canonical_organizations([canonical_record])
    return (canonical_record["canonical_organization_key"],)


def merge_canonical_group(group: list[dict]) -> dict:
    """Merge a group of canonical records into a unified record."""
    if not group:
        raise ValueError("group cannot be empty")

    # Sort group by canonical_organization_id for stable order
    sorted_group = sorted(group, key=lambda r: r["canonical_organization_id"])

    canonical_organization_ids = sorted([r["canonical_organization_id"] for r in sorted_group])
    source_ids = sorted(set([r["source_id"] for r in sorted_group]))
    canonical_organization_name = sorted_group[0]["canonical_organization_name"]
    domain_candidate = sorted_group[0]["domain_candidate"]
    canonical_organization_key = sorted_group[0]["canonical_organization_key"]

    unified_organization_id = canonical_organization_key + "::unified"

    unified_record = {
        "unified_organization_id": unified_organization_id,
        "canonical_organization_ids": canonical_organization_ids,
        "canonical_organization_name": canonical_organization_name,
        "canonical_organization_key": canonical_organization_key,
        "domain_candidate": domain_candidate,
        "source_ids": source_ids,
        "candidate_state": "unified",
    }

    if tuple(unified_record.keys()) != UNIFIED_ORGANIZATION_FIELDS:
        raise ValueError(
            "unified organization fields must match the required contract exactly"
        )

    return deepcopy(unified_record)


def build_unified_identity(unified_record: dict) -> tuple:
    """Build deterministic identity tuple for unified records."""
    if not isinstance(unified_record, dict):
        raise TypeError("unified_record must be a dictionary")

    field_names = set(unified_record.keys())
    required_field_names = set(UNIFIED_ORGANIZATION_FIELDS)

    missing_fields = sorted(required_field_names - field_names)
    extra_fields = sorted(field_names - required_field_names)

    if missing_fields:
        raise ValueError(
            "unified record is missing required fields: " + ", ".join(missing_fields)
        )

    if extra_fields:
        raise ValueError(
            "unified record contains unknown fields: " + ", ".join(extra_fields)
        )

    return (
        unified_record["domain_candidate"],
        unified_record["canonical_organization_key"],
        unified_record["unified_organization_id"],
    )


def assemble_unified_canonical_organizations(canonical_records: list[dict]) -> list[dict]:
    """Assemble deterministic unified canonical organization records."""
    validate_canonical_organizations(canonical_records)

    groups = defaultdict(list)
    for record in deepcopy(canonical_records):
        key = build_deduplication_key(record)
        groups[key].append(record)

    unified_records = []
    seen_unified_ids = set()

    for group in groups.values():
        unified_record = merge_canonical_group(group)
        unified_id = unified_record["unified_organization_id"]
        if unified_id in seen_unified_ids:
            raise ValueError("duplicate unified_organization_id detected: " + unified_id)
        seen_unified_ids.add(unified_id)
        unified_records.append(unified_record)

    sorted_records = sorted(unified_records, key=build_unified_identity)
    return deepcopy(sorted_records)