"""FG6-MB1 deterministic lead record assembler."""

from copy import deepcopy
from typing import Any


FG5_MB2_FIELDS = (
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
    "opportunity_rank",
    "contact_enrichment_request_id",
    "contact_enrichment_request",
    "contact_coordination_state",
    "matched_contact",
    "contact_assembly_state",
)

MATCHED_CONTACT_FIELDS = (
    "contact_name",
    "contact_title",
    "contact_email",
    "contact_source",
    "contact_match_state",
)

LEAD_RECORD_FIELDS = (
    "canonical_organization_id",
    "canonical_organization_key",
    "canonical_organization_name",
    "organization_domain_candidate_id",
    "organization_candidate_id",
    "candidate_id",
    "seed_id",
    "source_id",
    "source_label",
    "domain_candidate",
    "opportunity_rank",
    "contact_name",
    "contact_title",
    "contact_email",
    "contact_source",
    "contact_match_state",
)

LEAD_RECORD_STATE = "assembled"
ALLOWED_CONTACT_MATCH_STATES = ("matched", "unmatched")


def validate_fg5_mb2_records(records: list[dict[str, Any]]) -> None:
    """Validate exact FG5-MB2 records for lead assembly input."""
    if not isinstance(records, list):
        raise TypeError("records must be a list")

    for record in records:
        if not isinstance(record, dict):
            raise TypeError("each record must be a dictionary")

        field_names = set(record.keys())
        required_field_names = set(FG5_MB2_FIELDS)

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

        for field_name in FG5_MB2_FIELDS:
            if field_name in (
                "opportunity_rank",
                "contact_enrichment_request",
                "matched_contact",
            ):
                continue

            field_value = record[field_name]
            if not isinstance(field_value, str):
                raise TypeError(f"{field_name} must be a non-empty string")
            if field_value.strip() == "":
                raise ValueError(f"{field_name} must be a non-empty string")

        if record["candidate_state"] != "canonicalized":
            raise ValueError("candidate_state must be exactly 'canonicalized'")

        if record["relevance_gate_outcome"] != "relevant":
            raise ValueError("relevance_gate_outcome must be exactly 'relevant'")

        if record["contact_coordination_state"] != "prepared":
            raise ValueError("contact_coordination_state must be exactly 'prepared'")

        if record["contact_assembly_state"] != "assembled":
            raise ValueError("contact_assembly_state must be exactly 'assembled'")

        rank = record["opportunity_rank"]
        if not isinstance(rank, int) or isinstance(rank, bool):
            raise TypeError("opportunity_rank must be an integer")
        if rank < 1:
            raise ValueError("opportunity_rank must be >= 1")

        contact_enrichment_request = record["contact_enrichment_request"]
        if not isinstance(contact_enrichment_request, dict):
            raise TypeError("contact_enrichment_request must be a dictionary")

        matched_contact = record["matched_contact"]
        if not isinstance(matched_contact, dict):
            raise TypeError("matched_contact must be a dictionary")

        matched_contact_field_names = set(matched_contact.keys())
        required_matched_contact_field_names = set(MATCHED_CONTACT_FIELDS)

        missing_contact_fields = sorted(
            required_matched_contact_field_names - matched_contact_field_names
        )
        extra_contact_fields = sorted(
            matched_contact_field_names - required_matched_contact_field_names
        )

        if missing_contact_fields:
            raise ValueError(
                "matched_contact is missing required fields: "
                + ", ".join(missing_contact_fields)
            )

        if extra_contact_fields:
            raise ValueError(
                "matched_contact contains unknown fields: "
                + ", ".join(extra_contact_fields)
            )

        for field_name in MATCHED_CONTACT_FIELDS:
            field_value = matched_contact[field_name]
            if not isinstance(field_value, str):
                raise TypeError(f"matched_contact.{field_name} must be a non-empty string")
            if field_value.strip() == "":
                raise ValueError(f"matched_contact.{field_name} must be a non-empty string")

        contact_match_state = matched_contact["contact_match_state"]
        if contact_match_state not in ALLOWED_CONTACT_MATCH_STATES:
            raise ValueError("contact_match_state must be exactly 'matched' or 'unmatched'")


def _build_lead_record_id(record: dict[str, Any]) -> str:
    return (
        "leadrecord:"
        + record["canonical_organization_key"]
        + ":"
        + record["source_id"]
        + ":"
        + str(record["opportunity_rank"])
    )


def _build_lead_record(record: dict[str, Any]) -> dict[str, Any]:
    matched_contact = record["matched_contact"]

    return {
        "canonical_organization_id": record["canonical_organization_id"],
        "canonical_organization_key": record["canonical_organization_key"],
        "canonical_organization_name": record["canonical_organization_name"],
        "organization_domain_candidate_id": record["organization_domain_candidate_id"],
        "organization_candidate_id": record["organization_candidate_id"],
        "candidate_id": record["candidate_id"],
        "seed_id": record["seed_id"],
        "source_id": record["source_id"],
        "source_label": record["source_label"],
        "domain_candidate": record["domain_candidate"],
        "opportunity_rank": record["opportunity_rank"],
        "contact_name": matched_contact["contact_name"],
        "contact_title": matched_contact["contact_title"],
        "contact_email": matched_contact["contact_email"],
        "contact_source": matched_contact["contact_source"],
        "contact_match_state": matched_contact["contact_match_state"],
    }


def apply_lead_record_assembler(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Assemble deterministic lead records in stable input order."""
    validate_fg5_mb2_records(records)

    output_records: list[dict[str, Any]] = []

    for record in records:
        output_record = deepcopy(record)
        output_record["lead_record_id"] = _build_lead_record_id(record)
        output_record["lead_record"] = _build_lead_record(record)
        output_record["lead_record_state"] = LEAD_RECORD_STATE
        output_records.append(output_record)

    return output_records
