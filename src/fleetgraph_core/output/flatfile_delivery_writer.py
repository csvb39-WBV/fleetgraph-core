"""FG6-MB2 deterministic flatfile delivery writer."""

from copy import deepcopy
from typing import Any


FG6_MB1_FIELDS = (
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
    "lead_record_id",
    "lead_record",
    "lead_record_state",
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

FLATFILE_DELIVERY_ROW_FIELDS = (
    "lead_record_id",
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

_CONSISTENCY_FIELDS = (
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
)

DELIVERY_ROW_STATE = "flatfile_ready"


def validate_fg6_mb1_records(records: list[dict[str, Any]]) -> None:
    """Validate exact FG6-MB1 records for flatfile delivery writer input."""
    if not isinstance(records, list):
        raise TypeError("records must be a list")

    for record in records:
        if not isinstance(record, dict):
            raise TypeError("each record must be a dictionary")

        field_names = set(record.keys())
        required_field_names = set(FG6_MB1_FIELDS)

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

        for field_name in FG6_MB1_FIELDS:
            if field_name in (
                "opportunity_rank",
                "contact_enrichment_request",
                "matched_contact",
                "lead_record",
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

        if record["lead_record_state"] != "assembled":
            raise ValueError("lead_record_state must be exactly 'assembled'")

        rank = record["opportunity_rank"]
        if not isinstance(rank, int) or isinstance(rank, bool):
            raise TypeError("opportunity_rank must be an integer")
        if rank < 1:
            raise ValueError("opportunity_rank must be >= 1")

        if not isinstance(record["contact_enrichment_request"], dict):
            raise TypeError("contact_enrichment_request must be a dictionary")

        if not isinstance(record["matched_contact"], dict):
            raise TypeError("matched_contact must be a dictionary")

        lead_record = record["lead_record"]
        if not isinstance(lead_record, dict):
            raise TypeError("lead_record must be a dictionary")

        lead_record_field_names = set(lead_record.keys())
        required_lead_record_field_names = set(LEAD_RECORD_FIELDS)

        missing_lead_fields = sorted(required_lead_record_field_names - lead_record_field_names)
        extra_lead_fields = sorted(lead_record_field_names - required_lead_record_field_names)

        if missing_lead_fields:
            raise ValueError(
                "lead_record is missing required fields: " + ", ".join(missing_lead_fields)
            )

        if extra_lead_fields:
            raise ValueError(
                "lead_record contains unknown fields: " + ", ".join(extra_lead_fields)
            )

        for field_name in LEAD_RECORD_FIELDS:
            if field_name == "opportunity_rank":
                continue

            field_value = lead_record[field_name]
            if not isinstance(field_value, str):
                raise TypeError(f"lead_record.{field_name} must be a non-empty string")
            if field_value.strip() == "":
                raise ValueError(f"lead_record.{field_name} must be a non-empty string")

        nested_rank = lead_record["opportunity_rank"]
        if not isinstance(nested_rank, int) or isinstance(nested_rank, bool):
            raise TypeError("lead_record.opportunity_rank must be an integer")
        if nested_rank < 1:
            raise ValueError("lead_record.opportunity_rank must be >= 1")

        for field_name in _CONSISTENCY_FIELDS:
            if record[field_name] != lead_record[field_name]:
                raise ValueError(
                    f"lead_record.{field_name} must match record.{field_name}"
                )


def _build_delivery_row_id(record: dict[str, Any]) -> str:
    return (
        "deliveryrow:"
        + record["canonical_organization_key"]
        + ":"
        + record["source_id"]
        + ":"
        + str(record["opportunity_rank"])
    )


def _build_delivery_row(record: dict[str, Any]) -> dict[str, Any]:
    lead_record = record["lead_record"]
    return {
        "lead_record_id": record["lead_record_id"],
        "canonical_organization_id": lead_record["canonical_organization_id"],
        "canonical_organization_key": lead_record["canonical_organization_key"],
        "canonical_organization_name": lead_record["canonical_organization_name"],
        "organization_domain_candidate_id": lead_record["organization_domain_candidate_id"],
        "organization_candidate_id": lead_record["organization_candidate_id"],
        "candidate_id": lead_record["candidate_id"],
        "seed_id": lead_record["seed_id"],
        "source_id": lead_record["source_id"],
        "source_label": lead_record["source_label"],
        "domain_candidate": lead_record["domain_candidate"],
        "opportunity_rank": lead_record["opportunity_rank"],
        "contact_name": lead_record["contact_name"],
        "contact_title": lead_record["contact_title"],
        "contact_email": lead_record["contact_email"],
        "contact_source": lead_record["contact_source"],
        "contact_match_state": lead_record["contact_match_state"],
    }


def apply_flatfile_delivery_writer(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Format deterministic flatfile delivery rows in stable input order."""
    validate_fg6_mb1_records(records)

    output_records: list[dict[str, Any]] = []

    for record in records:
        output_record = deepcopy(record)
        output_record["delivery_row_id"] = _build_delivery_row_id(record)
        output_record["delivery_row"] = _build_delivery_row(record)
        output_record["delivery_row_state"] = DELIVERY_ROW_STATE
        output_records.append(output_record)

    return output_records
