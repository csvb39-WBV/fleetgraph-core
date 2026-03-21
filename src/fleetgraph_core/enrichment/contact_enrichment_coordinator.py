"""FG5-MB1 deterministic contact enrichment coordinator."""

from copy import deepcopy
from typing import Any


FG4_MB2_FIELDS = (
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
)

CONTACT_COORDINATION_STATE = "prepared"
ENRICHMENT_REQUEST_TYPE = "contact_enrichment"


def validate_fg4_mb2_records(records: list[dict[str, Any]]) -> None:
    """Validate exact FG4-MB2 records for enrichment coordination input."""
    if not isinstance(records, list):
        raise TypeError("records must be a list")

    for record in records:
        if not isinstance(record, dict):
            raise TypeError("each record must be a dictionary")

        field_names = set(record.keys())
        required_field_names = set(FG4_MB2_FIELDS)

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

        for field_name in FG4_MB2_FIELDS:
            if field_name == "opportunity_rank":
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

        rank = record["opportunity_rank"]
        if not isinstance(rank, int) or isinstance(rank, bool):
            raise TypeError("opportunity_rank must be an integer")
        if rank < 1:
            raise ValueError("opportunity_rank must be >= 1")


def _build_contact_enrichment_request_id(record: dict[str, Any]) -> str:
    return (
        "enrichrequest:"
        + record["canonical_organization_key"]
        + ":"
        + record["source_id"]
        + ":"
        + str(record["opportunity_rank"])
    )


def _build_contact_enrichment_request(record: dict[str, Any]) -> dict[str, Any]:
    return {
        "request_type": ENRICHMENT_REQUEST_TYPE,
        "canonical_organization_id": record["canonical_organization_id"],
        "canonical_organization_key": record["canonical_organization_key"],
        "canonical_organization_name": record["canonical_organization_name"],
        "domain_candidate": record["domain_candidate"],
        "source_id": record["source_id"],
        "opportunity_rank": record["opportunity_rank"],
    }


def apply_contact_enrichment_coordinator(
    records: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Prepare deterministic contact enrichment requests in stable input order."""
    validate_fg4_mb2_records(records)

    output_records: list[dict[str, Any]] = []

    for record in records:
        output_record = deepcopy(record)
        output_record["contact_enrichment_request_id"] = (
            _build_contact_enrichment_request_id(record)
        )
        output_record["contact_enrichment_request"] = (
            _build_contact_enrichment_request(record)
        )
        output_record["contact_coordination_state"] = CONTACT_COORDINATION_STATE
        output_records.append(output_record)

    return output_records
