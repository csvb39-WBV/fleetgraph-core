"""FG5-MB2 deterministic matched contact assembler."""

from copy import deepcopy
from typing import Any


FG5_MB1_FIELDS = (
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
)

MATCHED_CONTACT_STATE = "matched"
MATCHED_CONTACT_REQUEST_TYPE = "contact_enrichment"
_REQUEST_FIELD_NAMES = (
    "request_type",
    "canonical_organization_id",
    "canonical_organization_key",
    "canonical_organization_name",
    "domain_candidate",
    "source_id",
    "opportunity_rank",
)


def validate_fg5_mb1_records(records: list[dict[str, Any]]) -> None:
    """Validate exact FG5-MB1 records for matched contact assembly input."""
    if not isinstance(records, list):
        raise TypeError("records must be a list")

    for record in records:
        if not isinstance(record, dict):
            raise TypeError("each record must be a dictionary")

        field_names = set(record.keys())
        required_field_names = set(FG5_MB1_FIELDS)

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

        for field_name in FG5_MB1_FIELDS:
            if field_name in ("opportunity_rank", "contact_enrichment_request"):
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

        rank = record["opportunity_rank"]
        if not isinstance(rank, int) or isinstance(rank, bool):
            raise TypeError("opportunity_rank must be an integer")
        if rank < 1:
            raise ValueError("opportunity_rank must be >= 1")

        request = record["contact_enrichment_request"]
        if not isinstance(request, dict):
            raise TypeError("contact_enrichment_request must be a dictionary")

        request_field_names = set(request.keys())
        required_request_field_names = set(_REQUEST_FIELD_NAMES)

        request_missing_fields = sorted(required_request_field_names - request_field_names)
        request_extra_fields = sorted(request_field_names - required_request_field_names)

        if request_missing_fields:
            raise ValueError(
                "contact_enrichment_request is missing required fields: "
                + ", ".join(request_missing_fields)
            )

        if request_extra_fields:
            raise ValueError(
                "contact_enrichment_request contains unknown fields: "
                + ", ".join(request_extra_fields)
            )

        if request["request_type"] != MATCHED_CONTACT_REQUEST_TYPE:
            raise ValueError("request_type must be exactly 'contact_enrichment'")

        if request["canonical_organization_id"] != record["canonical_organization_id"]:
            raise ValueError("request canonical_organization_id must match record")

        if request["canonical_organization_key"] != record["canonical_organization_key"]:
            raise ValueError("request canonical_organization_key must match record")

        if request["canonical_organization_name"] != record["canonical_organization_name"]:
            raise ValueError("request canonical_organization_name must match record")

        if request["domain_candidate"] != record["domain_candidate"]:
            raise ValueError("request domain_candidate must match record")

        if request["source_id"] != record["source_id"]:
            raise ValueError("request source_id must match record")

        if request["opportunity_rank"] != record["opportunity_rank"]:
            raise ValueError("request opportunity_rank must match record")


def _build_matched_contact_id(record: dict[str, Any]) -> str:
    return (
        "matchedcontact:"
        + record["canonical_organization_key"]
        + ":"
        + record["source_id"]
        + ":"
        + str(record["opportunity_rank"])
    )


def _build_matched_contact(record: dict[str, Any]) -> dict[str, Any]:
    return {
        "contact_id": (
            "contact:"
            + record["canonical_organization_key"]
            + ":"
            + record["source_id"]
            + ":"
            + str(record["opportunity_rank"])
        ),
        "full_name": record["canonical_organization_name"] + " Contact",
        "email": "contact@" + record["domain_candidate"],
        "role": "decision_maker",
        "source_id": record["source_id"],
        "canonical_organization_id": record["canonical_organization_id"],
        "opportunity_rank": record["opportunity_rank"],
    }


def apply_matched_contact_assembler(
    records: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Assemble deterministic matched contacts in stable input order."""
    validate_fg5_mb1_records(records)

    output_records: list[dict[str, Any]] = []

    for record in records:
        output_record = deepcopy(record)
        output_record["matched_contact_id"] = _build_matched_contact_id(record)
        output_record["matched_contact"] = _build_matched_contact(record)
        output_record["matched_contact_state"] = MATCHED_CONTACT_STATE
        output_records.append(output_record)

    return output_records