"""FG6-MB3 deterministic CRM push gateway."""

from typing import Any


FG6_MB2_FIELDS = (
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
    "delivery_row_id",
    "delivery_row",
    "delivery_row_state",
)

DELIVERY_ROW_FIELDS = (
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

CRM_PAYLOAD_FIELDS = (
    "company_name",
    "company_domain",
    "company_source",
    "contact_full_name",
    "contact_job_title",
    "contact_email_address",
    "contact_data_source",
    "contact_match_status",
    "fleet_opportunity_rank",
    "internal_org_id",
    "internal_org_key",
    "internal_lead_id",
    "internal_candidate_id",
    "internal_source_id",
)

_CONSISTENCY_FIELDS = (
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
)

CRM_PAYLOAD_STATE = "gateway_ready"


def validate_fg6_mb2_records(records: list[dict[str, Any]]) -> None:
    """Validate exact FG6-MB2 records for CRM push gateway input."""
    if not isinstance(records, list):
        raise TypeError("records must be a list")

    for record in records:
        if not isinstance(record, dict):
            raise TypeError("each record must be a dictionary")

        field_names = set(record.keys())
        required_field_names = set(FG6_MB2_FIELDS)

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

        for field_name in FG6_MB2_FIELDS:
            if field_name in (
                "opportunity_rank",
                "contact_enrichment_request",
                "matched_contact",
                "lead_record",
                "delivery_row",
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

        if record["delivery_row_state"] != "flatfile_ready":
            raise ValueError("delivery_row_state must be exactly 'flatfile_ready'")

        rank = record["opportunity_rank"]
        if not isinstance(rank, int) or isinstance(rank, bool):
            raise TypeError("opportunity_rank must be an integer")
        if rank < 1:
            raise ValueError("opportunity_rank must be >= 1")

        if not isinstance(record["contact_enrichment_request"], dict):
            raise TypeError("contact_enrichment_request must be a dictionary")

        if not isinstance(record["matched_contact"], dict):
            raise TypeError("matched_contact must be a dictionary")

        if not isinstance(record["lead_record"], dict):
            raise TypeError("lead_record must be a dictionary")

        delivery_row = record["delivery_row"]
        if not isinstance(delivery_row, dict):
            raise TypeError("delivery_row must be a dictionary")

        delivery_row_field_names = set(delivery_row.keys())
        required_delivery_row_field_names = set(DELIVERY_ROW_FIELDS)

        missing_delivery_fields = sorted(
            required_delivery_row_field_names - delivery_row_field_names
        )
        extra_delivery_fields = sorted(
            delivery_row_field_names - required_delivery_row_field_names
        )

        if missing_delivery_fields:
            raise ValueError(
                "delivery_row is missing required fields: "
                + ", ".join(missing_delivery_fields)
            )

        if extra_delivery_fields:
            raise ValueError(
                "delivery_row contains unknown fields: "
                + ", ".join(extra_delivery_fields)
            )

        for field_name in DELIVERY_ROW_FIELDS:
            if field_name == "opportunity_rank":
                continue

            field_value = delivery_row[field_name]
            if not isinstance(field_value, str):
                raise TypeError(f"delivery_row.{field_name} must be a non-empty string")
            if field_value.strip() == "":
                raise ValueError(f"delivery_row.{field_name} must be a non-empty string")

        nested_rank = delivery_row["opportunity_rank"]
        if not isinstance(nested_rank, int) or isinstance(nested_rank, bool):
            raise TypeError("delivery_row.opportunity_rank must be an integer")
        if nested_rank < 1:
            raise ValueError("delivery_row.opportunity_rank must be >= 1")

        for field_name in _CONSISTENCY_FIELDS:
            if record[field_name] != delivery_row[field_name]:
                raise ValueError(
                    f"delivery_row.{field_name} must match record.{field_name}"
                )


def _build_crm_payload_id(record: dict[str, Any]) -> str:
    return (
        "crmpayload:"
        + record["canonical_organization_key"]
        + ":"
        + record["source_id"]
        + ":"
        + str(record["opportunity_rank"])
    )


def _build_crm_payload(record: dict[str, Any]) -> dict[str, Any]:
    delivery_row = record["delivery_row"]
    return {
        "company_name": delivery_row["canonical_organization_name"],
        "company_domain": delivery_row["domain_candidate"],
        "company_source": delivery_row["source_label"],
        "contact_full_name": delivery_row["contact_name"],
        "contact_job_title": delivery_row["contact_title"],
        "contact_email_address": delivery_row["contact_email"],
        "contact_data_source": delivery_row["contact_source"],
        "contact_match_status": delivery_row["contact_match_state"],
        "fleet_opportunity_rank": delivery_row["opportunity_rank"],
        "internal_org_id": delivery_row["canonical_organization_id"],
        "internal_org_key": delivery_row["canonical_organization_key"],
        "internal_lead_id": delivery_row["lead_record_id"],
        "internal_candidate_id": delivery_row["candidate_id"],
        "internal_source_id": delivery_row["source_id"],
    }


def apply_crm_push_gateway(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Construct deterministic CRM gateway result objects in stable input order."""
    validate_fg6_mb2_records(records)

    output_records: list[dict[str, Any]] = []

    for record in records:
        output_records.append(
            {
                "crm_payload_id": _build_crm_payload_id(record),
                "crm_payload": _build_crm_payload(record),
                "crm_payload_state": CRM_PAYLOAD_STATE,
                "delivery_row_id": record["delivery_row_id"],
            }
        )

    return output_records
