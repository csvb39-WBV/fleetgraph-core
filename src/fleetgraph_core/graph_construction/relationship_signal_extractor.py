from typing import Any, Dict, List


def validate_shared_domain_aggregate_records(records: List[Dict[str, Any]]) -> None:
    if not isinstance(records, list) or not records:
        raise ValueError("records must be a non-empty list")

    required_fields = {
        "shared_domain_aggregate_id": str,
        "shared_domain_id": str,
        "shared_domain_node_id": str,
        "shared_domain_candidate": str,
        "shared_domain_classification": str,
        "shared_domain_link_ids": list,
        "organization_node_pairs": list,
        "organization_node_ids": list,
        "supporting_unified_organization_ids": list,
        "supporting_source_ids": list,
        "candidate_state": str,
    }

    for record in records:
        if not isinstance(record, dict):
            raise ValueError("each record must be a dict")

        if set(record.keys()) != set(required_fields.keys()):
            raise ValueError("record has missing or extra fields")

        for field, expected_type in required_fields.items():
            value = record[field]

            if not isinstance(value, expected_type):
                raise ValueError(f"{field} must be {expected_type.__name__}")

            if expected_type == str and not value:
                raise ValueError(f"{field} must be non-empty string")

            if expected_type == list:
                if not value:
                    raise ValueError(f"{field} must be non-empty list")
                for item in value:
                    if not isinstance(item, str) or not item:
                        raise ValueError(f"all elements in {field} must be non-empty strings")

        if record["shared_domain_classification"] not in ("corporate", "generic"):
            raise ValueError("shared_domain_classification must be exactly 'corporate' or 'generic'")

        if record["candidate_state"] != "shared_domain_link_aggregated":
            raise ValueError("candidate_state must be exactly 'shared_domain_link_aggregated'")


def build_relationship_signal_id(shared_domain_candidate: str) -> str:
    return "relationshipsignal:" + shared_domain_candidate


def build_relationship_signal_record(record: Dict[str, Any]) -> Dict[str, Any]:
    organization_node_ids = list(dict.fromkeys(record["organization_node_ids"]))
    supporting_unified_organization_ids = list(
        dict.fromkeys(record["supporting_unified_organization_ids"])
    )
    supporting_source_ids = list(dict.fromkeys(record["supporting_source_ids"]))

    return {
        "relationship_signal_id": build_relationship_signal_id(record["shared_domain_candidate"]),
        "signal_type": "shared_domain_relationship_detected",
        "shared_domain_aggregate_id": record["shared_domain_aggregate_id"],
        "shared_domain_id": record["shared_domain_id"],
        "shared_domain_node_id": record["shared_domain_node_id"],
        "shared_domain_candidate": record["shared_domain_candidate"],
        "shared_domain_classification": record["shared_domain_classification"],
        "organization_node_ids": organization_node_ids,
        "organization_node_pairs": list(record["organization_node_pairs"]),
        "link_count": len(record["shared_domain_link_ids"]),
        "organization_count": len(organization_node_ids),
        "supporting_unified_organization_ids": supporting_unified_organization_ids,
        "supporting_source_ids": supporting_source_ids,
        "candidate_state": "relationship_signal_extracted",
    }


def assemble_relationship_signals(records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    validate_shared_domain_aggregate_records(records)
    return [build_relationship_signal_record(record) for record in records]
