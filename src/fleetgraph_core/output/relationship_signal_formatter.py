from typing import Any, Dict, List


def validate_relationship_signal_records(records: List[Dict[str, Any]]) -> None:
    if not isinstance(records, list) or not records:
        raise ValueError("records must be a non-empty list")

    required_fields = {
        "relationship_signal_id": str,
        "signal_type": str,
        "shared_domain_aggregate_id": str,
        "shared_domain_id": str,
        "shared_domain_node_id": str,
        "shared_domain_candidate": str,
        "shared_domain_classification": str,
        "organization_node_ids": list,
        "organization_node_pairs": list,
        "link_count": int,
        "organization_count": int,
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

            if expected_type is int:
                if type(value) is not int:
                    raise ValueError(f"{field} must be int")
                if value < 0:
                    raise ValueError(f"{field} must be non-negative integer")
                continue

            if not isinstance(value, expected_type):
                raise ValueError(f"{field} must be {expected_type.__name__}")

            if expected_type is str and not value:
                raise ValueError(f"{field} must be non-empty string")

            if expected_type is list:
                if not value:
                    raise ValueError(f"{field} must be non-empty list")
                for item in value:
                    if not isinstance(item, str) or not item:
                        raise ValueError(f"all elements in {field} must be non-empty strings")

        if record["shared_domain_classification"] not in ("corporate", "generic"):
            raise ValueError("shared_domain_classification must be exactly 'corporate' or 'generic'")

        if record["candidate_state"] != "relationship_signal_extracted":
            raise ValueError("candidate_state must be exactly 'relationship_signal_extracted'")


def build_output_record_id(relationship_signal_id: str) -> str:
    return "formattedsignal:" + relationship_signal_id


def build_formatted_relationship_signal_record(record: Dict[str, Any]) -> Dict[str, Any]:
    supporting_unified_organization_ids = list(
        dict.fromkeys(record["supporting_unified_organization_ids"])
    )
    supporting_source_ids = list(dict.fromkeys(record["supporting_source_ids"]))

    return {
        "output_record_id": build_output_record_id(record["relationship_signal_id"]),
        "output_schema_version": "1.0",
        "signal_id": record["relationship_signal_id"],
        "signal_type": record["signal_type"],
        "domain": record["shared_domain_candidate"],
        "domain_classification": record["shared_domain_classification"],
        "organization_count": record["organization_count"],
        "link_count": record["link_count"],
        "organization_node_ids": list(record["organization_node_ids"]),
        "organization_node_pairs": list(record["organization_node_pairs"]),
        "supporting_unified_organization_ids": supporting_unified_organization_ids,
        "supporting_source_ids": supporting_source_ids,
        "candidate_state": "relationship_signal_formatted",
    }


def assemble_formatted_relationship_signals(records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    validate_relationship_signal_records(records)
    return [build_formatted_relationship_signal_record(record) for record in records]
