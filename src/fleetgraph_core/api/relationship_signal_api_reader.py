import json
from typing import Any, Dict, List


def validate_relationship_signal_output_payload(payload: Dict[str, Any]) -> None:
    if not isinstance(payload, dict):
        raise ValueError("payload must be a dict")

    required_top_level_fields = {
        "output_type": str,
        "output_schema_version": str,
        "record_count": int,
        "records": list,
    }
    required_record_fields = {
        "output_record_id": str,
        "output_schema_version": str,
        "signal_id": str,
        "signal_type": str,
        "domain": str,
        "domain_classification": str,
        "organization_count": int,
        "link_count": int,
        "organization_node_ids": list,
        "organization_node_pairs": list,
        "supporting_unified_organization_ids": list,
        "supporting_source_ids": list,
        "candidate_state": str,
    }

    if set(payload.keys()) != set(required_top_level_fields.keys()):
        raise ValueError("payload has missing or extra top-level fields")

    for field, expected_type in required_top_level_fields.items():
        value = payload[field]

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

    if payload["output_type"] != "relationship_signal_output":
        raise ValueError("output_type must be exactly 'relationship_signal_output'")

    if payload["output_schema_version"] != "1.0":
        raise ValueError("output_schema_version must be exactly '1.0'")

    if payload["record_count"] != len(payload["records"]):
        raise ValueError("record_count must equal len(records)")

    for record in payload["records"]:
        if not isinstance(record, dict):
            raise ValueError("each record must be a dict")

        if set(record.keys()) != set(required_record_fields.keys()):
            raise ValueError("record has missing or extra fields")

        for field, expected_type in required_record_fields.items():
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

        if record["output_schema_version"] != "1.0":
            raise ValueError("record output_schema_version must be exactly '1.0'")

        if record["domain_classification"] not in ("corporate", "generic"):
            raise ValueError("domain_classification must be exactly 'corporate' or 'generic'")

        if record["candidate_state"] != "relationship_signal_formatted":
            raise ValueError("candidate_state must be exactly 'relationship_signal_formatted'")


def load_relationship_signal_output(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as file_handle:
        payload = json.load(file_handle)

    validate_relationship_signal_output_payload(payload)
    return payload


def get_relationship_signal_records(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    records = []
    for record in payload["records"]:
        records.append(
            {
                "output_record_id": record["output_record_id"],
                "output_schema_version": record["output_schema_version"],
                "signal_id": record["signal_id"],
                "signal_type": record["signal_type"],
                "domain": record["domain"],
                "domain_classification": record["domain_classification"],
                "organization_count": record["organization_count"],
                "link_count": record["link_count"],
                "organization_node_ids": list(record["organization_node_ids"]),
                "organization_node_pairs": list(record["organization_node_pairs"]),
                "supporting_unified_organization_ids": list(
                    record["supporting_unified_organization_ids"]
                ),
                "supporting_source_ids": list(record["supporting_source_ids"]),
                "candidate_state": record["candidate_state"],
            }
        )
    return records


def get_relationship_signal_output_summary(payload: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "output_type": payload["output_type"],
        "output_schema_version": payload["output_schema_version"],
        "record_count": payload["record_count"],
    }