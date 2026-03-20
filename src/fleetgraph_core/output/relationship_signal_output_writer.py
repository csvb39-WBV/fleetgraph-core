import json
from typing import Any, Dict, List


def validate_formatted_relationship_signal_records(records: List[Dict[str, Any]]) -> None:
    if not isinstance(records, list) or not records:
        raise ValueError("records must be a non-empty list")

    required_fields = {
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

        if record["output_schema_version"] != "1.0":
            raise ValueError("output_schema_version must be exactly '1.0'")

        if record["domain_classification"] not in ("corporate", "generic"):
            raise ValueError("domain_classification must be exactly 'corporate' or 'generic'")

        if record["candidate_state"] != "relationship_signal_formatted":
            raise ValueError("candidate_state must be exactly 'relationship_signal_formatted'")


def build_output_payload(records: List[Dict[str, Any]]) -> Dict[str, Any]:
    payload_records = []
    for record in records:
        payload_records.append(
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

    return {
        "output_type": "relationship_signal_output",
        "output_schema_version": "1.0",
        "record_count": len(records),
        "records": payload_records,
    }


def serialize_relationship_signal_output(payload: Dict[str, Any]) -> str:
    return json.dumps(payload, indent=2, sort_keys=False, ensure_ascii=True)


def write_relationship_signal_output(
    records: List[Dict[str, Any]],
    output_path: str = "relationship_signals_output.json",
) -> str:
    validate_formatted_relationship_signal_records(records)
    payload = build_output_payload(records)
    serialized_payload = serialize_relationship_signal_output(payload)

    with open(output_path, "w", encoding="utf-8") as file_handle:
        file_handle.write(serialized_payload)

    return output_path