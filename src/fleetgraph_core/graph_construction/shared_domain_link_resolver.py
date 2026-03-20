from typing import Any, Dict, List, Tuple


def validate_unified_domain_records(records: List[Dict[str, Any]]) -> None:
    if not isinstance(records, list) or not records:
        raise ValueError("records must be a non-empty list")

    required_fields = {
        "unified_domain_id": str,
        "domain_node_ids": list,
        "domain_node_id": str,
        "domain_node_type": str,
        "domain_node_label": str,
        "domain_candidate": str,
        "domain_classification": str,
        "edge_tos": list,
        "source_ids": list,
        "organization_node_ids": list,
        "unified_organization_ids": list,
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

            if expected_type == str:
                if not value:
                    raise ValueError(f"{field} must be non-empty string")
                if field == "domain_node_type" and value != "domain":
                    raise ValueError("domain_node_type must be exactly 'domain'")
                if field == "domain_classification" and value not in ("corporate", "generic"):
                    raise ValueError("domain_classification must be exactly 'corporate' or 'generic'")
                if field == "candidate_state" and value != "domain_node_unified":
                    raise ValueError("candidate_state must be exactly 'domain_node_unified'")

            if expected_type == list:
                if not value:
                    raise ValueError(f"{field} must be non-empty list")
                for item in value:
                    if not isinstance(item, str) or not item:
                        raise ValueError(f"all elements in {field} must be non-empty strings")


def _ordered_pairs(ids: List[str]) -> List[Tuple[str, str]]:
    pairs = []
    for i in range(len(ids)):
        for j in range(i + 1, len(ids)):
            left = min(ids[i], ids[j])
            right = max(ids[i], ids[j])
            pairs.append((left, right))
    return pairs


def build_shared_domain_pairs(organization_node_ids: List[str]) -> List[Tuple[str, str]]:
    unique_ids = list(dict.fromkeys(organization_node_ids))
    if len(unique_ids) < 2:
        return []
    return _ordered_pairs(unique_ids)


def build_shared_domain_link_id(
    domain_candidate: str,
    left_organization_node_id: str,
    right_organization_node_id: str,
) -> str:
    return (
        "shareddomainlink:"
        + domain_candidate
        + ":"
        + left_organization_node_id
        + ":"
        + right_organization_node_id
    )


def build_shared_domain_link_record(
    record: Dict[str, Any],
    left_organization_node_id: str,
    right_organization_node_id: str,
) -> Dict[str, Any]:
    return {
        "shared_domain_link_id": build_shared_domain_link_id(
            record["domain_candidate"],
            left_organization_node_id,
            right_organization_node_id,
        ),
        "left_organization_node_id": left_organization_node_id,
        "right_organization_node_id": right_organization_node_id,
        "shared_domain_id": record["unified_domain_id"],
        "shared_domain_node_id": record["domain_node_id"],
        "shared_domain_candidate": record["domain_candidate"],
        "shared_domain_classification": record["domain_classification"],
        "supporting_unified_organization_ids": list(record["unified_organization_ids"]),
        "supporting_source_ids": list(record["source_ids"]),
        "candidate_state": "shared_domain_link_built",
    }


def assemble_shared_domain_links(records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    validate_unified_domain_records(records)
    result = []
    for record in records:
        pairs = build_shared_domain_pairs(record["organization_node_ids"])
        for left, right in pairs:
            result.append(build_shared_domain_link_record(record, left, right))
    return result
