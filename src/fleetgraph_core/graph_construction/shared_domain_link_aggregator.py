from typing import Any, Dict, List


def validate_shared_domain_link_records(records: List[Dict[str, Any]]) -> None:
    if not isinstance(records, list) or not records:
        raise ValueError("records must be a non-empty list")

    required_fields = {
        "shared_domain_link_id": str,
        "left_organization_node_id": str,
        "right_organization_node_id": str,
        "shared_domain_id": str,
        "shared_domain_node_id": str,
        "shared_domain_candidate": str,
        "shared_domain_classification": str,
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

        if record["candidate_state"] != "shared_domain_link_built":
            raise ValueError("candidate_state must be exactly 'shared_domain_link_built'")

        if record["left_organization_node_id"] == record["right_organization_node_id"]:
            raise ValueError("left_organization_node_id must not equal right_organization_node_id")


def build_shared_domain_aggregate_key(record: Dict[str, Any]) -> str:
    return record["shared_domain_id"]


def merge_shared_domain_link_group(group: List[Dict[str, Any]]) -> Dict[str, Any]:
    if not isinstance(group, list) or not group:
        raise ValueError("group must be a non-empty list")

    first = group[0]

    shared_domain_link_ids: List[str] = []
    organization_node_pairs: List[str] = []
    organization_node_ids: List[str] = []
    supporting_unified_organization_ids: List[str] = []
    supporting_source_ids: List[str] = []

    organization_node_ids_seen = set()
    supporting_unified_organization_ids_seen = set()
    supporting_source_ids_seen = set()

    for record in group:
        shared_domain_link_ids.append(record["shared_domain_link_id"])

        left_id = record["left_organization_node_id"]
        right_id = record["right_organization_node_id"]
        organization_node_pairs.append(left_id + "|" + right_id)

        if left_id not in organization_node_ids_seen:
            organization_node_ids_seen.add(left_id)
            organization_node_ids.append(left_id)
        if right_id not in organization_node_ids_seen:
            organization_node_ids_seen.add(right_id)
            organization_node_ids.append(right_id)

        for unified_org_id in record["supporting_unified_organization_ids"]:
            if unified_org_id not in supporting_unified_organization_ids_seen:
                supporting_unified_organization_ids_seen.add(unified_org_id)
                supporting_unified_organization_ids.append(unified_org_id)

        for source_id in record["supporting_source_ids"]:
            if source_id not in supporting_source_ids_seen:
                supporting_source_ids_seen.add(source_id)
                supporting_source_ids.append(source_id)

    return {
        "shared_domain_aggregate_id": "shareddomainaggregate:" + first["shared_domain_candidate"],
        "shared_domain_id": first["shared_domain_id"],
        "shared_domain_node_id": first["shared_domain_node_id"],
        "shared_domain_candidate": first["shared_domain_candidate"],
        "shared_domain_classification": first["shared_domain_classification"],
        "shared_domain_link_ids": shared_domain_link_ids,
        "organization_node_pairs": organization_node_pairs,
        "organization_node_ids": organization_node_ids,
        "supporting_unified_organization_ids": supporting_unified_organization_ids,
        "supporting_source_ids": supporting_source_ids,
        "candidate_state": "shared_domain_link_aggregated",
    }


def build_shared_domain_aggregate(records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    grouped_records: Dict[str, List[Dict[str, Any]]] = {}
    group_order: List[str] = []

    for record in records:
        key = build_shared_domain_aggregate_key(record)
        if key not in grouped_records:
            grouped_records[key] = []
            group_order.append(key)
        grouped_records[key].append(record)

    return [merge_shared_domain_link_group(grouped_records[key]) for key in group_order]


def assemble_shared_domain_aggregates(records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    validate_shared_domain_link_records(records)
    return build_shared_domain_aggregate(records)
