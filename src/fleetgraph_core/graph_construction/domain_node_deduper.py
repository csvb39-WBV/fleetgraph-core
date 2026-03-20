from typing import Any, Dict, List


def validate_domain_node_records(records: List[Dict[str, Any]]) -> None:
    if not isinstance(records, list) or not records:
        raise ValueError("records must be a non-empty list")

    required_fields = {
        "unified_organization_id": str,
        "canonical_organization_ids": list,
        "canonical_organization_name": str,
        "canonical_organization_key": str,
        "domain_candidate": str,
        "source_ids": list,
        "domain_classification": str,
        "node_id": str,
        "node_type": str,
        "node_label": str,
        "edge_id": str,
        "edge_type": str,
        "edge_from": str,
        "edge_to": str,
        "domain_node_id": str,
        "domain_node_type": str,
        "domain_node_label": str,
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

            if field == "domain_classification" and record["domain_classification"] not in ["corporate", "generic"]:
                raise ValueError("domain_classification must be exactly 'corporate' or 'generic'")

            if field == "node_type" and record["node_type"] != "organization":
                raise ValueError("node_type must be exactly 'organization'")

            if field == "edge_type" and record["edge_type"] != "has_domain":
                raise ValueError("edge_type must be exactly 'has_domain'")

            if field == "edge_to" and record["edge_to"] != "domainnode:" + record["domain_candidate"]:
                raise ValueError("edge_to must be exactly 'domainnode:' + domain_candidate")

            if field == "domain_node_id" and record["domain_node_id"] != "domainnode:" + record["domain_candidate"]:
                raise ValueError("domain_node_id must be exactly 'domainnode:' + domain_candidate")

            if field == "domain_node_type" and record["domain_node_type"] != "domain":
                raise ValueError("domain_node_type must be exactly 'domain'")

            if field == "domain_node_label" and record["domain_node_label"] != record["domain_candidate"]:
                raise ValueError("domain_node_label must be exactly domain_candidate")

            if field == "candidate_state" and record["candidate_state"] != "domain_node_built":
                raise ValueError("candidate_state must be exactly 'domain_node_built'")

            if expected_type == list:
                if not value:
                    raise ValueError(f"{field} must be non-empty list")
                for item in value:
                    if not isinstance(item, str) or not item:
                        raise ValueError(f"all elements in {field} must be non-empty strings")


def build_domain_group_key(record: Dict[str, Any]) -> str:
    return record["domain_node_id"]


def _ordered_unique(items: List[Any]) -> List[Any]:
    seen = set()
    result = []
    for item in items:
        if item not in seen:
            seen.add(item)
            result.append(item)
    return result


def merge_domain_node_group(group: List[Dict[str, Any]]) -> Dict[str, Any]:
    if not group:
        raise ValueError("group must be a non-empty list")

    first = group[0]
    domain_candidate = first["domain_candidate"]

    return {
        "unified_domain_id": "unifieddomain:" + domain_candidate,
        "domain_node_ids": [r["domain_node_id"] for r in group],
        "domain_node_id": first["domain_node_id"],
        "domain_node_type": first["domain_node_type"],
        "domain_node_label": first["domain_node_label"],
        "domain_candidate": first["domain_candidate"],
        "domain_classification": first["domain_classification"],
        "edge_tos": _ordered_unique([r["edge_to"] for r in group]),
        "source_ids": _ordered_unique([sid for r in group for sid in r["source_ids"]]),
        "organization_node_ids": _ordered_unique([r["node_id"] for r in group]),
        "unified_organization_ids": _ordered_unique([r["unified_organization_id"] for r in group]),
        "candidate_state": "domain_node_unified",
    }


def build_unified_domain_identity(records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    groups: Dict[str, List[Dict[str, Any]]] = {}
    order: List[str] = []

    for record in records:
        key = build_domain_group_key(record)
        if key not in groups:
            groups[key] = []
            order.append(key)
        groups[key].append(record)

    return [merge_domain_node_group(groups[key]) for key in order]


def assemble_unified_domain_nodes(records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    validate_domain_node_records(records)
    return build_unified_domain_identity(records)
