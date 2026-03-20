from typing import List, Dict, Any


def validate_organization_domain_edge_records(records: List[Dict[str, Any]]) -> None:
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
        "candidate_state": str,
    }
    
    for record in records:
        if not isinstance(record, dict):
            raise ValueError("each record must be a dict")
        
        # Check for extra fields
        if set(record.keys()) != set(required_fields.keys()):
            raise ValueError("record has missing or extra fields")
        
        # Check types and values
        for field, expected_type in required_fields.items():
            value = record[field]
            if not isinstance(value, expected_type):
                raise ValueError(f"{field} must be {expected_type.__name__}")
            
            if expected_type == str:
                if not value:
                    raise ValueError(f"{field} must be non-empty string")
                if field == "domain_classification" and value not in ["corporate", "generic"]:
                    raise ValueError(f"{field} must be exactly 'corporate' or 'generic'")
                elif field == "node_type" and value != "organization":
                    raise ValueError(f"{field} must be exactly 'organization'")
                elif field == "edge_type" and value != "has_domain":
                    raise ValueError(f"{field} must be exactly 'has_domain'")
                elif field == "edge_to" and value != "domainnode:" + record["domain_candidate"]:
                    raise ValueError(f"{field} must be exactly 'domainnode:' + domain_candidate")
                elif field == "candidate_state" and value != "organization_domain_edge_built":
                    raise ValueError(f"{field} must be exactly 'organization_domain_edge_built'")
            elif expected_type == list:
                if not value:
                    raise ValueError(f"{field} must be non-empty list")
                for item in value:
                    if not isinstance(item, str) or not item:
                        raise ValueError(f"all elements in {field} must be non-empty strings")


def build_domain_node_id(domain_candidate: str) -> str:
    return "domainnode:" + domain_candidate


def build_domain_node_record(record: Dict[str, Any]) -> Dict[str, Any]:
    new_record = record.copy()
    new_record["domain_node_id"] = build_domain_node_id(record["domain_candidate"])
    new_record["domain_node_type"] = "domain"
    new_record["domain_node_label"] = record["domain_candidate"]
    new_record["candidate_state"] = "domain_node_built"
    return new_record


def assemble_domain_nodes(records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    validate_organization_domain_edge_records(records)
    return [build_domain_node_record(record) for record in records]