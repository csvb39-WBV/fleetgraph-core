from __future__ import annotations

from copy import deepcopy
from typing import Any

from fleetgraph_core.intelligence.unified_event_schema import (
    get_supported_event_types,
    validate_unified_event_record,
)


_EVENT_TYPE_TO_NODE_TYPE = {
    "litigation": "case",
    "audit": "audit",
    "enforcement": "enforcement_action",
    "lien": "lien",
    "bond_claim": "bond_claim",
}


def get_supported_event_node_types() -> tuple[str, ...]:
    return ("case", "audit", "enforcement_action", "lien", "bond_claim")


def build_event_node(record: dict[str, Any]) -> dict[str, Any]:
    validate_unified_event_record(record)

    event_type = record["event_type"]
    if event_type not in get_supported_event_types():
        raise ValueError("event_type cannot be mapped to a supported event node type.")

    if event_type not in _EVENT_TYPE_TO_NODE_TYPE:
        raise ValueError("event_type cannot be mapped to a supported event node type.")

    node_type = _EVENT_TYPE_TO_NODE_TYPE[event_type]
    event_id = record["event_id"]
    company_name = record["company_name"]

    return {
        "node_id": f"{node_type}:{event_id}",
        "node_type": node_type,
        "label": f"{company_name} {node_type.replace('_', ' ')}",
        "properties": {
            "event_id": event_id,
            "event_type": event_type,
            "company_name": company_name,
            "source_name": record["source_name"],
            "status": record["status"],
            "event_date": record["event_date"],
            "jurisdiction": record["jurisdiction"],
            "project_name": record["project_name"],
            "agency_or_court": record["agency_or_court"],
            "severity": record["severity"],
            "amount": record["amount"],
            "currency": record["currency"],
            "service_fit": list(record["service_fit"]),
            "trigger_tags": list(record["trigger_tags"]),
            "evidence_summary": record["evidence"]["summary"],
            "source_record_id": record["evidence"]["source_record_id"],
            "event_details": deepcopy(record["event_details"]),
        },
    }


def build_event_node_batch(records: list[dict[str, Any]]) -> dict[str, Any]:
    if not isinstance(records, list):
        raise ValueError("records must be a list.")

    results = []
    nodes = []

    for index, record in enumerate(records):
        if not isinstance(record, dict):
            raise ValueError(f"records[{index}] must be a dictionary.")

        try:
            node = build_event_node(record)
        except ValueError as error:
            results.append(
                {
                    "index": index,
                    "ok": False,
                    "node_id": None,
                    "node_type": None,
                    "error": str(error),
                }
            )
            continue

        nodes.append(node)
        results.append(
            {
                "index": index,
                "ok": True,
                "node_id": node["node_id"],
                "node_type": node["node_type"],
                "error": None,
            }
        )

    invalid_records = sum(1 for result in results if not result["ok"])
    valid_records = sum(1 for result in results if result["ok"])

    return {
        "ok": invalid_records == 0,
        "total_records": len(records),
        "valid_records": valid_records,
        "invalid_records": invalid_records,
        "results": results,
        "nodes": nodes,
    }
