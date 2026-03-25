from __future__ import annotations

from copy import deepcopy
from typing import Any

from fleetgraph_core.graph_construction.event_node_models import build_event_node
from fleetgraph_core.graph_construction.supporting_entity_models import (
    build_supporting_entity_nodes,
)


_EVENT_TO_COMPANY_EDGE_TYPE = {
    "litigation": "SUBJECT_OF_CASE",
    "audit": "SUBJECT_OF_AUDIT",
    "enforcement": "SUBJECT_OF_ENFORCEMENT",
    "lien": "SUBJECT_OF_CASE",
    "bond_claim": "SUBJECT_OF_CASE",
}


def get_supported_event_relationship_types() -> tuple[str, ...]:
    return (
        "SUBJECT_OF_CASE",
        "SUBJECT_OF_AUDIT",
        "SUBJECT_OF_ENFORCEMENT",
        "RELATES_TO_PROJECT",
        "ISSUED_BY",
    )


def _build_edge(
    from_node: str,
    to_node: str,
    edge_type: str,
    source_event_id: str,
    source_event_type: str,
) -> dict[str, Any]:
    return {
        "edge_id": f"{from_node}->{edge_type}->{to_node}",
        "from_node": from_node,
        "to_node": to_node,
        "edge_type": edge_type,
        "properties": {
            "source_event_id": source_event_id,
            "source_event_type": source_event_type,
        },
    }


def build_event_relationship_edges(record: dict[str, Any]) -> list[dict[str, Any]]:
    event_node = build_event_node(record)
    supporting_nodes = build_supporting_entity_nodes(record)

    company_node = None
    project_node = None
    agency_node = None
    court_node = None

    for node in supporting_nodes:
        node_type = node["node_type"]
        if node_type == "company":
            company_node = node
        elif node_type == "project":
            project_node = node
        elif node_type == "agency":
            agency_node = node
        elif node_type == "court":
            court_node = node

    source_event_type = event_node["properties"]["event_type"]
    source_event_id = event_node["properties"]["event_id"]

    if company_node is None or source_event_type not in _EVENT_TO_COMPANY_EDGE_TYPE:
        raise ValueError(
            "event relationship edges could not be derived from the provided record."
        )

    edges = [
        _build_edge(
            event_node["node_id"],
            company_node["node_id"],
            _EVENT_TO_COMPANY_EDGE_TYPE[source_event_type],
            source_event_id,
            source_event_type,
        )
    ]

    if project_node is not None:
        edges.append(
            _build_edge(
                event_node["node_id"],
                project_node["node_id"],
                "RELATES_TO_PROJECT",
                source_event_id,
                source_event_type,
            )
        )

    if agency_node is not None:
        edges.append(
            _build_edge(
                event_node["node_id"],
                agency_node["node_id"],
                "ISSUED_BY",
                source_event_id,
                source_event_type,
            )
        )
    elif court_node is not None:
        edges.append(
            _build_edge(
                event_node["node_id"],
                court_node["node_id"],
                "ISSUED_BY",
                source_event_id,
                source_event_type,
            )
        )

    return edges


def build_event_relationship_edge_batch(records: list[dict[str, Any]]) -> dict[str, Any]:
    if not isinstance(records, list):
        raise ValueError("records must be a list.")

    for index, record in enumerate(records):
        if not isinstance(record, dict):
            raise ValueError(f"records[{index}] must be a dictionary.")

    results = []
    edges = []
    valid_records = 0
    invalid_records = 0

    for index, record in enumerate(records):
        try:
            built_edges = build_event_relationship_edges(record)
        except ValueError as error:
            invalid_records += 1
            results.append(
                {
                    "index": index,
                    "ok": False,
                    "edge_count": None,
                    "error": str(error),
                }
            )
            continue

        valid_records += 1
        results.append(
            {
                "index": index,
                "ok": True,
                "edge_count": len(built_edges),
                "error": None,
            }
        )
        edges.extend(deepcopy(built_edges))

    return {
        "ok": invalid_records == 0,
        "total_records": len(records),
        "valid_records": valid_records,
        "invalid_records": invalid_records,
        "results": results,
        "edges": edges,
    }
