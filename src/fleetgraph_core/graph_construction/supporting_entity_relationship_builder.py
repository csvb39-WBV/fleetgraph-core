from copy import deepcopy
from typing import Any

from fleetgraph_core.graph_construction.supporting_entity_models import (
    build_supporting_entity_nodes,
)


def get_supported_supporting_entity_relationship_types() -> tuple[str, ...]:
    return (
        "OVERSEEN_BY",
        "ADJUDICATED_BY",
    )


def _build_relationship_edge(
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


def build_supporting_entity_relationship_edges(
    record: dict[str, Any],
) -> list[dict[str, Any]]:
    supporting_nodes = build_supporting_entity_nodes(record)

    project_node = None
    agency_node = None
    court_node = None

    for supporting_node in supporting_nodes:
        if not isinstance(supporting_node, dict):
            raise ValueError(
                "supporting entity relationship edges could not be derived from the provided record."
            )

        expected_keys = {"node_id", "node_type", "label", "properties"}
        if set(supporting_node.keys()) != expected_keys:
            raise ValueError(
                "supporting entity relationship edges could not be derived from the provided record."
            )

        properties = supporting_node["properties"]
        if not isinstance(properties, dict):
            raise ValueError(
                "supporting entity relationship edges could not be derived from the provided record."
            )

        if set(properties.keys()) != {
            "name",
            "source_event_id",
            "source_event_type",
        }:
            raise ValueError(
                "supporting entity relationship edges could not be derived from the provided record."
            )

        node_type = supporting_node["node_type"]
        if node_type == "project" and project_node is None:
            project_node = supporting_node
        elif node_type == "agency" and agency_node is None:
            agency_node = supporting_node
        elif node_type == "court" and court_node is None:
            court_node = supporting_node

    edges: list[dict[str, Any]] = []
    seen_edge_ids: set[str] = set()

    if project_node is not None and agency_node is not None:
        agency_edge = _build_relationship_edge(
            project_node["node_id"],
            agency_node["node_id"],
            "OVERSEEN_BY",
            project_node["properties"]["source_event_id"],
            project_node["properties"]["source_event_type"],
        )
        if agency_edge["edge_id"] not in seen_edge_ids:
            edges.append(agency_edge)
            seen_edge_ids.add(agency_edge["edge_id"])

    if project_node is not None and court_node is not None:
        court_edge = _build_relationship_edge(
            project_node["node_id"],
            court_node["node_id"],
            "ADJUDICATED_BY",
            project_node["properties"]["source_event_id"],
            project_node["properties"]["source_event_type"],
        )
        if court_edge["edge_id"] not in seen_edge_ids:
            edges.append(court_edge)
            seen_edge_ids.add(court_edge["edge_id"])

    return edges


def build_supporting_entity_relationship_edge_batch(
    records: list[dict[str, Any]],
) -> dict[str, Any]:
    if not isinstance(records, list):
        raise ValueError("records must be a list.")

    for index, record in enumerate(records):
        if not isinstance(record, dict):
            raise ValueError(f"records[{index}] must be a dictionary.")

    results: list[dict[str, Any]] = []
    edges: list[dict[str, Any]] = []
    valid_records = 0
    invalid_records = 0

    for index, record in enumerate(records):
        try:
            built_edges = build_supporting_entity_relationship_edges(record)
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

    return {
        "ok": invalid_records == 0,
        "total_records": len(records),
        "valid_records": valid_records,
        "invalid_records": invalid_records,
        "results": results,
        "edges": edges,
    }


__all__ = [
    "build_supporting_entity_relationship_edges",
    "build_supporting_entity_relationship_edge_batch",
    "get_supported_supporting_entity_relationship_types",
]
