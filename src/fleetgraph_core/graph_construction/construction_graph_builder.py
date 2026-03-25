from __future__ import annotations

from copy import deepcopy
from typing import Any

from fleetgraph_core.graph_construction.event_node_models import build_event_node
from fleetgraph_core.graph_construction.event_relationship_builder import (
    build_event_relationship_edges,
)
from fleetgraph_core.graph_construction.supporting_entity_models import (
    build_supporting_entity_nodes,
)
from fleetgraph_core.graph_construction.supporting_entity_relationship_builder import (
    build_supporting_entity_relationship_edges,
)


def get_supported_construction_graph_sections() -> tuple[str, ...]:
    return (
        "nodes",
        "edges",
        "metadata",
    )


def _dedupe_by_id(
    items: list[dict[str, Any]],
    id_field: str,
) -> list[dict[str, Any]]:
    deduped_items: list[dict[str, Any]] = []
    seen_ids: set[str] = set()

    for item in items:
        if not isinstance(item, dict):
            raise ValueError(
                "graph assembly could not be completed from the provided record."
            )

        item_id = item.get(id_field)
        if not isinstance(item_id, str) or not item_id:
            raise ValueError(
                "graph assembly could not be completed from the provided record."
            )

        if item_id in seen_ids:
            continue

        deduped_items.append(deepcopy(item))
        seen_ids.add(item_id)

    return deduped_items


def build_construction_graph(record: dict[str, Any]) -> dict[str, Any]:
    event_node = build_event_node(record)
    supporting_nodes = build_supporting_entity_nodes(record)
    event_relationship_edges = build_event_relationship_edges(record)
    supporting_entity_relationship_edges = build_supporting_entity_relationship_edges(
        record
    )

    nodes = _dedupe_by_id(
        [event_node, *supporting_nodes],
        "node_id",
    )
    edges = _dedupe_by_id(
        [*event_relationship_edges, *supporting_entity_relationship_edges],
        "edge_id",
    )

    return {
        "nodes": nodes,
        "edges": edges,
        "metadata": {
            "source_event_id": event_node["properties"]["event_id"],
            "source_event_type": event_node["properties"]["event_type"],
            "node_count": len(nodes),
            "edge_count": len(edges),
        },
    }


def build_construction_graph_batch(records: list[dict[str, Any]]) -> dict[str, Any]:
    if not isinstance(records, list):
        raise ValueError("records must be a list.")

    for index, record in enumerate(records):
        if not isinstance(record, dict):
            raise ValueError(f"records[{index}] must be a dictionary.")

    results: list[dict[str, Any]] = []
    graphs: list[dict[str, Any]] = []
    valid_records = 0
    invalid_records = 0

    for index, record in enumerate(records):
        try:
            graph = build_construction_graph(record)
        except ValueError as error:
            invalid_records += 1
            results.append(
                {
                    "index": index,
                    "ok": False,
                    "node_count": None,
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
                "node_count": graph["metadata"]["node_count"],
                "edge_count": graph["metadata"]["edge_count"],
                "error": None,
            }
        )
        graphs.append(deepcopy(graph))

    return {
        "ok": invalid_records == 0,
        "total_records": len(records),
        "valid_records": valid_records,
        "invalid_records": invalid_records,
        "results": results,
        "graphs": graphs,
    }
