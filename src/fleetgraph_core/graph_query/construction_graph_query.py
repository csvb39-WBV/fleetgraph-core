from __future__ import annotations

from copy import deepcopy
from typing import Any

from fleetgraph_core.graph_construction.construction_graph_builder import (
    build_construction_graph,
)


def get_supported_construction_query_sections() -> tuple[str, ...]:
    return (
        "node_matches",
        "edge_matches",
        "summary",
    )


def _validate_filter_value(
    filter_value: str | None,
    field_name: str,
) -> str | None:
    if filter_value is None:
        return None

    if not isinstance(filter_value, str):
        raise ValueError(f"{field_name} must be a string or None.")

    return filter_value


def _validate_graph(
    graph: dict[str, Any],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], dict[str, Any]]:
    if not isinstance(graph, dict):
        raise ValueError(
            "construction graph query could not be completed from the provided record."
        )

    if set(graph.keys()) != {"nodes", "edges", "metadata"}:
        raise ValueError(
            "construction graph query could not be completed from the provided record."
        )

    nodes = graph["nodes"]
    edges = graph["edges"]
    metadata = graph["metadata"]

    if (
        not isinstance(nodes, list)
        or not isinstance(edges, list)
        or not isinstance(metadata, dict)
    ):
        raise ValueError(
            "construction graph query could not be completed from the provided record."
        )

    required_metadata_keys = {
        "source_event_id",
        "source_event_type",
        "node_count",
        "edge_count",
    }
    if set(metadata.keys()) != required_metadata_keys:
        raise ValueError(
            "construction graph query could not be completed from the provided record."
        )

    if not isinstance(metadata["source_event_id"], str) or not isinstance(
        metadata["source_event_type"], str
    ):
        raise ValueError(
            "construction graph query could not be completed from the provided record."
        )

    if not isinstance(metadata["node_count"], int) or not isinstance(
        metadata["edge_count"], int
    ):
        raise ValueError(
            "construction graph query could not be completed from the provided record."
        )

    for node in nodes:
        if not isinstance(node, dict):
            raise ValueError(
                "construction graph query could not be completed from the provided record."
            )

    for edge in edges:
        if not isinstance(edge, dict):
            raise ValueError(
                "construction graph query could not be completed from the provided record."
            )

    return nodes, edges, metadata


def query_construction_graph(
    record,
    *,
    node_type: str | None = None,
    edge_type: str | None = None,
) -> dict[str, Any]:
    validated_node_type = _validate_filter_value(node_type, "node_type")
    validated_edge_type = _validate_filter_value(edge_type, "edge_type")

    graph = build_construction_graph(record)
    nodes, edges, metadata = _validate_graph(graph)

    if validated_node_type is None:
        node_matches = deepcopy(nodes)
    else:
        node_matches = [
            deepcopy(node)
            for node in nodes
            if node.get("node_type") == validated_node_type
        ]

    if validated_edge_type is None:
        edge_matches = deepcopy(edges)
    else:
        edge_matches = [
            deepcopy(edge)
            for edge in edges
            if edge.get("edge_type") == validated_edge_type
        ]

    return {
        "node_matches": node_matches,
        "edge_matches": edge_matches,
        "summary": {
            "source_event_id": metadata["source_event_id"],
            "source_event_type": metadata["source_event_type"],
            "total_nodes": metadata["node_count"],
            "total_edges": metadata["edge_count"],
            "matched_nodes": len(node_matches),
            "matched_edges": len(edge_matches),
        },
    }


def query_construction_graph_batch(
    records: list[dict[str, Any]],
    *,
    node_type: str | None = None,
    edge_type: str | None = None,
) -> dict[str, Any]:
    validated_node_type = _validate_filter_value(node_type, "node_type")
    validated_edge_type = _validate_filter_value(edge_type, "edge_type")

    if not isinstance(records, list):
        raise ValueError("records must be a list.")

    for index, record in enumerate(records):
        if not isinstance(record, dict):
            raise ValueError(f"records[{index}] must be a dictionary.")

    results: list[dict[str, Any]] = []
    queries: list[dict[str, Any]] = []
    valid_records = 0
    invalid_records = 0

    for index, record in enumerate(records):
        try:
            query_result = query_construction_graph(
                record,
                node_type=validated_node_type,
                edge_type=validated_edge_type,
            )
        except ValueError as error:
            invalid_records += 1
            results.append(
                {
                    "index": index,
                    "ok": False,
                    "matched_nodes": None,
                    "matched_edges": None,
                    "error": str(error),
                }
            )
            continue

        valid_records += 1
        results.append(
            {
                "index": index,
                "ok": True,
                "matched_nodes": query_result["summary"]["matched_nodes"],
                "matched_edges": query_result["summary"]["matched_edges"],
                "error": None,
            }
        )
        queries.append(deepcopy(query_result))

    return {
        "ok": invalid_records == 0,
        "total_records": len(records),
        "valid_records": valid_records,
        "invalid_records": invalid_records,
        "results": results,
        "queries": queries,
    }
