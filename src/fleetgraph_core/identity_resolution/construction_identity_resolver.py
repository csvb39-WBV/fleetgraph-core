from __future__ import annotations

from copy import deepcopy
from typing import Any

from fleetgraph_core.graph_construction.construction_graph_builder import (
    build_construction_graph,
)


def get_supported_construction_identity_types() -> tuple[str, ...]:
    return (
        "event",
        "company",
        "project",
        "agency",
        "court",
    )


def _map_node_type_to_identity_type(node_type: str) -> str:
    if node_type in {"case", "audit", "enforcement_action", "lien", "bond_claim"}:
        return "event"
    if node_type in get_supported_construction_identity_types():
        return node_type
    raise ValueError(
        "construction identities could not be resolved from the provided record."
    )


def resolve_construction_graph_identities(
    record: dict[str, Any],
) -> dict[str, Any]:
    graph = build_construction_graph(record)

    metadata = graph.get("metadata")
    nodes = graph.get("nodes")

    if not isinstance(metadata, dict) or not isinstance(nodes, list):
        raise ValueError(
            "construction identities could not be resolved from the provided record."
        )

    source_event_id = metadata.get("source_event_id")
    source_event_type = metadata.get("source_event_type")

    if not isinstance(source_event_id, str) or not isinstance(
        source_event_type, str
    ):
        raise ValueError(
            "construction identities could not be resolved from the provided record."
        )

    identities: list[dict[str, Any]] = []
    seen_node_ids: set[str] = set()

    for node in nodes:
        if not isinstance(node, dict):
            raise ValueError(
                "construction identities could not be resolved from the provided record."
            )

        node_id = node.get("node_id")
        node_type = node.get("node_type")
        label = node.get("label")

        if (
            not isinstance(node_id, str)
            or not node_id
            or not isinstance(node_type, str)
            or not isinstance(label, str)
        ):
            raise ValueError(
                "construction identities could not be resolved from the provided record."
            )

        if node_id in seen_node_ids:
            continue

        identity_type = _map_node_type_to_identity_type(node_type)

        identities.append(
            {
                "identity_type": identity_type,
                "node_id": node_id,
                "canonical_name": label,
                "source_event_id": source_event_id,
                "source_event_type": source_event_type,
            }
        )

        seen_node_ids.add(node_id)

    return {
        "source_event_id": source_event_id,
        "source_event_type": source_event_type,
        "identities": identities,
        "identity_count": len(identities),
    }


def resolve_construction_graph_identity_batch(
    records: list[dict[str, Any]],
) -> dict[str, Any]:
    if not isinstance(records, list):
        raise ValueError("records must be a list.")

    for index, record in enumerate(records):
        if not isinstance(record, dict):
            raise ValueError(f"records[{index}] must be a dictionary.")

    results: list[dict[str, Any]] = []
    identity_sets: list[dict[str, Any]] = []
    valid_records = 0
    invalid_records = 0

    for index, record in enumerate(records):
        try:
            identities = resolve_construction_graph_identities(record)
            valid_records += 1
            results.append(
                {
                    "index": index,
                    "ok": True,
                    "identity_count": identities["identity_count"],
                    "error": None,
                }
            )
            identity_sets.append(deepcopy(identities))
        except ValueError as error:
            invalid_records += 1
            results.append(
                {
                    "index": index,
                    "ok": False,
                    "identity_count": None,
                    "error": str(error),
                }
            )

    return {
        "ok": invalid_records == 0,
        "total_records": len(records),
        "valid_records": valid_records,
        "invalid_records": invalid_records,
        "results": results,
        "identity_sets": identity_sets,
    }


__all__ = [
    "resolve_construction_graph_identities",
    "resolve_construction_graph_identity_batch",
    "get_supported_construction_identity_types",
]
