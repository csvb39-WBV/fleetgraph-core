from __future__ import annotations

from copy import deepcopy
from typing import Any

from fleetgraph_core.graph_construction.construction_graph_builder import (
    build_construction_graph,
)
from fleetgraph_core.identity_resolution.construction_identity_resolver import (
    resolve_construction_graph_identities,
)


def get_supported_construction_signal_types() -> tuple[str, ...]:
    return (
        "litigation_risk",
        "audit_risk",
        "enforcement_risk",
        "payment_risk",
    )


def _map_event_type_to_signal_type(event_type: str) -> str:
    if event_type == "litigation":
        return "litigation_risk"
    if event_type == "audit":
        return "audit_risk"
    if event_type == "enforcement":
        return "enforcement_risk"
    if event_type in ("lien", "bond_claim"):
        return "payment_risk"
    raise ValueError(
        "construction signals could not be extracted from the provided record."
    )


def _validate_graph_output(graph: dict[str, Any]) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    if not isinstance(graph, dict):
        raise ValueError(
            "construction signals could not be extracted from the provided record."
        )

    if set(graph.keys()) != {"nodes", "edges", "metadata"}:
        raise ValueError(
            "construction signals could not be extracted from the provided record."
        )

    nodes = graph.get("nodes")
    metadata = graph.get("metadata")

    if not isinstance(nodes, list) or not isinstance(metadata, dict):
        raise ValueError(
            "construction signals could not be extracted from the provided record."
        )

    required_metadata_keys = {
        "source_event_id",
        "source_event_type",
        "node_count",
        "edge_count",
    }
    if set(metadata.keys()) != required_metadata_keys:
        raise ValueError(
            "construction signals could not be extracted from the provided record."
        )

    if not isinstance(metadata["source_event_id"], str) or not isinstance(
        metadata["source_event_type"], str
    ):
        raise ValueError(
            "construction signals could not be extracted from the provided record."
        )

    for node in nodes:
        if not isinstance(node, dict):
            raise ValueError(
                "construction signals could not be extracted from the provided record."
            )

    return nodes, metadata


def _validate_identity_output(identity_result: dict[str, Any]) -> list[dict[str, Any]]:
    if not isinstance(identity_result, dict):
        raise ValueError(
            "construction signals could not be extracted from the provided record."
        )

    required_keys = {
        "source_event_id",
        "source_event_type",
        "identities",
        "identity_count",
    }
    if set(identity_result.keys()) != required_keys:
        raise ValueError(
            "construction signals could not be extracted from the provided record."
        )

    identities = identity_result.get("identities")
    if not isinstance(identities, list):
        raise ValueError(
            "construction signals could not be extracted from the provided record."
        )

    for identity in identities:
        if not isinstance(identity, dict):
            raise ValueError(
                "construction signals could not be extracted from the provided record."
            )

    return identities


def extract_construction_signals(record: dict[str, Any]) -> dict[str, Any]:
    graph = build_construction_graph(record)
    identity_result = resolve_construction_graph_identities(record)

    nodes, metadata = _validate_graph_output(graph)
    identities = _validate_identity_output(identity_result)

    source_event_id = metadata["source_event_id"]
    source_event_type = metadata["source_event_type"]

    if identity_result["source_event_id"] != source_event_id:
        raise ValueError(
            "construction signals could not be extracted from the provided record."
        )
    if identity_result["source_event_type"] != source_event_type:
        raise ValueError(
            "construction signals could not be extracted from the provided record."
        )

    graph_node_ids = []
    for node in nodes:
        node_id = node.get("node_id")
        node_type = node.get("node_type")
        if not isinstance(node_id, str) or not isinstance(node_type, str):
            raise ValueError(
                "construction signals could not be extracted from the provided record."
            )
        graph_node_ids.append(node_id)

    identity_node_ids = set()
    for identity in identities:
        identity_node_id = identity.get("node_id")
        if not isinstance(identity_node_id, str):
            raise ValueError(
                "construction signals could not be extracted from the provided record."
            )
        identity_node_ids.add(identity_node_id)

    company_node_id = None
    related_entities: list[str] = []

    for node in nodes:
        node_id = node["node_id"]
        node_type = node["node_type"]

        if node_id not in identity_node_ids:
            continue

        if node_type == "company" and company_node_id is None:
            company_node_id = node_id
        elif node_type == "project":
            related_entities.append(node_id)
        elif source_event_type == "litigation" and node_type == "court":
            related_entities.append(node_id)
        elif source_event_type in ("audit", "enforcement") and node_type == "agency":
            related_entities.append(node_id)

    if company_node_id is None:
        raise ValueError(
            "construction signals could not be extracted from the provided record."
        )

    signal_type = _map_event_type_to_signal_type(source_event_type)
    signal = {
        "signal_id": f"{signal_type}:{source_event_id}",
        "signal_type": signal_type,
        "primary_entity": company_node_id,
        "related_entities": related_entities,
        "source_event_id": source_event_id,
        "source_event_type": source_event_type,
    }

    return {
        "source_event_id": source_event_id,
        "source_event_type": source_event_type,
        "signals": [signal],
        "signal_count": 1,
    }


def extract_construction_signal_batch(records: list[dict[str, Any]]) -> dict[str, Any]:
    if not isinstance(records, list):
        raise ValueError("records must be a list.")

    for index, record in enumerate(records):
        if not isinstance(record, dict):
            raise ValueError(f"records[{index}] must be a dictionary.")

    results: list[dict[str, Any]] = []
    signals: list[dict[str, Any]] = []
    valid_records = 0
    invalid_records = 0

    for index, record in enumerate(records):
        try:
            signal_result = extract_construction_signals(record)
            valid_records += 1
            results.append(
                {
                    "index": index,
                    "ok": True,
                    "signal_count": signal_result["signal_count"],
                    "error": None,
                }
            )
            signals.extend(deepcopy(signal_result["signals"]))
        except ValueError as error:
            invalid_records += 1
            results.append(
                {
                    "index": index,
                    "ok": False,
                    "signal_count": None,
                    "error": str(error),
                }
            )

    return {
        "ok": invalid_records == 0,
        "total_records": len(records),
        "valid_records": valid_records,
        "invalid_records": invalid_records,
        "results": results,
        "signals": signals,
    }


__all__ = [
    "extract_construction_signals",
    "extract_construction_signal_batch",
    "get_supported_construction_signal_types",
]
