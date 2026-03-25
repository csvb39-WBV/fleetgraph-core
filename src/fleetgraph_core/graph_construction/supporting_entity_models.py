from copy import deepcopy
from typing import Any

from fleetgraph_core.intelligence.unified_event_schema import (
    validate_unified_event_record,
)


def get_supported_supporting_entity_types() -> tuple[str, ...]:
    return ("company", "project", "agency", "court")


def _build_supporting_entity_node(
    node_type: str,
    name: str,
    event_id: str,
    event_type: str,
) -> dict[str, Any]:
    return {
        "node_id": f"{node_type}:{name}",
        "node_type": node_type,
        "label": name,
        "properties": {
            "name": name,
            "source_event_id": event_id,
            "source_event_type": event_type,
        },
    }


def build_supporting_entity_nodes(record: dict[str, Any]) -> list[dict[str, Any]]:
    validate_unified_event_record(record)

    event_record = deepcopy(record)
    event_id = event_record["event_id"]
    event_type = event_record["event_type"]

    nodes: list[dict[str, Any]] = []

    nodes.append(
        _build_supporting_entity_node(
            "company",
            event_record["company_name"],
            event_id,
            event_type,
        )
    )

    if event_record["project_name"] is not None:
        nodes.append(
            _build_supporting_entity_node(
                "project",
                event_record["project_name"],
                event_id,
                event_type,
            )
        )

    agency_or_court = event_record["agency_or_court"]
    if agency_or_court is not None:
        if event_type in ("audit", "enforcement"):
            nodes.append(
                _build_supporting_entity_node(
                    "agency",
                    agency_or_court,
                    event_id,
                    event_type,
                )
            )
        elif event_type == "litigation":
            nodes.append(
                _build_supporting_entity_node(
                    "court",
                    agency_or_court,
                    event_id,
                    event_type,
                )
            )

    return nodes


def build_supporting_entity_node_batch(records: list[dict[str, Any]]) -> dict[str, Any]:
    if not isinstance(records, list):
        raise ValueError("records must be a list.")

    for index, record in enumerate(records):
        if not isinstance(record, dict):
            raise ValueError(f"records[{index}] must be a dictionary.")

    results: list[dict[str, Any]] = []
    nodes: list[dict[str, Any]] = []
    valid_records = 0
    invalid_records = 0

    for index, record in enumerate(records):
        try:
            built_nodes = build_supporting_entity_nodes(record)
            valid_records += 1
            results.append(
                {
                    "index": index,
                    "ok": True,
                    "entity_count": len(built_nodes),
                    "error": None,
                }
            )
            nodes.extend(deepcopy(built_nodes))
        except Exception as error:
            invalid_records += 1
            results.append(
                {
                    "index": index,
                    "ok": False,
                    "entity_count": None,
                    "error": str(error),
                }
            )

    return {
        "ok": invalid_records == 0,
        "total_records": len(records),
        "valid_records": valid_records,
        "invalid_records": invalid_records,
        "results": results,
        "nodes": nodes,
    }


__all__ = [
    "build_supporting_entity_nodes",
    "build_supporting_entity_node_batch",
    "get_supported_supporting_entity_types",
]
