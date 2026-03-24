"""Construction-specific validation helpers layered on the canonical unified event schema."""

from __future__ import annotations

from typing import Any

from fleetgraph_core.intelligence.unified_event_schema import (
    get_supported_event_types,
    validate_unified_event_record,
)

_SUPPORTED_CONSTRUCTION_EVENT_TYPES: tuple[str, ...] = (
    "litigation",
    "audit",
    "enforcement",
    "lien",
    "bond_claim",
)


def get_supported_construction_event_types() -> tuple[str, ...]:
    """Return the construction-supported subset of canonical event types."""
    canonical_event_types = get_supported_event_types()
    return tuple(
        event_type
        for event_type in canonical_event_types
        if event_type in _SUPPORTED_CONSTRUCTION_EVENT_TYPES
    )


def validate_construction_event_record(record: dict[str, Any]) -> dict[str, Any]:
    """Validate one construction event record against the canonical unified schema."""
    validate_unified_event_record(record)

    if record["event_type"] not in get_supported_construction_event_types():
        raise ValueError("event_type must be one of the supported construction event types.")

    return {
        "ok": True,
        "event_type": record["event_type"],
        "event_id": record["event_id"],
    }


def validate_construction_event_batch(records: list[dict[str, Any]]) -> dict[str, Any]:
    """Validate a batch of construction event records with indexed deterministic results."""
    if not isinstance(records, list):
        raise ValueError("records must be a list.")

    results: list[dict[str, Any]] = []
    valid_records = 0
    invalid_records = 0

    for index, record in enumerate(records):
        if not isinstance(record, dict):
            raise ValueError(f"records[{index}] must be a dictionary.")

        try:
            validated_record = validate_construction_event_record(record)
        except ValueError as error:
            invalid_records += 1
            results.append(
                {
                    "index": index,
                    "ok": False,
                    "event_type": None,
                    "event_id": None,
                    "error": str(error),
                }
            )
            continue

        valid_records += 1
        results.append(
            {
                "index": index,
                "ok": True,
                "event_type": validated_record["event_type"],
                "event_id": validated_record["event_id"],
                "error": None,
            }
        )

    return {
        "ok": invalid_records == 0,
        "total_records": len(records),
        "valid_records": valid_records,
        "invalid_records": invalid_records,
        "results": results,
    }


__all__ = [
    "validate_construction_event_record",
    "validate_construction_event_batch",
    "get_supported_construction_event_types",
]
