"""Vertical-aware signal aggregation for active registry membership."""

from __future__ import annotations

from collections.abc import Mapping
from copy import deepcopy
from typing import Any

from fleetgraph_core.intelligence import signal_source_registry


def _append_unique(target: list[str], seen: set[str], source_name: str) -> None:
    if source_name not in seen:
        seen.add(source_name)
        target.append(source_name)


def _get_rejected_source_name(record: Mapping[str, Any]) -> str | None:
    source_name = record.get("source_name")
    if not isinstance(source_name, str):
        return None

    normalized_source_name = source_name.strip()
    if normalized_source_name == "":
        return None

    return normalized_source_name


def _resolve_active_vertical(runtime_config: Mapping[str, Any] | None) -> str:
    get_active_vertical = getattr(signal_source_registry, "get_active_vertical", None)
    if callable(get_active_vertical):
        return get_active_vertical(runtime_config)

    active_registry = signal_source_registry.get_signal_source_registry(runtime_config)
    active_source_names = signal_source_registry.get_signal_source_names(runtime_config)

    if tuple(active_registry.keys()) != active_source_names:
        raise ValueError("signal source registry adapter returned inconsistent source names")

    active_source_name_set = set(active_source_names)
    if "court_dockets" in active_source_name_set:
        return "construction_audit_litigation"

    if "permit" in active_source_name_set:
        return "fleet"

    raise ValueError("active vertical could not be derived from signal source registry")


def aggregate_signals(
    signal_records: list[dict[str, Any]],
    runtime_config: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Aggregate signals by active-vertical source registry membership."""
    if not isinstance(signal_records, list):
        raise ValueError("signal_records must be a list")

    active_vertical = _resolve_active_vertical(runtime_config)

    accepted_sources: list[str] = []
    rejected_sources: list[str] = []
    accepted_records: list[dict[str, Any]] = []
    rejected_records: list[dict[str, Any]] = []

    seen_accepted_sources: set[str] = set()
    seen_rejected_sources: set[str] = set()

    for signal_record in signal_records:
        if not isinstance(signal_record, dict):
            rejected_records.append(
                {
                    "record": deepcopy(signal_record),
                    "reason": "record must be a dictionary",
                }
            )
            continue

        record_copy = deepcopy(signal_record)

        if "source_name" not in signal_record:
            rejected_records.append(
                {
                    "record": record_copy,
                    "reason": "source_name is required",
                }
            )
            continue

        source_name = signal_record.get("source_name")
        if not signal_source_registry.has_signal_source(source_name, runtime_config):
            rejected_source_name = _get_rejected_source_name(signal_record)
            if rejected_source_name is not None:
                _append_unique(rejected_sources, seen_rejected_sources, rejected_source_name)

            rejected_records.append(
                {
                    "record": record_copy,
                    "reason": "source_name is not supported for active vertical",
                }
            )
            continue

        source_metadata = signal_source_registry.get_signal_source(source_name, runtime_config)
        accepted_source_name = str(source_metadata["source_name"])

        _append_unique(accepted_sources, seen_accepted_sources, accepted_source_name)
        accepted_records.append(record_copy)

    return {
        "ok": True,
        "vertical": active_vertical,
        "accepted_sources": accepted_sources,
        "rejected_sources": rejected_sources,
        "accepted_records": accepted_records,
        "rejected_records": rejected_records,
        "accepted_count": len(accepted_records),
        "rejected_count": len(rejected_records),
    }


__all__ = ["aggregate_signals"]