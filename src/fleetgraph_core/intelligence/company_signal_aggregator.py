from __future__ import annotations

from copy import deepcopy
from typing import Any

from fleetgraph_core.intelligence.construction_signal_extractor import (
    extract_construction_signal_batch,
)


def get_supported_company_signal_types() -> tuple[str, ...]:
    return (
        "litigation_risk",
        "audit_risk",
        "enforcement_risk",
        "payment_risk",
    )


def _validate_signal_batch_output(signal_batch: dict[str, Any]) -> list[dict[str, Any]]:
    if not isinstance(signal_batch, dict):
        raise ValueError(
            "company signal aggregation could not be completed from the provided record."
        )

    required_keys = {
        "ok",
        "total_records",
        "valid_records",
        "invalid_records",
        "results",
        "signals",
    }
    if set(signal_batch.keys()) != required_keys:
        raise ValueError(
            "company signal aggregation could not be completed from the provided record."
        )

    signals = signal_batch.get("signals")
    results = signal_batch.get("results")
    if not isinstance(signals, list):
        raise ValueError(
            "company signal aggregation could not be completed from the provided record."
        )
    if not isinstance(results, list):
        raise ValueError(
            "company signal aggregation could not be completed from the provided record."
        )

    if signal_batch["total_records"] != 1:
        raise ValueError(
            "company signal aggregation could not be completed from the provided record."
        )
    if len(results) != 1:
        raise ValueError(
            "company signal aggregation could not be completed from the provided record."
        )

    result_row = results[0]
    if not isinstance(result_row, dict):
        raise ValueError(
            "company signal aggregation could not be completed from the provided record."
        )

    required_result_keys = {"index", "ok", "signal_count", "error"}
    if set(result_row.keys()) != required_result_keys:
        raise ValueError(
            "company signal aggregation could not be completed from the provided record."
        )

    if result_row["ok"] is False:
        if not isinstance(result_row["error"], str):
            raise ValueError(
                "company signal aggregation could not be completed from the provided record."
            )
        raise ValueError(result_row["error"])

    for signal in signals:
        if not isinstance(signal, dict):
            raise ValueError(
                "company signal aggregation could not be completed from the provided record."
            )

    return signals


def _validate_signal(signal: dict[str, Any]) -> None:
    required_keys = {
        "signal_id",
        "signal_type",
        "primary_entity",
        "related_entities",
        "source_event_id",
        "source_event_type",
    }
    if set(signal.keys()) != required_keys:
        raise ValueError(
            "company signal aggregation could not be completed from the provided record."
        )

    if not isinstance(signal["signal_id"], str):
        raise ValueError(
            "company signal aggregation could not be completed from the provided record."
        )
    if not isinstance(signal["signal_type"], str):
        raise ValueError(
            "company signal aggregation could not be completed from the provided record."
        )
    if not isinstance(signal["primary_entity"], str):
        raise ValueError(
            "company signal aggregation could not be completed from the provided record."
        )
    if not isinstance(signal["source_event_id"], str):
        raise ValueError(
            "company signal aggregation could not be completed from the provided record."
        )
    if not isinstance(signal["source_event_type"], str):
        raise ValueError(
            "company signal aggregation could not be completed from the provided record."
        )

    related_entities = signal["related_entities"]
    if not isinstance(related_entities, list):
        raise ValueError(
            "company signal aggregation could not be completed from the provided record."
        )

    for related_entity in related_entities:
        if not isinstance(related_entity, str):
            raise ValueError(
                "company signal aggregation could not be completed from the provided record."
            )


def aggregate_company_signals(record: dict[str, Any]) -> dict[str, Any]:
    signal_batch = extract_construction_signal_batch([record])
    signals = _validate_signal_batch_output(signal_batch)

    companies: list[dict[str, Any]] = []
    companies_by_node_id: dict[str, dict[str, Any]] = {}
    source_event_id: str | None = None

    for signal in signals:
        _validate_signal(signal)

        if source_event_id is None:
            source_event_id = signal["source_event_id"]

        company_node_id = signal["primary_entity"]
        if company_node_id not in companies_by_node_id:
            company_entry = {
                "company_node_id": company_node_id,
                "signal_types": [],
                "signal_count": 0,
                "related_entities": [],
            }
            companies_by_node_id[company_node_id] = company_entry
            companies.append(company_entry)

        company_entry = companies_by_node_id[company_node_id]
        company_entry["signal_count"] += 1

        signal_type = signal["signal_type"]
        if signal_type not in company_entry["signal_types"]:
            company_entry["signal_types"].append(signal_type)

        for related_entity in signal["related_entities"]:
            if related_entity not in company_entry["related_entities"]:
                company_entry["related_entities"].append(related_entity)

    if source_event_id is None:
        source_event_id = record.get("event_id")
        if not isinstance(source_event_id, str):
            raise ValueError(
                "company signal aggregation could not be completed from the provided record."
            )

    return {
        "source_event_id": source_event_id,
        "companies": deepcopy(companies),
        "company_count": len(companies),
    }


def aggregate_company_signal_batch(records: list[dict[str, Any]]) -> dict[str, Any]:
    if not isinstance(records, list):
        raise ValueError("records must be a list.")

    for index, record in enumerate(records):
        if not isinstance(record, dict):
            raise ValueError(f"records[{index}] must be a dictionary.")

    results: list[dict[str, Any]] = []
    companies: list[dict[str, Any]] = []
    valid_records = 0
    invalid_records = 0

    for index, record in enumerate(records):
        try:
            company_result = aggregate_company_signals(record)
            valid_records += 1
            results.append(
                {
                    "index": index,
                    "ok": True,
                    "company_count": company_result["company_count"],
                    "error": None,
                }
            )
            companies.extend(deepcopy(company_result["companies"]))
        except ValueError as error:
            invalid_records += 1
            results.append(
                {
                    "index": index,
                    "ok": False,
                    "company_count": None,
                    "error": str(error),
                }
            )

    return {
        "ok": invalid_records == 0,
        "total_records": len(records),
        "valid_records": valid_records,
        "invalid_records": invalid_records,
        "results": results,
        "companies": companies,
    }


__all__ = [
    "aggregate_company_signals",
    "aggregate_company_signal_batch",
    "get_supported_company_signal_types",
]
