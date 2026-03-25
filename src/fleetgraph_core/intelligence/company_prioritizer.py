from __future__ import annotations

from copy import deepcopy
from typing import Any

from fleetgraph_core.intelligence.company_signal_aggregator import (
    aggregate_company_signal_batch,
)


SIGNAL_TYPE_SCORES = {
    "litigation_risk": 40,
    "audit_risk": 30,
    "enforcement_risk": 35,
    "payment_risk": 25,
}


def get_supported_priority_levels() -> tuple[str, ...]:
    return (
        "critical",
        "high",
        "medium",
        "low",
    )


def _validate_aggregator_output(
    aggregator_output: dict[str, Any],
) -> list[dict[str, Any]]:
    if not isinstance(aggregator_output, dict):
        raise ValueError(
            "company prioritization could not be completed from the provided record."
        )

    required_keys = {
        "ok",
        "total_records",
        "valid_records",
        "invalid_records",
        "results",
        "companies",
    }
    if set(aggregator_output.keys()) != required_keys:
        raise ValueError(
            "company prioritization could not be completed from the provided record."
        )

    companies = aggregator_output.get("companies")
    results = aggregator_output.get("results")
    if not isinstance(companies, list) or not isinstance(results, list):
        raise ValueError(
            "company prioritization could not be completed from the provided record."
        )

    if aggregator_output["total_records"] != 1 or len(results) != 1:
        raise ValueError(
            "company prioritization could not be completed from the provided record."
        )

    result_row = results[0]
    if not isinstance(result_row, dict):
        raise ValueError(
            "company prioritization could not be completed from the provided record."
        )

    required_result_keys = {"index", "ok", "company_count", "error"}
    if set(result_row.keys()) != required_result_keys:
        raise ValueError(
            "company prioritization could not be completed from the provided record."
        )

    if result_row["ok"] is False:
        if not isinstance(result_row["error"], str):
            raise ValueError(
                "company prioritization could not be completed from the provided record."
            )
        raise ValueError(result_row["error"])

    for company in companies:
        if not isinstance(company, dict):
            raise ValueError(
                "company prioritization could not be completed from the provided record."
            )

    return companies


def _validate_company(company: dict[str, Any]) -> None:
    required_keys = {
        "company_node_id",
        "signal_types",
        "signal_count",
        "related_entities",
    }
    if set(company.keys()) != required_keys:
        raise ValueError(
            "company prioritization could not be completed from the provided record."
        )

    if not isinstance(company["company_node_id"], str):
        raise ValueError(
            "company prioritization could not be completed from the provided record."
        )
    if not isinstance(company["signal_count"], int):
        raise ValueError(
            "company prioritization could not be completed from the provided record."
        )

    signal_types = company["signal_types"]
    related_entities = company["related_entities"]
    if not isinstance(signal_types, list) or not isinstance(related_entities, list):
        raise ValueError(
            "company prioritization could not be completed from the provided record."
        )

    for signal_type in signal_types:
        if not isinstance(signal_type, str):
            raise ValueError(
                "company prioritization could not be completed from the provided record."
            )
        if signal_type not in SIGNAL_TYPE_SCORES:
            raise ValueError(
                "company prioritization could not be completed from the provided record."
            )

    for related_entity in related_entities:
        if not isinstance(related_entity, str):
            raise ValueError(
                "company prioritization could not be completed from the provided record."
            )


def _calculate_priority_score(company: dict[str, Any]) -> int:
    signal_type_score = 0
    for signal_type in company["signal_types"]:
        signal_type_score += SIGNAL_TYPE_SCORES[signal_type]

    volume_modifier = 10 if company["signal_count"] >= 2 else 0
    related_entity_modifier = 5 if len(company["related_entities"]) >= 2 else 0

    return signal_type_score + volume_modifier + related_entity_modifier


def _map_priority_level(priority_score: int) -> str:
    if priority_score >= 70:
        return "critical"
    if priority_score >= 45:
        return "high"
    if priority_score >= 25:
        return "medium"
    return "low"


def prioritize_companies(record: dict[str, Any]) -> dict[str, Any]:
    aggregator_output = aggregate_company_signal_batch([record])
    companies = _validate_aggregator_output(aggregator_output)

    prioritized_companies: list[dict[str, Any]] = []
    source_event_id = record.get("event_id")
    if not isinstance(source_event_id, str):
        raise ValueError(
            "company prioritization could not be completed from the provided record."
        )

    for company in companies:
        _validate_company(company)
        priority_score = _calculate_priority_score(company)
        prioritized_companies.append(
            {
                "company_node_id": company["company_node_id"],
                "priority_level": _map_priority_level(priority_score),
                "priority_score": priority_score,
                "signal_types": deepcopy(company["signal_types"]),
                "signal_count": company["signal_count"],
                "related_entities": deepcopy(company["related_entities"]),
            }
        )

    prioritized_companies = sorted(
        prioritized_companies,
        key=lambda company: company["priority_score"],
        reverse=True,
    )

    return {
        "source_event_id": source_event_id,
        "companies": prioritized_companies,
        "company_count": len(prioritized_companies),
    }


def prioritize_company_batch(records: list[dict[str, Any]]) -> dict[str, Any]:
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
            prioritized_result = prioritize_companies(record)
            valid_records += 1
            results.append(
                {
                    "index": index,
                    "ok": True,
                    "company_count": prioritized_result["company_count"],
                    "error": None,
                }
            )
            companies.extend(deepcopy(prioritized_result["companies"]))
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
    "prioritize_companies",
    "prioritize_company_batch",
    "get_supported_priority_levels",
]
