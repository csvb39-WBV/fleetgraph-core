from __future__ import annotations

from copy import deepcopy
from typing import Any

from fleetgraph_core.intelligence.company_prioritizer import (
    prioritize_company_batch,
)


_PRIORITY_LEVELS = (
    "critical",
    "high",
    "medium",
    "low",
)
_PRIORITY_RANK = {
    "critical": 0,
    "high": 1,
    "medium": 2,
    "low": 3,
}


def get_supported_opportunity_priority_levels() -> tuple[str, ...]:
    return _PRIORITY_LEVELS


def _validate_limit(limit: int | None) -> int | None:
    if limit is None:
        return None

    if isinstance(limit, bool) or not isinstance(limit, int):
        raise ValueError("limit must be an integer or None.")

    if limit <= 0:
        raise ValueError("limit must be greater than zero.")

    return limit


def _validate_minimum_priority(minimum_priority: str | None) -> str | None:
    if minimum_priority is None:
        return None

    if not isinstance(minimum_priority, str):
        raise ValueError("minimum_priority must be a string or None.")

    if minimum_priority not in _PRIORITY_LEVELS:
        raise ValueError("minimum_priority must be one of the supported priority levels.")

    return minimum_priority


def _validate_prioritizer_output(prioritizer_output: dict[str, Any]) -> list[dict[str, Any]]:
    if not isinstance(prioritizer_output, dict):
        raise ValueError("top opportunities view could not be built from the provided record.")

    required_keys = {
        "ok",
        "total_records",
        "valid_records",
        "invalid_records",
        "results",
        "companies",
    }
    if set(prioritizer_output.keys()) != required_keys:
        raise ValueError("top opportunities view could not be built from the provided record.")

    results = prioritizer_output["results"]
    companies = prioritizer_output["companies"]
    if not isinstance(results, list) or not isinstance(companies, list):
        raise ValueError("top opportunities view could not be built from the provided record.")

    if prioritizer_output["total_records"] != 1 or len(results) != 1:
        raise ValueError("top opportunities view could not be built from the provided record.")

    result_row = results[0]
    if not isinstance(result_row, dict):
        raise ValueError("top opportunities view could not be built from the provided record.")

    required_result_keys = {"index", "ok", "company_count", "error"}
    if set(result_row.keys()) != required_result_keys:
        raise ValueError("top opportunities view could not be built from the provided record.")

    if result_row["ok"] is False:
        if not isinstance(result_row["error"], str):
            raise ValueError("top opportunities view could not be built from the provided record.")
        raise ValueError(result_row["error"])

    for company in companies:
        _validate_company(company)

    return companies


def _validate_company(company: dict[str, Any]) -> None:
    if not isinstance(company, dict):
        raise ValueError("top opportunities view could not be built from the provided record.")

    required_keys = {
        "company_node_id",
        "priority_level",
        "priority_score",
        "signal_types",
        "signal_count",
        "related_entities",
    }
    if set(company.keys()) != required_keys:
        raise ValueError("top opportunities view could not be built from the provided record.")

    if not isinstance(company["company_node_id"], str):
        raise ValueError("top opportunities view could not be built from the provided record.")
    if not isinstance(company["priority_level"], str):
        raise ValueError("top opportunities view could not be built from the provided record.")
    if company["priority_level"] not in _PRIORITY_LEVELS:
        raise ValueError("top opportunities view could not be built from the provided record.")
    if isinstance(company["priority_score"], bool) or not isinstance(company["priority_score"], int):
        raise ValueError("top opportunities view could not be built from the provided record.")
    if isinstance(company["signal_count"], bool) or not isinstance(company["signal_count"], int):
        raise ValueError("top opportunities view could not be built from the provided record.")

    signal_types = company["signal_types"]
    related_entities = company["related_entities"]
    if not isinstance(signal_types, list) or not isinstance(related_entities, list):
        raise ValueError("top opportunities view could not be built from the provided record.")

    for signal_type in signal_types:
        if not isinstance(signal_type, str):
            raise ValueError("top opportunities view could not be built from the provided record.")

    for related_entity in related_entities:
        if not isinstance(related_entity, str):
            raise ValueError("top opportunities view could not be built from the provided record.")


def build_top_opportunities_view(
    record,
    *,
    limit: int | None = None,
    minimum_priority: str | None = None,
) -> dict[str, Any]:
    validated_limit = _validate_limit(limit)
    validated_minimum_priority = _validate_minimum_priority(minimum_priority)

    prioritizer_output = prioritize_company_batch([record])
    companies = _validate_prioritizer_output(prioritizer_output)

    source_event_id = record.get("event_id")
    if not isinstance(source_event_id, str):
        raise ValueError("top opportunities view could not be built from the provided record.")

    filtered_companies = []
    for company in companies:
        if validated_minimum_priority is not None:
            if _PRIORITY_RANK[company["priority_level"]] > _PRIORITY_RANK[validated_minimum_priority]:
                continue

        filtered_companies.append(
            {
                "company_node_id": company["company_node_id"],
                "priority_level": company["priority_level"],
                "priority_score": company["priority_score"],
                "signal_types": deepcopy(company["signal_types"]),
                "signal_count": company["signal_count"],
                "related_entities": deepcopy(company["related_entities"]),
            }
        )

    if validated_limit is not None:
        filtered_companies = filtered_companies[:validated_limit]

    return {
        "source_event_id": source_event_id,
        "opportunities": filtered_companies,
        "opportunity_count": len(filtered_companies),
    }


def build_top_opportunities_view_batch(
    records: list[dict[str, Any]],
    *,
    limit: int | None = None,
    minimum_priority: str | None = None,
) -> dict[str, Any]:
    validated_limit = _validate_limit(limit)
    validated_minimum_priority = _validate_minimum_priority(minimum_priority)

    if not isinstance(records, list):
        raise ValueError("records must be a list.")

    for index, record in enumerate(records):
        if not isinstance(record, dict):
            raise ValueError(f"records[{index}] must be a dictionary.")

    results: list[dict[str, Any]] = []
    views: list[dict[str, Any]] = []
    valid_records = 0
    invalid_records = 0

    for index, record in enumerate(records):
        try:
            view = build_top_opportunities_view(
                record,
                limit=validated_limit,
                minimum_priority=validated_minimum_priority,
            )
        except ValueError as error:
            invalid_records += 1
            results.append(
                {
                    "index": index,
                    "ok": False,
                    "opportunity_count": None,
                    "error": str(error),
                }
            )
            continue

        valid_records += 1
        results.append(
            {
                "index": index,
                "ok": True,
                "opportunity_count": view["opportunity_count"],
                "error": None,
            }
        )
        views.append(deepcopy(view))

    return {
        "ok": invalid_records == 0,
        "total_records": len(records),
        "valid_records": valid_records,
        "invalid_records": invalid_records,
        "results": results,
        "views": views,
    }

