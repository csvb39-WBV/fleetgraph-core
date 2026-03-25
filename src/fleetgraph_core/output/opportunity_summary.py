from __future__ import annotations

from copy import deepcopy
from typing import Any

from fleetgraph_core.output.top_opportunities_view import (
    build_top_opportunities_view_batch,
)


_PRIORITY_LEVELS = (
    "critical",
    "high",
    "medium",
    "low",
)


def get_supported_summary_priority_levels() -> tuple[str, ...]:
    return _PRIORITY_LEVELS


def _validate_view_batch_output(view_batch_output: dict[str, Any]) -> list[dict[str, Any]]:
    if not isinstance(view_batch_output, dict):
        raise ValueError("opportunity summary could not be built from the provided record.")

    required_keys = {
        "ok",
        "total_records",
        "valid_records",
        "invalid_records",
        "results",
        "views",
    }
    if set(view_batch_output.keys()) != required_keys:
        raise ValueError("opportunity summary could not be built from the provided record.")

    results = view_batch_output["results"]
    views = view_batch_output["views"]
    if not isinstance(results, list) or not isinstance(views, list):
        raise ValueError("opportunity summary could not be built from the provided record.")

    if view_batch_output["total_records"] != 1 or len(results) != 1:
        raise ValueError("opportunity summary could not be built from the provided record.")

    result_row = results[0]
    if not isinstance(result_row, dict):
        raise ValueError("opportunity summary could not be built from the provided record.")

    required_result_keys = {"index", "ok", "opportunity_count", "error"}
    if set(result_row.keys()) != required_result_keys:
        raise ValueError("opportunity summary could not be built from the provided record.")

    if result_row["ok"] is False:
        if not isinstance(result_row["error"], str):
            raise ValueError("opportunity summary could not be built from the provided record.")
        raise ValueError(result_row["error"])

    if len(views) != 1:
        raise ValueError("opportunity summary could not be built from the provided record.")

    for view in views:
        _validate_view(view)

    return views


def _validate_view(view: dict[str, Any]) -> None:
    if not isinstance(view, dict):
        raise ValueError("opportunity summary could not be built from the provided record.")

    required_keys = {
        "source_event_id",
        "opportunities",
        "opportunity_count",
    }
    if set(view.keys()) != required_keys:
        raise ValueError("opportunity summary could not be built from the provided record.")

    if not isinstance(view["source_event_id"], str):
        raise ValueError("opportunity summary could not be built from the provided record.")
    if isinstance(view["opportunity_count"], bool) or not isinstance(view["opportunity_count"], int):
        raise ValueError("opportunity summary could not be built from the provided record.")

    opportunities = view["opportunities"]
    if not isinstance(opportunities, list):
        raise ValueError("opportunity summary could not be built from the provided record.")

    for opportunity in opportunities:
        _validate_opportunity(opportunity)


def _validate_opportunity(opportunity: dict[str, Any]) -> None:
    if not isinstance(opportunity, dict):
        raise ValueError("opportunity summary could not be built from the provided record.")

    required_keys = {
        "company_node_id",
        "priority_level",
        "priority_score",
        "signal_types",
        "signal_count",
        "related_entities",
    }
    if set(opportunity.keys()) != required_keys:
        raise ValueError("opportunity summary could not be built from the provided record.")

    if not isinstance(opportunity["company_node_id"], str):
        raise ValueError("opportunity summary could not be built from the provided record.")
    if not isinstance(opportunity["priority_level"], str):
        raise ValueError("opportunity summary could not be built from the provided record.")
    if opportunity["priority_level"] not in _PRIORITY_LEVELS:
        raise ValueError("opportunity summary could not be built from the provided record.")
    if isinstance(opportunity["priority_score"], bool) or not isinstance(opportunity["priority_score"], int):
        raise ValueError("opportunity summary could not be built from the provided record.")


def build_opportunity_summary(
    record,
    *,
    limit: int | None = None,
    minimum_priority: str | None = None,
) -> dict[str, Any]:
    view_batch_output = build_top_opportunities_view_batch(
        [record],
        limit=limit,
        minimum_priority=minimum_priority,
    )
    views = _validate_view_batch_output(view_batch_output)
    view = views[0]

    opportunities = view["opportunities"]
    counts = {
        "critical_count": 0,
        "high_count": 0,
        "medium_count": 0,
        "low_count": 0,
    }

    for opportunity in opportunities:
        counts[f"{opportunity['priority_level']}_count"] += 1

    top_company_node_id: str | None = None
    top_priority_level: str | None = None
    top_priority_score: int | None = None

    if opportunities:
        first_opportunity = opportunities[0]
        top_company_node_id = first_opportunity["company_node_id"]
        top_priority_level = first_opportunity["priority_level"]
        top_priority_score = first_opportunity["priority_score"]

    return {
        "source_event_id": view["source_event_id"],
        "summary": {
            "opportunity_count": len(opportunities),
            "critical_count": counts["critical_count"],
            "high_count": counts["high_count"],
            "medium_count": counts["medium_count"],
            "low_count": counts["low_count"],
            "top_company_node_id": top_company_node_id,
            "top_priority_level": top_priority_level,
            "top_priority_score": top_priority_score,
        },
    }


def build_opportunity_summary_batch(
    records: list[dict[str, Any]],
    *,
    limit: int | None = None,
    minimum_priority: str | None = None,
) -> dict[str, Any]:
    if not isinstance(records, list):
        raise ValueError("records must be a list.")

    for index, record in enumerate(records):
        if not isinstance(record, dict):
            raise ValueError(f"records[{index}] must be a dictionary.")

    results: list[dict[str, Any]] = []
    summaries: list[dict[str, Any]] = []
    valid_records = 0
    invalid_records = 0

    for index, record in enumerate(records):
        try:
            summary = build_opportunity_summary(
                record,
                limit=limit,
                minimum_priority=minimum_priority,
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
                "opportunity_count": summary["summary"]["opportunity_count"],
                "error": None,
            }
        )
        summaries.append(deepcopy(summary))

    return {
        "ok": invalid_records == 0,
        "total_records": len(records),
        "valid_records": valid_records,
        "invalid_records": invalid_records,
        "results": results,
        "summaries": summaries,
    }


__all__ = [
    "build_opportunity_summary",
    "build_opportunity_summary_batch",
    "get_supported_summary_priority_levels",
]
