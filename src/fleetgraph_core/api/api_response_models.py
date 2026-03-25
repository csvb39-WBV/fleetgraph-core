from __future__ import annotations

from copy import deepcopy
from typing import Any

from fleetgraph_core.output.opportunity_summary import build_opportunity_summary
from fleetgraph_core.output.top_opportunities_view import build_top_opportunities_view


def get_supported_api_response_types() -> tuple[str, ...]:
    return (
        "analysis",
        "summary",
    )


def _validate_analysis_output(analysis_output: dict[str, Any]) -> None:
    if not isinstance(analysis_output, dict):
        raise ValueError("api response could not be built from the provided record.")

    required_keys = {
        "source_event_id",
        "opportunities",
        "opportunity_count",
    }
    if set(analysis_output.keys()) != required_keys:
        raise ValueError("api response could not be built from the provided record.")

    if not isinstance(analysis_output["source_event_id"], str):
        raise ValueError("api response could not be built from the provided record.")
    if isinstance(analysis_output["opportunity_count"], bool) or not isinstance(
        analysis_output["opportunity_count"], int
    ):
        raise ValueError("api response could not be built from the provided record.")
    if not isinstance(analysis_output["opportunities"], list):
        raise ValueError("api response could not be built from the provided record.")

    for opportunity in analysis_output["opportunities"]:
        _validate_opportunity(opportunity)


def _validate_opportunity(opportunity: dict[str, Any]) -> None:
    if not isinstance(opportunity, dict):
        raise ValueError("api response could not be built from the provided record.")

    required_keys = {
        "company_node_id",
        "priority_level",
        "priority_score",
        "signal_types",
        "signal_count",
        "related_entities",
    }
    if set(opportunity.keys()) != required_keys:
        raise ValueError("api response could not be built from the provided record.")

    if not isinstance(opportunity["company_node_id"], str):
        raise ValueError("api response could not be built from the provided record.")
    if not isinstance(opportunity["priority_level"], str):
        raise ValueError("api response could not be built from the provided record.")
    if isinstance(opportunity["priority_score"], bool) or not isinstance(
        opportunity["priority_score"], int
    ):
        raise ValueError("api response could not be built from the provided record.")
    if isinstance(opportunity["signal_count"], bool) or not isinstance(
        opportunity["signal_count"], int
    ):
        raise ValueError("api response could not be built from the provided record.")
    if not isinstance(opportunity["signal_types"], list):
        raise ValueError("api response could not be built from the provided record.")
    if not isinstance(opportunity["related_entities"], list):
        raise ValueError("api response could not be built from the provided record.")

    for signal_type in opportunity["signal_types"]:
        if not isinstance(signal_type, str):
            raise ValueError("api response could not be built from the provided record.")

    for related_entity in opportunity["related_entities"]:
        if not isinstance(related_entity, str):
            raise ValueError("api response could not be built from the provided record.")


def _validate_summary_output(summary_output: dict[str, Any]) -> None:
    if not isinstance(summary_output, dict):
        raise ValueError("api response could not be built from the provided record.")

    required_keys = {
        "source_event_id",
        "summary",
    }
    if set(summary_output.keys()) != required_keys:
        raise ValueError("api response could not be built from the provided record.")

    if not isinstance(summary_output["source_event_id"], str):
        raise ValueError("api response could not be built from the provided record.")

    summary = summary_output["summary"]
    if not isinstance(summary, dict):
        raise ValueError("api response could not be built from the provided record.")

    required_summary_keys = {
        "opportunity_count",
        "critical_count",
        "high_count",
        "medium_count",
        "low_count",
        "top_company_node_id",
        "top_priority_level",
        "top_priority_score",
    }
    if set(summary.keys()) != required_summary_keys:
        raise ValueError("api response could not be built from the provided record.")

    integer_keys = {
        "opportunity_count",
        "critical_count",
        "high_count",
        "medium_count",
        "low_count",
    }
    for key in integer_keys:
        if isinstance(summary[key], bool) or not isinstance(summary[key], int):
            raise ValueError("api response could not be built from the provided record.")

    if summary["top_company_node_id"] is not None and not isinstance(
        summary["top_company_node_id"], str
    ):
        raise ValueError("api response could not be built from the provided record.")
    if summary["top_priority_level"] is not None and not isinstance(
        summary["top_priority_level"], str
    ):
        raise ValueError("api response could not be built from the provided record.")
    if summary["top_priority_score"] is not None and (
        isinstance(summary["top_priority_score"], bool)
        or not isinstance(summary["top_priority_score"], int)
    ):
        raise ValueError("api response could not be built from the provided record.")


def build_analysis_response(
    record,
    *,
    limit: int | None = None,
    minimum_priority: str | None = None,
) -> dict[str, Any]:
    analysis_output = build_top_opportunities_view(
        record,
        limit=limit,
        minimum_priority=minimum_priority,
    )
    _validate_analysis_output(analysis_output)

    return {
        "response_type": "analysis",
        "source_event_id": analysis_output["source_event_id"],
        "opportunity_count": analysis_output["opportunity_count"],
        "opportunities": deepcopy(analysis_output["opportunities"]),
    }


def build_summary_response(
    record,
    *,
    limit: int | None = None,
    minimum_priority: str | None = None,
) -> dict[str, Any]:
    summary_output = build_opportunity_summary(
        record,
        limit=limit,
        minimum_priority=minimum_priority,
    )
    _validate_summary_output(summary_output)

    return {
        "response_type": "summary",
        "source_event_id": summary_output["source_event_id"],
        "summary": deepcopy(summary_output["summary"]),
    }


__all__ = [
    "build_analysis_response",
    "build_summary_response",
    "get_supported_api_response_types",
]
