from __future__ import annotations

from copy import deepcopy
from typing import Any

from fleetgraph_core.api.api_response_models import (
    build_analysis_response,
    build_summary_response,
)


_SUPPORTED_RESPONSE_TYPES = (
    "analysis",
    "summary",
)


def get_supported_single_record_response_types() -> tuple[str, ...]:
    return _SUPPORTED_RESPONSE_TYPES


def _validate_analysis_response(response: dict[str, Any]) -> None:
    if not isinstance(response, dict):
        raise ValueError(
            "single record endpoint could not be built from the provided request."
        )

    required_keys = {
        "response_type",
        "source_event_id",
        "opportunity_count",
        "opportunities",
    }
    if set(response.keys()) != required_keys:
        raise ValueError(
            "single record endpoint could not be built from the provided request."
        )

    if response["response_type"] != "analysis":
        raise ValueError(
            "single record endpoint could not be built from the provided request."
        )
    if not isinstance(response["source_event_id"], str):
        raise ValueError(
            "single record endpoint could not be built from the provided request."
        )
    if isinstance(response["opportunity_count"], bool) or not isinstance(
        response["opportunity_count"], int
    ):
        raise ValueError(
            "single record endpoint could not be built from the provided request."
        )
    if not isinstance(response["opportunities"], list):
        raise ValueError(
            "single record endpoint could not be built from the provided request."
        )

    for opportunity in response["opportunities"]:
        if not isinstance(opportunity, dict):
            raise ValueError(
                "single record endpoint could not be built from the provided request."
            )

        required_opportunity_keys = {
            "company_node_id",
            "priority_level",
            "priority_score",
            "signal_types",
            "signal_count",
            "related_entities",
        }
        if set(opportunity.keys()) != required_opportunity_keys:
            raise ValueError(
                "single record endpoint could not be built from the provided request."
            )

        if not isinstance(opportunity["company_node_id"], str):
            raise ValueError(
                "single record endpoint could not be built from the provided request."
            )
        if not isinstance(opportunity["priority_level"], str):
            raise ValueError(
                "single record endpoint could not be built from the provided request."
            )
        if isinstance(opportunity["priority_score"], bool) or not isinstance(
            opportunity["priority_score"], int
        ):
            raise ValueError(
                "single record endpoint could not be built from the provided request."
            )
        if isinstance(opportunity["signal_count"], bool) or not isinstance(
            opportunity["signal_count"], int
        ):
            raise ValueError(
                "single record endpoint could not be built from the provided request."
            )
        if not isinstance(opportunity["signal_types"], list):
            raise ValueError(
                "single record endpoint could not be built from the provided request."
            )
        if not isinstance(opportunity["related_entities"], list):
            raise ValueError(
                "single record endpoint could not be built from the provided request."
            )

        for signal_type in opportunity["signal_types"]:
            if not isinstance(signal_type, str):
                raise ValueError(
                    "single record endpoint could not be built from the provided request."
                )

        for related_entity in opportunity["related_entities"]:
            if not isinstance(related_entity, str):
                raise ValueError(
                    "single record endpoint could not be built from the provided request."
                )


def _validate_summary_response(response: dict[str, Any]) -> None:
    if not isinstance(response, dict):
        raise ValueError(
            "single record endpoint could not be built from the provided request."
        )

    required_keys = {
        "response_type",
        "source_event_id",
        "summary",
    }
    if set(response.keys()) != required_keys:
        raise ValueError(
            "single record endpoint could not be built from the provided request."
        )

    if response["response_type"] != "summary":
        raise ValueError(
            "single record endpoint could not be built from the provided request."
        )
    if not isinstance(response["source_event_id"], str):
        raise ValueError(
            "single record endpoint could not be built from the provided request."
        )
    if not isinstance(response["summary"], dict):
        raise ValueError(
            "single record endpoint could not be built from the provided request."
        )

    summary = response["summary"]
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
        raise ValueError(
            "single record endpoint could not be built from the provided request."
        )

    integer_keys = {
        "opportunity_count",
        "critical_count",
        "high_count",
        "medium_count",
        "low_count",
    }
    for key in integer_keys:
        if isinstance(summary[key], bool) or not isinstance(summary[key], int):
            raise ValueError(
                "single record endpoint could not be built from the provided request."
            )

    if summary["top_company_node_id"] is not None and not isinstance(
        summary["top_company_node_id"], str
    ):
        raise ValueError(
            "single record endpoint could not be built from the provided request."
        )
    if summary["top_priority_level"] is not None and not isinstance(
        summary["top_priority_level"], str
    ):
        raise ValueError(
            "single record endpoint could not be built from the provided request."
        )
    if summary["top_priority_score"] is not None and (
        isinstance(summary["top_priority_score"], bool)
        or not isinstance(summary["top_priority_score"], int)
    ):
        raise ValueError(
            "single record endpoint could not be built from the provided request."
        )


def _validate_response_output(response_type: str, response: dict[str, Any]) -> None:
    if response_type == "analysis":
        _validate_analysis_response(response)
        return

    _validate_summary_response(response)


def handle_single_record_request(request_payload: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(request_payload, dict):
        raise ValueError("request_payload must be a dictionary.")

    required_keys = {
        "response_type",
        "record",
        "limit",
        "minimum_priority",
    }
    if set(request_payload.keys()) != required_keys:
        raise ValueError("request_payload must contain exactly the required keys.")

    response_type = request_payload["response_type"]
    record = request_payload["record"]
    limit = request_payload["limit"]
    minimum_priority = request_payload["minimum_priority"]

    if not isinstance(response_type, str):
        raise ValueError("response_type must be a string.")
    if response_type not in _SUPPORTED_RESPONSE_TYPES:
        raise ValueError("response_type must be one of the supported response types.")
    if not isinstance(record, dict):
        raise ValueError("record must be a dictionary.")
    if limit is not None and (isinstance(limit, bool) or not isinstance(limit, int)):
        raise ValueError("limit must be an integer or None.")
    if minimum_priority is not None and not isinstance(minimum_priority, str):
        raise ValueError("minimum_priority must be a string or None.")

    if response_type == "analysis":
        response = build_analysis_response(
            record,
            limit=limit,
            minimum_priority=minimum_priority,
        )
    else:
        response = build_summary_response(
            record,
            limit=limit,
            minimum_priority=minimum_priority,
        )

    _validate_response_output(response_type, response)

    return {
        "ok": True,
        "response": deepcopy(response),
    }


__all__ = [
    "handle_single_record_request",
    "get_supported_single_record_response_types",
]
