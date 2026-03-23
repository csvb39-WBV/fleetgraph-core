"""FG-W17-P17-MB5 deterministic runtime guardrail orchestrator."""

from __future__ import annotations

from copy import deepcopy
from typing import Any

from fleetgraph_core.runtime.runtime_query_cost_classifier import (
    build_runtime_query_cost_classification,
)
from fleetgraph_core.runtime.runtime_resource_guardrails import (
    build_runtime_resource_guardrails_response,
)
from fleetgraph_core.runtime.runtime_retrieval_guardrails import (
    build_runtime_retrieval_guardrails_response,
)


_REQUIRED_INPUT_KEYS: tuple[str, ...] = (
    "resource_guardrail_input",
    "retrieval_guardrail_input",
    "query_cost_input",
)


def _validate_closed_schema(orchestrator_input: dict[str, Any]) -> None:
    present = set(orchestrator_input.keys())
    required = set(_REQUIRED_INPUT_KEYS)

    missing = required - present
    if missing:
        raise ValueError(
            "orchestrator_input is missing required fields: "
            + ", ".join(sorted(missing))
        )

    extra = present - required
    if extra:
        raise ValueError(
            "orchestrator_input contains unexpected fields: "
            + ", ".join(sorted(extra))
        )


def _require_dict_field(value: Any, field_name: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise TypeError(f"orchestrator_input field '{field_name}' must be a dict")
    return value


def build_runtime_guardrail_orchestration(
    orchestrator_input: dict[str, Any],
) -> dict[str, Any]:
    """Apply runtime guardrail evaluators in deterministic stop-on-reject order."""
    if not isinstance(orchestrator_input, dict):
        raise TypeError("orchestrator_input must be a dict")

    _validate_closed_schema(orchestrator_input)

    resource_guardrail_input = _require_dict_field(
        orchestrator_input["resource_guardrail_input"],
        "resource_guardrail_input",
    )
    retrieval_guardrail_input = _require_dict_field(
        orchestrator_input["retrieval_guardrail_input"],
        "retrieval_guardrail_input",
    )
    query_cost_input = _require_dict_field(
        orchestrator_input["query_cost_input"],
        "query_cost_input",
    )

    resource_result = build_runtime_resource_guardrails_response(
        deepcopy(resource_guardrail_input)
    )
    if resource_result["status"] == "reject":
        return {
            "status": "stop",
            "stage": "resource_guardrails",
            "reasons": list(resource_result["violations"]),
        }

    retrieval_result = build_runtime_retrieval_guardrails_response(
        deepcopy(retrieval_guardrail_input)
    )
    if retrieval_result["status"] == "reject":
        return {
            "status": "stop",
            "stage": "retrieval_guardrails",
            "reasons": list(retrieval_result["violations"]),
        }

    query_cost_result = build_runtime_query_cost_classification(deepcopy(query_cost_input))
    if query_cost_result["classification"] == "reject":
        return {
            "status": "stop",
            "stage": "query_cost",
            "reasons": list(query_cost_result["reasons"]),
        }

    return {
        "status": "continue",
        "stage": "complete",
        "reasons": ["guardrail_checks_passed"],
    }