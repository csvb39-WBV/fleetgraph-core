from __future__ import annotations

from copy import deepcopy
from typing import Any

from fleetgraph_core.commercial.billing_hooks import evaluate_billing_hook
from fleetgraph_core.commercial.usage_metering import evaluate_usage_metering


_TOP_LEVEL_KEYS: tuple[str, ...] = (
    "usage_metering_input",
    "billing_hooks_input",
)


def _validate_top_level_schema(orchestrator_input: dict[str, Any]) -> None:
    present = set(orchestrator_input.keys())
    required = set(_TOP_LEVEL_KEYS)

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


def _require_dict(value: Any, field_name: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise TypeError(f"orchestrator_input field '{field_name}' must be a dict")
    return value


def build_runtime_commercial_orchestration(
    orchestrator_input: dict[str, Any],
) -> dict[str, Any]:
    """Execute deterministic commercial orchestration via delegated evaluators."""
    if not isinstance(orchestrator_input, dict):
        raise TypeError("orchestrator_input must be a dict")

    _validate_top_level_schema(orchestrator_input)

    usage_metering_input = _require_dict(
        orchestrator_input["usage_metering_input"],
        "usage_metering_input",
    )
    billing_hooks_input = _require_dict(
        orchestrator_input["billing_hooks_input"],
        "billing_hooks_input",
    )

    usage_result = evaluate_usage_metering(deepcopy(usage_metering_input))

    delegated_billing_input = deepcopy(billing_hooks_input)
    delegated_billing_input["usage_record"] = deepcopy(usage_result["usage_record"])
    billing_result = evaluate_billing_hook(delegated_billing_input)

    return {
        "status": "completed",
        "stage": "complete",
        "reasons": ["commercial_pipeline_completed"],
        "result": {
            "usage_record": usage_result["usage_record"],
            "billing_event": billing_result["billing_event"],
        },
    }