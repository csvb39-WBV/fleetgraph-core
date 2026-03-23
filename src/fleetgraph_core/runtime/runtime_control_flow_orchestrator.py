"""FG-W17-P17-MB9 deterministic runtime control-flow orchestrator."""

from __future__ import annotations

from copy import deepcopy
from typing import Any

from fleetgraph_core.runtime.runtime_guardrail_orchestrator import (
    build_runtime_guardrail_orchestration,
)
from fleetgraph_core.runtime.runtime_operation_router import route_runtime_operation
from fleetgraph_core.runtime.runtime_request_envelope import (
    build_runtime_request_envelope,
)
from fleetgraph_core.runtime.runtime_response_envelope import (
    build_runtime_response_envelope,
)
from fleetgraph_core.runtime.runtime_security_orchestrator import (
    orchestrate_runtime_security,
)


_REQUIRED_TOP_LEVEL_KEYS: tuple[str, ...] = (
    "request_envelope_input",
    "security_orchestrator_input",
    "guardrail_orchestrator_input",
)


def _validate_top_level_schema(orchestrator_input: dict[str, Any]) -> None:
    present = set(orchestrator_input.keys())
    required = set(_REQUIRED_TOP_LEVEL_KEYS)

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


def _build_final_output(
    *,
    request_envelope: dict[str, Any],
    output_status: str,
    reasons: list[str],
    result: dict[str, Any],
) -> dict[str, Any]:
    # Delegate final envelope normalization/validation to the existing response builder.
    response_envelope = build_runtime_response_envelope(
        {
            "request_id": request_envelope["request_id"],
            "client_id": request_envelope["client_id"],
            "operation_type": request_envelope["operation_type"],
            "status": "accepted" if output_status == "accepted" else "failed",
            "result": deepcopy(result),
            "errors": list(reasons),
            "billing_enabled": request_envelope["billing_enabled"],
        }
    )

    return {
        "status": "accepted" if response_envelope["status"] == "accepted" else "rejected",
        "operation_type": response_envelope["operation_type"],
        "reasons": list(response_envelope["errors"]),
        "result": response_envelope["result"],
    }


def build_runtime_control_flow_orchestration(
    orchestrator_input: dict[str, Any],
) -> dict[str, Any]:
    """Run deterministic runtime control-flow orchestration across integrated modules."""
    if not isinstance(orchestrator_input, dict):
        raise TypeError("orchestrator_input must be a dict")

    _validate_top_level_schema(orchestrator_input)

    request_envelope_input = _require_dict(
        orchestrator_input["request_envelope_input"],
        "request_envelope_input",
    )
    security_orchestrator_input = _require_dict(
        orchestrator_input["security_orchestrator_input"],
        "security_orchestrator_input",
    )
    guardrail_orchestrator_input = _require_dict(
        orchestrator_input["guardrail_orchestrator_input"],
        "guardrail_orchestrator_input",
    )

    request_envelope = build_runtime_request_envelope(deepcopy(request_envelope_input))

    route = route_runtime_operation(
        {
            "operation_type": request_envelope["operation_type"],
        }
    )

    security = orchestrate_runtime_security(deepcopy(security_orchestrator_input))
    if security["status"] == "stop":
        return _build_final_output(
            request_envelope=request_envelope,
            output_status="rejected",
            reasons=list(security["reasons"]),
            result={
                "request_envelope": request_envelope,
                "route": route,
                "security": security,
                "guardrails": {},
            },
        )

    guardrails = build_runtime_guardrail_orchestration(deepcopy(guardrail_orchestrator_input))
    if guardrails["status"] == "stop":
        return _build_final_output(
            request_envelope=request_envelope,
            output_status="rejected",
            reasons=list(guardrails["reasons"]),
            result={
                "request_envelope": request_envelope,
                "route": route,
                "security": security,
                "guardrails": guardrails,
            },
        )

    return _build_final_output(
        request_envelope=request_envelope,
        output_status="accepted",
        reasons=["control_flow_checks_passed"],
        result={
            "request_envelope": request_envelope,
            "route": route,
            "security": security,
            "guardrails": guardrails,
        },
    )
