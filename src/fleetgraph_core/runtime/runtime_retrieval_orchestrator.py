"""FG-W17-P17-MB7 deterministic runtime retrieval orchestrator."""

from __future__ import annotations

from copy import deepcopy
from typing import Any

from fleetgraph_core.persistence.recompute_gate import evaluate_recompute_gate
from fleetgraph_core.persistence.retrieval_projection_builder import (
    build_retrieval_projection,
)


_TOP_LEVEL_KEYS: tuple[str, ...] = (
    "recompute_gate_input",
    "canonical_store_input",
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


def build_runtime_retrieval_orchestration(
    orchestrator_input: dict[str, Any],
) -> dict[str, Any]:
    """Execute deterministic retrieval orchestration via delegated modules."""
    if not isinstance(orchestrator_input, dict):
        raise TypeError("orchestrator_input must be a dict")

    _validate_top_level_schema(orchestrator_input)

    recompute_gate_input = _require_dict(
        orchestrator_input["recompute_gate_input"],
        "recompute_gate_input",
    )
    canonical_store_input = _require_dict(
        orchestrator_input["canonical_store_input"],
        "canonical_store_input",
    )

    recompute_decision = evaluate_recompute_gate(deepcopy(recompute_gate_input))
    retrieval_projection = build_retrieval_projection(deepcopy(canonical_store_input))

    decision = recompute_decision["decision"]
    if decision == "reuse_stored_artifacts":
        path = "reuse_stored_artifacts"
    elif decision == "recompute_required":
        path = "recompute_required"
    else:  # pragma: no cover
        raise RuntimeError(f"Unsupported recompute decision: {decision}")

    return {
        "status": "completed",
        "stage": "complete",
        "reasons": ["retrieval_pipeline_completed"],
        "result": {
            "recompute_decision": recompute_decision,
            "retrieval_projection": retrieval_projection,
            "path": path,
        },
    }