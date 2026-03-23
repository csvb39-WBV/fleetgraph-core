from __future__ import annotations

from copy import deepcopy
from typing import Any

from fleetgraph_core.ingestion.batch_ingestion_envelope import (
    build_batch_ingestion_envelope,
)
from fleetgraph_core.persistence.canonical_artifact_store import (
    build_canonical_artifact_store,
)
from fleetgraph_core.persistence.ingestion_artifact_manifest import (
    build_ingestion_artifact_manifest,
)
from fleetgraph_core.persistence.retrieval_projection_builder import (
    build_retrieval_projection,
)


_REQUIRED_INPUT_KEYS: tuple[str, ...] = (
    "batch_input",
    "manifest_input",
    "artifact_input",
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


def _require_dict_field(value: object, field_name: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ValueError(f"orchestrator_input field '{field_name}' must be a dict")
    return value


def build_runtime_ingest_orchestration(
    orchestrator_input: dict[str, Any],
) -> dict[str, Any]:
    if not isinstance(orchestrator_input, dict):
        raise ValueError("orchestrator_input must be a dict")

    _validate_closed_schema(orchestrator_input)

    batch_input = _require_dict_field(orchestrator_input["batch_input"], "batch_input")
    manifest_input = _require_dict_field(
        orchestrator_input["manifest_input"],
        "manifest_input",
    )
    artifact_input = _require_dict_field(
        orchestrator_input["artifact_input"],
        "artifact_input",
    )

    batch_envelope = build_batch_ingestion_envelope(deepcopy(batch_input))
    manifest = build_ingestion_artifact_manifest(deepcopy(manifest_input))
    canonical_store = build_canonical_artifact_store(deepcopy(artifact_input))
    retrieval_projection = build_retrieval_projection(deepcopy(artifact_input))

    return {
        "status": "completed",
        "stage": "complete",
        "reasons": ["ingest_pipeline_completed"],
        "result": {
            "batch_envelope": batch_envelope,
            "manifest": manifest,
            "canonical_store": canonical_store,
            "retrieval_projection": retrieval_projection,
        },
    }