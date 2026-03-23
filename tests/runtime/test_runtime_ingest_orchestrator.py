from __future__ import annotations

from copy import deepcopy

import pytest

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
from fleetgraph_core.runtime.runtime_ingest_orchestrator import (
    build_runtime_ingest_orchestration,
)


def _valid_orchestrator_input() -> dict[str, object]:
    return {
        "batch_input": {
            "batch_id": "batch-001",
            "documents": [
                {
                    "document_id": "doc-001",
                    "content": "First document",
                },
                {
                    "document_id": "doc-002",
                    "content": "Second document",
                },
            ],
        },
        "manifest_input": {
            "matter_id": "matter-001",
            "document_set_version": "v1",
            "ingestion_run_id": "run-001",
            "pipeline_version": "p1",
            "schema_version": "s1",
            "source_hash": "abc123",
            "artifact_keys": ["relationships", "entities", "events"],
        },
        "artifact_input": {
            "manifest": {
                "matter_id": "matter-001",
                "document_set_version": "v1",
                "ingestion_run_id": "run-001",
                "pipeline_version": "p1",
                "schema_version": "s1",
                "source_hash": "abc123",
                "artifact_keys": ["relationships", "entities", "events"],
            },
            "artifacts": {
                "entities": [{"id": "ent-2"}, {"id": "ent-1"}],
                "events": [{"id": "evt-2"}, {"id": "evt-1"}],
                "relationships": [{"id": "rel-2"}, {"id": "rel-1"}],
                "evidence_links": [{"id": "evl-1"}],
                "graph_artifacts": [{"id": "gph-1"}],
            },
        },
    }


def test_build_runtime_ingest_orchestration_successful_path() -> None:
    payload = _valid_orchestrator_input()

    result = build_runtime_ingest_orchestration(payload)

    assert result["status"] == "completed"
    assert result["stage"] == "complete"
    assert result["reasons"] == ["ingest_pipeline_completed"]


def test_build_runtime_ingest_orchestration_exact_output_key_order() -> None:
    payload = _valid_orchestrator_input()

    result = build_runtime_ingest_orchestration(payload)

    assert tuple(result.keys()) == ("status", "stage", "reasons", "result")
    assert tuple(result["result"].keys()) == (
        "batch_envelope",
        "manifest",
        "canonical_store",
        "retrieval_projection",
    )


def test_build_runtime_ingest_orchestration_deterministic_repeated_calls() -> None:
    payload = _valid_orchestrator_input()

    first = build_runtime_ingest_orchestration(payload)
    second = build_runtime_ingest_orchestration(payload)

    assert first == second


def test_build_runtime_ingest_orchestration_input_immutability() -> None:
    payload = _valid_orchestrator_input()
    before = deepcopy(payload)

    build_runtime_ingest_orchestration(payload)

    assert payload == before


def test_build_runtime_ingest_orchestration_preserves_delegated_outputs() -> None:
    payload = _valid_orchestrator_input()

    result = build_runtime_ingest_orchestration(payload)

    expected_batch = build_batch_ingestion_envelope(deepcopy(payload["batch_input"]))
    expected_manifest = build_ingestion_artifact_manifest(deepcopy(payload["manifest_input"]))
    expected_store = build_canonical_artifact_store(deepcopy(payload["artifact_input"]))
    expected_projection = build_retrieval_projection(deepcopy(payload["artifact_input"]))

    assert result["result"]["batch_envelope"] == expected_batch
    assert result["result"]["manifest"] == expected_manifest
    assert result["result"]["canonical_store"] == expected_store
    assert result["result"]["retrieval_projection"] == expected_projection


def test_build_runtime_ingest_orchestration_stage_and_status_values_exact() -> None:
    payload = _valid_orchestrator_input()

    result = build_runtime_ingest_orchestration(payload)

    assert result["stage"] == "complete"
    assert result["status"] == "completed"


def test_build_runtime_ingest_orchestration_rejects_missing_top_level_keys() -> None:
    payload = _valid_orchestrator_input()
    del payload["artifact_input"]

    with pytest.raises(ValueError, match="missing required fields"):
        build_runtime_ingest_orchestration(payload)


def test_build_runtime_ingest_orchestration_rejects_extra_top_level_keys() -> None:
    payload = _valid_orchestrator_input()
    payload["unexpected"] = {}

    with pytest.raises(ValueError, match="unexpected fields"):
        build_runtime_ingest_orchestration(payload)


@pytest.mark.parametrize(
    "field,bad_value",
    [
        ("batch_input", []),
        ("manifest_input", "bad"),
        ("artifact_input", 123),
    ],
)
def test_build_runtime_ingest_orchestration_rejects_non_dict_top_level_values(
    field: str,
    bad_value: object,
) -> None:
    payload = _valid_orchestrator_input()
    payload[field] = bad_value

    with pytest.raises(ValueError, match=f"field '{field}'.*dict"):
        build_runtime_ingest_orchestration(payload)
