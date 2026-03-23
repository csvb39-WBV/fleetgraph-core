"""Test suite for FG-W17-P17-MB7 runtime retrieval orchestrator."""

from __future__ import annotations

from copy import deepcopy

import pytest

from fleetgraph_core.persistence.recompute_gate import evaluate_recompute_gate
from fleetgraph_core.persistence.retrieval_projection_builder import (
    build_retrieval_projection,
)
from fleetgraph_core.runtime.runtime_retrieval_orchestrator import (
    build_runtime_retrieval_orchestration,
)


def make_valid_input() -> dict[str, object]:
    return {
        "recompute_gate_input": {
            "stored_manifest": {
                "matter_id": "matter_001",
                "document_set_version": "v2026.03.23",
                "ingestion_run_id": "ingest_run_abc123",
                "pipeline_version": "pipeline_v1",
                "schema_version": "schema_v1",
                "source_hash": "sha256:abc123",
                "artifact_keys": [
                    "artifacts/entities/entities.json",
                    "artifacts/events/events.json",
                    "artifacts/graph/graph.json",
                ],
            },
            "requested_state": {
                "document_set_version": "v2026.03.23",
                "pipeline_version": "pipeline_v1",
                "schema_version": "schema_v1",
                "source_hash": "sha256:abc123",
                "force_reprocess": False,
            },
        },
        "canonical_store_input": {
            "manifest": {
                "matter_id": "matter_001",
                "document_set_version": "v2026.03.23",
                "ingestion_run_id": "ingest_run_abc123",
                "pipeline_version": "pipeline_v1",
                "schema_version": "schema_v1",
                "source_hash": "sha256:abc123",
                "artifact_keys": [
                    "artifacts/entities/entities.json",
                    "artifacts/events/events.json",
                    "artifacts/graph/graph.json",
                ],
            },
            "artifacts": {
                "entities": [
                    {"entity_id": "e2", "name": "Beta"},
                    {"entity_id": "e1", "name": "Alpha"},
                ],
                "events": [
                    {"event_id": "ev2", "sequence": 2},
                    {"event_id": "ev1", "sequence": 1},
                ],
                "relationships": [
                    {"source": "e2", "target": "e1", "type": "related_to"},
                    {"source": "e1", "target": "e2", "type": "related_to"},
                ],
                "evidence_links": [{"event_id": "ev1", "doc_id": "d1"}],
                "graph_artifacts": [{"node_count": 2, "edge_count": 1}],
            },
        },
    }


class TestSuccessfulPaths:
    def test_successful_reuse_stored_artifacts_path(self) -> None:
        payload = make_valid_input()

        result = build_runtime_retrieval_orchestration(payload)

        assert result["status"] == "completed"
        assert result["stage"] == "complete"
        assert result["reasons"] == ["retrieval_pipeline_completed"]
        assert result["result"]["path"] == "reuse_stored_artifacts"

    def test_successful_recompute_required_path(self) -> None:
        payload = make_valid_input()
        payload["recompute_gate_input"]["requested_state"]["force_reprocess"] = True

        result = build_runtime_retrieval_orchestration(payload)

        assert result["status"] == "completed"
        assert result["stage"] == "complete"
        assert result["reasons"] == ["retrieval_pipeline_completed"]
        assert result["result"]["path"] == "recompute_required"


class TestDelegationAndOutputContract:
    def test_delegated_outputs_preserved_exactly_in_result(self) -> None:
        payload = make_valid_input()

        result = build_runtime_retrieval_orchestration(payload)
        expected_recompute = evaluate_recompute_gate(payload["recompute_gate_input"])
        expected_projection = build_retrieval_projection(payload["canonical_store_input"])

        assert result["result"]["recompute_decision"] == expected_recompute
        assert result["result"]["retrieval_projection"] == expected_projection

    def test_exact_output_key_order_top_level_and_result_level(self) -> None:
        payload = make_valid_input()

        result = build_runtime_retrieval_orchestration(payload)

        assert tuple(result.keys()) == ("status", "stage", "reasons", "result")
        assert tuple(result["result"].keys()) == (
            "recompute_decision",
            "retrieval_projection",
            "path",
        )

    def test_status_is_exactly_completed(self) -> None:
        result = build_runtime_retrieval_orchestration(make_valid_input())

        assert result["status"] == "completed"

    def test_stage_is_exactly_complete(self) -> None:
        result = build_runtime_retrieval_orchestration(make_valid_input())

        assert result["stage"] == "complete"

    def test_path_matches_recompute_gate_decision(self) -> None:
        payload = make_valid_input()
        expected_decision = evaluate_recompute_gate(payload["recompute_gate_input"])["decision"]

        result = build_runtime_retrieval_orchestration(payload)

        assert result["result"]["path"] == expected_decision


class TestDeterminismAndImmutability:
    def test_deterministic_repeated_calls(self) -> None:
        payload = make_valid_input()

        first = build_runtime_retrieval_orchestration(payload)
        second = build_runtime_retrieval_orchestration(payload)

        assert first == second

    def test_input_not_mutated(self) -> None:
        payload = make_valid_input()
        before = deepcopy(payload)

        build_runtime_retrieval_orchestration(payload)

        assert payload == before


class TestMalformedTopLevelInputRejection:
    def test_missing_top_level_keys_rejected(self) -> None:
        payload = make_valid_input()
        del payload["canonical_store_input"]

        with pytest.raises(ValueError, match="missing required fields"):
            build_runtime_retrieval_orchestration(payload)

    def test_extra_top_level_keys_rejected(self) -> None:
        payload = make_valid_input()
        payload["unexpected"] = {}

        with pytest.raises(ValueError, match="unexpected fields"):
            build_runtime_retrieval_orchestration(payload)

    @pytest.mark.parametrize(
        "field,bad_value",
        [
            ("recompute_gate_input", []),
            ("canonical_store_input", "bad"),
        ],
    )
    def test_top_level_inputs_must_be_dicts(
        self,
        field: str,
        bad_value: object,
    ) -> None:
        payload = make_valid_input()
        payload[field] = bad_value

        with pytest.raises(TypeError, match=f"field '{field}'.*dict"):
            build_runtime_retrieval_orchestration(payload)