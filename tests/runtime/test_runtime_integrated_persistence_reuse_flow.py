"""FG-W18-P18-MB3: Deterministic integration test for compute-once / retrieve-many.

Proves that:
1. The ingest path produces manifest, canonical store, and retrieval projection.
2. A retrieval with matching state selects "reuse_stored_artifacts".
3. A retrieval with changed state selects "recompute_required".
4. A retrieval with force_reprocess=True selects "recompute_required".
"""

from __future__ import annotations

from copy import deepcopy

from fleetgraph_core.persistence.canonical_artifact_store import (
    build_canonical_artifact_store,
)
from fleetgraph_core.persistence.ingestion_artifact_manifest import (
    build_ingestion_artifact_manifest,
)
from fleetgraph_core.persistence.retrieval_projection_builder import (
    build_retrieval_projection,
)
from fleetgraph_core.persistence.recompute_gate import evaluate_recompute_gate
from fleetgraph_core.runtime.runtime_ingest_orchestrator import (
    build_runtime_ingest_orchestration,
)
from fleetgraph_core.runtime.runtime_retrieval_orchestrator import (
    build_runtime_retrieval_orchestration,
)


# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------

def _ingest_orchestrator_input() -> dict[str, object]:
    return {
        "batch_input": {
            "batch_id": "batch-001",
            "documents": [
                {"document_id": "doc-001", "content": "Alpha document"},
                {"document_id": "doc-002", "content": "Beta document"},
            ],
        },
        "manifest_input": {
            "matter_id": "matter-001",
            "document_set_version": "v2026.03.24",
            "ingestion_run_id": "run-abc123",
            "pipeline_version": "pipeline-v1",
            "schema_version": "schema-v1",
            "source_hash": "sha256:deadbeef",
            "artifact_keys": [
                "artifacts/entities.json",
                "artifacts/events.json",
                "artifacts/graph.json",
            ],
        },
        "artifact_input": {
            "manifest": {
                "matter_id": "matter-001",
                "document_set_version": "v2026.03.24",
                "ingestion_run_id": "run-abc123",
                "pipeline_version": "pipeline-v1",
                "schema_version": "schema-v1",
                "source_hash": "sha256:deadbeef",
                "artifact_keys": [
                    "artifacts/entities.json",
                    "artifacts/events.json",
                    "artifacts/graph.json",
                ],
            },
            "artifacts": {
                "entities": [
                    {"entity_id": "e2", "name": "Beta Corp"},
                    {"entity_id": "e1", "name": "Alpha Inc"},
                ],
                "events": [
                    {"event_id": "ev2", "sequence": 2},
                    {"event_id": "ev1", "sequence": 1},
                ],
                "relationships": [
                    {"source": "e1", "target": "e2", "type": "partnered_with"},
                    {"source": "e2", "target": "e1", "type": "partnered_with"},
                ],
                "evidence_links": [{"event_id": "ev1", "doc_id": "doc-001"}],
                "graph_artifacts": [{"node_count": 2, "edge_count": 1}],
            },
        },
    }


def _retrieval_orchestrator_input(
    *,
    document_set_version: str = "v2026.03.24",
    pipeline_version: str = "pipeline-v1",
    schema_version: str = "schema-v1",
    source_hash: str = "sha256:deadbeef",
    force_reprocess: bool = False,
) -> dict[str, object]:
    """Build retrieval orchestrator input whose stored manifest matches ingest output."""
    stored_manifest = {
        "matter_id": "matter-001",
        "document_set_version": "v2026.03.24",
        "ingestion_run_id": "run-abc123",
        "pipeline_version": "pipeline-v1",
        "schema_version": "schema-v1",
        "source_hash": "sha256:deadbeef",
        "artifact_keys": [
            "artifacts/entities.json",
            "artifacts/events.json",
            "artifacts/graph.json",
        ],
    }
    return {
        "recompute_gate_input": {
            "stored_manifest": stored_manifest,
            "requested_state": {
                "document_set_version": document_set_version,
                "pipeline_version": pipeline_version,
                "schema_version": schema_version,
                "source_hash": source_hash,
                "force_reprocess": force_reprocess,
            },
        },
        "canonical_store_input": {
            "manifest": stored_manifest,
            "artifacts": {
                "entities": [
                    {"entity_id": "e2", "name": "Beta Corp"},
                    {"entity_id": "e1", "name": "Alpha Inc"},
                ],
                "events": [
                    {"event_id": "ev2", "sequence": 2},
                    {"event_id": "ev1", "sequence": 1},
                ],
                "relationships": [
                    {"source": "e1", "target": "e2", "type": "partnered_with"},
                    {"source": "e2", "target": "e1", "type": "partnered_with"},
                ],
                "evidence_links": [{"event_id": "ev1", "doc_id": "doc-001"}],
                "graph_artifacts": [{"node_count": 2, "edge_count": 1}],
            },
        },
    }


# ---------------------------------------------------------------------------
# 1. Ingest path output contract
# ---------------------------------------------------------------------------

class TestIngestPathProducesExpectedOutputs:
    def test_ingest_produces_manifest(self) -> None:
        result = build_runtime_ingest_orchestration(_ingest_orchestrator_input())

        assert result["status"] == "completed"
        assert "manifest" in result["result"]
        manifest = result["result"]["manifest"]
        assert manifest["matter_id"] == "matter-001"
        assert manifest["document_set_version"] == "v2026.03.24"
        assert manifest["source_hash"] == "sha256:deadbeef"

    def test_ingest_produces_canonical_store(self) -> None:
        result = build_runtime_ingest_orchestration(_ingest_orchestrator_input())

        canonical_store = result["result"]["canonical_store"]
        assert "manifest" in canonical_store
        assert "artifacts" in canonical_store
        assert isinstance(canonical_store["artifacts"]["entities"], list)
        assert isinstance(canonical_store["artifacts"]["events"], list)
        assert isinstance(canonical_store["artifacts"]["relationships"], list)

    def test_ingest_produces_retrieval_projection(self) -> None:
        result = build_runtime_ingest_orchestration(_ingest_orchestrator_input())

        projection = result["result"]["retrieval_projection"]
        assert "projections" in projection
        assert "entity_index" in projection["projections"]
        assert "event_timeline" in projection["projections"]
        assert "relationship_index" in projection["projections"]

    def test_ingest_exact_top_level_result_key_order(self) -> None:
        result = build_runtime_ingest_orchestration(_ingest_orchestrator_input())

        assert tuple(result.keys()) == ("status", "stage", "reasons", "result")
        assert tuple(result["result"].keys()) == (
            "batch_envelope",
            "manifest",
            "canonical_store",
            "retrieval_projection",
        )

    def test_ingest_manifest_key_order(self) -> None:
        result = build_runtime_ingest_orchestration(_ingest_orchestrator_input())

        manifest = result["result"]["manifest"]
        assert tuple(manifest.keys()) == (
            "matter_id",
            "document_set_version",
            "ingestion_run_id",
            "pipeline_version",
            "schema_version",
            "source_hash",
            "artifact_keys",
        )

    def test_ingest_retrieval_projection_key_order(self) -> None:
        result = build_runtime_ingest_orchestration(_ingest_orchestrator_input())

        projection = result["result"]["retrieval_projection"]
        assert tuple(projection.keys()) == ("manifest", "projections")
        assert tuple(projection["projections"].keys()) == (
            "entity_index",
            "event_timeline",
            "relationship_index",
        )


# ---------------------------------------------------------------------------
# 2. Retrieval reuse path
# ---------------------------------------------------------------------------

class TestRetrievalReusePath:
    def test_matching_state_selects_reuse_stored_artifacts(self) -> None:
        retrieval_result = build_runtime_retrieval_orchestration(
            _retrieval_orchestrator_input()
        )

        assert retrieval_result["result"]["path"] == "reuse_stored_artifacts"

    def test_reuse_path_recompute_decision_reasons(self) -> None:
        retrieval_result = build_runtime_retrieval_orchestration(
            _retrieval_orchestrator_input()
        )

        decision = retrieval_result["result"]["recompute_decision"]
        assert decision["decision"] == "reuse_stored_artifacts"
        assert decision["reasons"] == ["stored_artifacts_valid"]

    def test_reuse_path_retrieval_result_key_order(self) -> None:
        retrieval_result = build_runtime_retrieval_orchestration(
            _retrieval_orchestrator_input()
        )

        assert tuple(retrieval_result.keys()) == ("status", "stage", "reasons", "result")
        assert tuple(retrieval_result["result"].keys()) == (
            "recompute_decision",
            "retrieval_projection",
            "path",
        )


# ---------------------------------------------------------------------------
# 3. Retrieval recompute paths
# ---------------------------------------------------------------------------

class TestRetrievalRecomputePaths:
    def test_changed_document_set_version_triggers_recompute(self) -> None:
        retrieval_result = build_runtime_retrieval_orchestration(
            _retrieval_orchestrator_input(document_set_version="v2026.03.25")
        )

        assert retrieval_result["result"]["path"] == "recompute_required"
        reasons = retrieval_result["result"]["recompute_decision"]["reasons"]
        assert "document_set_version_changed" in reasons

    def test_changed_pipeline_version_triggers_recompute(self) -> None:
        retrieval_result = build_runtime_retrieval_orchestration(
            _retrieval_orchestrator_input(pipeline_version="pipeline-v2")
        )

        assert retrieval_result["result"]["path"] == "recompute_required"
        reasons = retrieval_result["result"]["recompute_decision"]["reasons"]
        assert "pipeline_version_changed" in reasons

    def test_changed_schema_version_triggers_recompute(self) -> None:
        retrieval_result = build_runtime_retrieval_orchestration(
            _retrieval_orchestrator_input(schema_version="schema-v2")
        )

        assert retrieval_result["result"]["path"] == "recompute_required"
        reasons = retrieval_result["result"]["recompute_decision"]["reasons"]
        assert "schema_version_changed" in reasons

    def test_changed_source_hash_triggers_recompute(self) -> None:
        retrieval_result = build_runtime_retrieval_orchestration(
            _retrieval_orchestrator_input(source_hash="sha256:cafebabe")
        )

        assert retrieval_result["result"]["path"] == "recompute_required"
        reasons = retrieval_result["result"]["recompute_decision"]["reasons"]
        assert "source_hash_changed" in reasons

    def test_force_reprocess_triggers_recompute(self) -> None:
        retrieval_result = build_runtime_retrieval_orchestration(
            _retrieval_orchestrator_input(force_reprocess=True)
        )

        assert retrieval_result["result"]["path"] == "recompute_required"
        reasons = retrieval_result["result"]["recompute_decision"]["reasons"]
        assert "force_reprocess_requested" in reasons


# ---------------------------------------------------------------------------
# 4. Integrated ingest → retrieval pipeline wiring
# ---------------------------------------------------------------------------

class TestIntegratedIngestRetrievalWiring:
    def test_ingest_manifest_usable_as_retrieval_stored_manifest(self) -> None:
        """Manifest produced by ingest can be passed directly as stored_manifest
        to the retrieval recompute gate without transformation."""
        ingest_result = build_runtime_ingest_orchestration(_ingest_orchestrator_input())
        ingested_manifest = ingest_result["result"]["manifest"]

        # Build retrieval gate input using the ingested manifest
        gate_input = {
            "stored_manifest": ingested_manifest,
            "requested_state": {
                "document_set_version": ingested_manifest["document_set_version"],
                "pipeline_version": ingested_manifest["pipeline_version"],
                "schema_version": ingested_manifest["schema_version"],
                "source_hash": ingested_manifest["source_hash"],
                "force_reprocess": False,
            },
        }
        gate_result = evaluate_recompute_gate(gate_input)

        assert gate_result["decision"] == "reuse_stored_artifacts"

    def test_ingest_canonical_store_usable_by_retrieval_projection_builder(self) -> None:
        """Canonical store from ingest output can be fed directly into
        build_retrieval_projection without transformation."""
        ingest_result = build_runtime_ingest_orchestration(_ingest_orchestrator_input())
        canonical_store = ingest_result["result"]["canonical_store"]

        # The retrieval projection builder expects the canonical store format
        projection = build_retrieval_projection(canonical_store)

        assert "projections" in projection
        assert isinstance(projection["projections"]["entity_index"], list)

    def test_ingest_then_retrieval_reuse_full_path(self) -> None:
        """Full compute-once / retrieve-many: ingest then reuse retrieval."""
        ingest_result = build_runtime_ingest_orchestration(_ingest_orchestrator_input())
        ingested_manifest = ingest_result["result"]["manifest"]
        canonical_store = ingest_result["result"]["canonical_store"]

        retrieval_result = build_runtime_retrieval_orchestration(
            {
                "recompute_gate_input": {
                    "stored_manifest": ingested_manifest,
                    "requested_state": {
                        "document_set_version": ingested_manifest["document_set_version"],
                        "pipeline_version": ingested_manifest["pipeline_version"],
                        "schema_version": ingested_manifest["schema_version"],
                        "source_hash": ingested_manifest["source_hash"],
                        "force_reprocess": False,
                    },
                },
                "canonical_store_input": canonical_store,
            }
        )

        assert retrieval_result["status"] == "completed"
        assert retrieval_result["result"]["path"] == "reuse_stored_artifacts"

    def test_ingest_then_retrieval_recompute_full_path(self) -> None:
        """Full compute-once / retrieve-many: ingest then force recompute."""
        ingest_result = build_runtime_ingest_orchestration(_ingest_orchestrator_input())
        ingested_manifest = ingest_result["result"]["manifest"]
        canonical_store = ingest_result["result"]["canonical_store"]

        retrieval_result = build_runtime_retrieval_orchestration(
            {
                "recompute_gate_input": {
                    "stored_manifest": ingested_manifest,
                    "requested_state": {
                        "document_set_version": ingested_manifest["document_set_version"],
                        "pipeline_version": ingested_manifest["pipeline_version"],
                        "schema_version": ingested_manifest["schema_version"],
                        "source_hash": ingested_manifest["source_hash"],
                        "force_reprocess": True,
                    },
                },
                "canonical_store_input": canonical_store,
            }
        )

        assert retrieval_result["status"] == "completed"
        assert retrieval_result["result"]["path"] == "recompute_required"


# ---------------------------------------------------------------------------
# 5. Determinism and immutability
# ---------------------------------------------------------------------------

class TestDeterminismAndImmutability:
    def test_ingest_deterministic_repeated_calls(self) -> None:
        payload = _ingest_orchestrator_input()

        first = build_runtime_ingest_orchestration(payload)
        second = build_runtime_ingest_orchestration(payload)

        assert first == second

    def test_retrieval_reuse_deterministic_repeated_calls(self) -> None:
        payload = _retrieval_orchestrator_input()

        first = build_runtime_retrieval_orchestration(payload)
        second = build_runtime_retrieval_orchestration(payload)

        assert first == second

    def test_retrieval_recompute_deterministic_repeated_calls(self) -> None:
        payload = _retrieval_orchestrator_input(force_reprocess=True)

        first = build_runtime_retrieval_orchestration(payload)
        second = build_runtime_retrieval_orchestration(payload)

        assert first == second

    def test_ingest_input_not_mutated(self) -> None:
        payload = _ingest_orchestrator_input()
        before = deepcopy(payload)

        build_runtime_ingest_orchestration(payload)

        assert payload == before

    def test_retrieval_input_not_mutated(self) -> None:
        payload = _retrieval_orchestrator_input()
        before = deepcopy(payload)

        build_runtime_retrieval_orchestration(payload)

        assert payload == before
