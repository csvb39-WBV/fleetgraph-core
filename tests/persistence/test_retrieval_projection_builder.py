"""
Test suite for P13A-MB3 retrieval projection builder.
"""

from __future__ import annotations

from copy import deepcopy

import pytest

import fleetgraph_core.persistence.retrieval_projection_builder as retrieval_projection_builder
from fleetgraph_core.persistence.retrieval_projection_builder import (
    build_retrieval_projection,
)


def make_valid_canonical_store_input() -> dict:
    return {
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
    }


class TestValidProjectionBuild:
    def test_valid_projection_build(self) -> None:
        result = build_retrieval_projection(make_valid_canonical_store_input())

        assert result == {
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
            "projections": {
                "entity_index": [
                    {"entity_id": "e1", "name": "Alpha"},
                    {"entity_id": "e2", "name": "Beta"},
                ],
                "event_timeline": [
                    {"event_id": "ev1", "sequence": 1},
                    {"event_id": "ev2", "sequence": 2},
                ],
                "relationship_index": [
                    {"source": "e1", "target": "e2", "type": "related_to"},
                    {"source": "e2", "target": "e1", "type": "related_to"},
                ],
            },
        }

    def test_exact_key_order(self) -> None:
        result = build_retrieval_projection(make_valid_canonical_store_input())

        assert tuple(result.keys()) == ("manifest", "projections")
        assert tuple(result["manifest"].keys()) == (
            "matter_id",
            "document_set_version",
            "ingestion_run_id",
            "pipeline_version",
            "schema_version",
            "source_hash",
            "artifact_keys",
        )
        assert tuple(result["projections"].keys()) == (
            "entity_index",
            "event_timeline",
            "relationship_index",
        )

    def test_deterministic_ordering_of_projections(self) -> None:
        result = build_retrieval_projection(make_valid_canonical_store_input())

        assert result["projections"]["entity_index"] == [
            {"entity_id": "e1", "name": "Alpha"},
            {"entity_id": "e2", "name": "Beta"},
        ]
        assert result["projections"]["event_timeline"] == [
            {"event_id": "ev1", "sequence": 1},
            {"event_id": "ev2", "sequence": 2},
        ]

    def test_manifest_passed_through_unchanged(self) -> None:
        payload = make_valid_canonical_store_input()

        result = build_retrieval_projection(payload)

        assert result["manifest"] == payload["manifest"]


class TestDeterminism:
    def test_deterministic_repeated_calls(self) -> None:
        payload = make_valid_canonical_store_input()

        first = build_retrieval_projection(payload)
        second = build_retrieval_projection(payload)

        assert first == second


class TestInputImmutability:
    def test_input_not_mutated(self) -> None:
        payload = make_valid_canonical_store_input()
        before = deepcopy(payload)

        build_retrieval_projection(payload)

        assert payload == before


class TestMalformedInputRejection:
    def test_non_dict_input_rejected(self) -> None:
        with pytest.raises(TypeError, match="store_input must be a dict"):
            build_retrieval_projection("not a dict")  # type: ignore[arg-type]

    def test_missing_top_level_keys_rejected(self) -> None:
        payload = make_valid_canonical_store_input()
        del payload["manifest"]

        with pytest.raises(ValueError, match="missing required fields"):
            build_retrieval_projection(payload)

    def test_extra_top_level_keys_rejected(self) -> None:
        payload = make_valid_canonical_store_input()
        payload["extra"] = "x"

        with pytest.raises(ValueError, match="unexpected fields"):
            build_retrieval_projection(payload)

    def test_malformed_canonical_store_input_rejected(self) -> None:
        payload = make_valid_canonical_store_input()
        del payload["manifest"]["source_hash"]

        with pytest.raises(ValueError, match="manifest_input is missing required fields"):
            build_retrieval_projection(payload)

    def test_missing_artifact_buckets_rejected(self) -> None:
        payload = make_valid_canonical_store_input()
        del payload["artifacts"]["events"]

        with pytest.raises(ValueError, match="missing required buckets"):
            build_retrieval_projection(payload)


class TestProjectionListEnforcement:
    def test_projection_not_list_rejected(self, monkeypatch: pytest.MonkeyPatch) -> None:
        payload = make_valid_canonical_store_input()

        monkeypatch.setattr(
            retrieval_projection_builder,
            "_sorted_projection",
            lambda _items: tuple(),
        )

        with pytest.raises(RuntimeError, match="projection 'entity_index' must be a list"):
            build_retrieval_projection(payload)


class TestEmptyArtifactLists:
    def test_empty_artifact_lists_produce_valid_projections(self) -> None:
        payload = make_valid_canonical_store_input()
        payload["artifacts"] = {
            "entities": [],
            "events": [],
            "relationships": [],
            "evidence_links": [],
            "graph_artifacts": [],
        }

        result = build_retrieval_projection(payload)

        assert result == {
            "manifest": payload["manifest"],
            "projections": {
                "entity_index": [],
                "event_timeline": [],
                "relationship_index": [],
            },
        }
