"""
Test suite for P13A-MB2 canonical artifact store builder.
"""

from __future__ import annotations

from copy import deepcopy

import pytest

from fleetgraph_core.persistence.canonical_artifact_store import (
    build_canonical_artifact_store,
)


def make_valid_store_input() -> dict:
    return {
        "manifest": {
            "matter_id": "matter_001",
            "document_set_version": "v2026.03.23",
            "ingestion_run_id": "ingest_run_abc123",
            "pipeline_version": "pipeline_v1",
            "schema_version": "schema_v1",
            "source_hash": "sha256:abc123",
            "artifact_keys": [
                "artifacts/events/events.json",
                "artifacts/entities/entities.json",
                "artifacts/graph/graph.json",
            ],
        },
        "artifacts": {
            "entities": [{"entity_id": "e1"}],
            "events": [{"event_id": "ev1"}],
            "relationships": [{"source": "e1", "target": "e2"}],
            "evidence_links": [{"event_id": "ev1", "doc_id": "d1"}],
            "graph_artifacts": [{"node_count": 2, "edge_count": 1}],
        },
    }


class TestValidCanonicalStoreBuild:
    def test_valid_canonical_store_build(self) -> None:
        result = build_canonical_artifact_store(make_valid_store_input())

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
            "artifacts": {
                "entities": [{"entity_id": "e1"}],
                "events": [{"event_id": "ev1"}],
                "relationships": [{"source": "e1", "target": "e2"}],
                "evidence_links": [{"event_id": "ev1", "doc_id": "d1"}],
                "graph_artifacts": [{"node_count": 2, "edge_count": 1}],
            },
        }

    def test_exact_key_order_top_level_and_nested(self) -> None:
        result = build_canonical_artifact_store(make_valid_store_input())

        assert tuple(result.keys()) == ("manifest", "artifacts")
        assert tuple(result["manifest"].keys()) == (
            "matter_id",
            "document_set_version",
            "ingestion_run_id",
            "pipeline_version",
            "schema_version",
            "source_hash",
            "artifact_keys",
        )
        assert tuple(result["artifacts"].keys()) == (
            "entities",
            "events",
            "relationships",
            "evidence_links",
            "graph_artifacts",
        )

    def test_manifest_passthrough_correctness(self) -> None:
        payload = make_valid_store_input()

        result = build_canonical_artifact_store(payload)

        assert result["manifest"]["matter_id"] == payload["manifest"]["matter_id"]
        assert result["manifest"]["document_set_version"] == payload["manifest"][
            "document_set_version"
        ]
        assert result["manifest"]["ingestion_run_id"] == payload["manifest"][
            "ingestion_run_id"
        ]
        assert result["manifest"]["pipeline_version"] == payload["manifest"][
            "pipeline_version"
        ]
        assert result["manifest"]["schema_version"] == payload["manifest"]["schema_version"]
        assert result["manifest"]["source_hash"] == payload["manifest"]["source_hash"]
        assert result["manifest"]["artifact_keys"] == sorted(
            payload["manifest"]["artifact_keys"]
        )


class TestDeterminism:
    def test_deterministic_repeated_calls(self) -> None:
        payload = make_valid_store_input()

        first = build_canonical_artifact_store(payload)
        second = build_canonical_artifact_store(payload)

        assert first == second


class TestInputImmutability:
    def test_input_not_mutated(self) -> None:
        payload = make_valid_store_input()
        before = deepcopy(payload)

        build_canonical_artifact_store(payload)

        assert payload == before


class TestMalformedInputRejection:
    def test_non_dict_input_rejected(self) -> None:
        with pytest.raises(TypeError, match="store_input must be a dict"):
            build_canonical_artifact_store("not a dict")  # type: ignore[arg-type]

    def test_missing_top_level_keys_rejected(self) -> None:
        payload = make_valid_store_input()
        del payload["artifacts"]

        with pytest.raises(ValueError, match="missing required fields"):
            build_canonical_artifact_store(payload)

    def test_extra_top_level_keys_rejected(self) -> None:
        payload = make_valid_store_input()
        payload["unexpected"] = "x"

        with pytest.raises(ValueError, match="unexpected fields"):
            build_canonical_artifact_store(payload)

    def test_manifest_container_not_dict_rejected(self) -> None:
        payload = make_valid_store_input()
        payload["manifest"] = "not a dict"

        with pytest.raises(TypeError, match="field 'manifest'.*type dict"):
            build_canonical_artifact_store(payload)

    def test_artifact_container_not_dict_rejected(self) -> None:
        payload = make_valid_store_input()
        payload["artifacts"] = "not a dict"

        with pytest.raises(TypeError, match="field 'artifacts'.*type dict"):
            build_canonical_artifact_store(payload)

    def test_malformed_manifest_rejected(self) -> None:
        payload = make_valid_store_input()
        del payload["manifest"]["source_hash"]

        with pytest.raises(ValueError, match="manifest_input is missing required fields"):
            build_canonical_artifact_store(payload)

    def test_missing_artifact_buckets_rejected(self) -> None:
        payload = make_valid_store_input()
        del payload["artifacts"]["events"]

        with pytest.raises(ValueError, match="missing required buckets"):
            build_canonical_artifact_store(payload)

    def test_extra_artifact_buckets_rejected(self) -> None:
        payload = make_valid_store_input()
        payload["artifacts"]["extra_bucket"] = []

        with pytest.raises(ValueError, match="unexpected buckets"):
            build_canonical_artifact_store(payload)


class TestArtifactBucketTypeEnforcement:
    @pytest.mark.parametrize(
        "bucket",
        [
            "entities",
            "events",
            "relationships",
            "evidence_links",
            "graph_artifacts",
        ],
    )
    def test_artifact_bucket_not_list_rejected(self, bucket: str) -> None:
        payload = make_valid_store_input()
        payload["artifacts"][bucket] = "not a list"

        with pytest.raises(TypeError, match=f"artifacts\\.{bucket}.*type list"):
            build_canonical_artifact_store(payload)
