"""
Test suite for P13A-MB1 ingestion artifact manifest builder.
"""

from __future__ import annotations

from copy import deepcopy

import pytest

from fleetgraph_core.persistence.ingestion_artifact_manifest import (
    build_ingestion_artifact_manifest,
)


def make_valid_manifest_input() -> dict:
    return {
        "matter_id": "matter_001",
        "document_set_version": "v2026.03.23",
        "ingestion_run_id": "ingest_run_abc123",
        "pipeline_version": "pipeline_v1",
        "schema_version": "schema_v1",
        "source_hash": "sha256:abc123",
        "artifact_keys": [
            "artifacts/normalized/doc_02.json",
            "artifacts/normalized/doc_01.json",
            "artifacts/summary/manifest.json",
        ],
    }


class TestValidManifestBuild:
    def test_valid_manifest_build(self) -> None:
        result = build_ingestion_artifact_manifest(make_valid_manifest_input())

        assert result == {
            "matter_id": "matter_001",
            "document_set_version": "v2026.03.23",
            "ingestion_run_id": "ingest_run_abc123",
            "pipeline_version": "pipeline_v1",
            "schema_version": "schema_v1",
            "source_hash": "sha256:abc123",
            "artifact_keys": [
                "artifacts/normalized/doc_01.json",
                "artifacts/normalized/doc_02.json",
                "artifacts/summary/manifest.json",
            ],
        }

    def test_sorted_artifact_keys(self) -> None:
        result = build_ingestion_artifact_manifest(make_valid_manifest_input())

        assert result["artifact_keys"] == sorted(result["artifact_keys"])

    def test_exact_key_order(self) -> None:
        result = build_ingestion_artifact_manifest(make_valid_manifest_input())

        assert tuple(result.keys()) == (
            "matter_id",
            "document_set_version",
            "ingestion_run_id",
            "pipeline_version",
            "schema_version",
            "source_hash",
            "artifact_keys",
        )


class TestDeterminism:
    def test_deterministic_repeated_calls(self) -> None:
        payload = make_valid_manifest_input()

        first = build_ingestion_artifact_manifest(payload)
        second = build_ingestion_artifact_manifest(payload)

        assert first == second


class TestInputImmutability:
    def test_input_not_mutated(self) -> None:
        payload = make_valid_manifest_input()
        before = deepcopy(payload)

        build_ingestion_artifact_manifest(payload)

        assert payload == before


class TestMalformedInputRejection:
    def test_non_dict_input_rejected(self) -> None:
        with pytest.raises(TypeError, match="manifest_input must be a dict"):
            build_ingestion_artifact_manifest("not a dict")  # type: ignore[arg-type]

    def test_missing_key_rejected(self) -> None:
        payload = make_valid_manifest_input()
        del payload["source_hash"]

        with pytest.raises(ValueError, match="missing required fields"):
            build_ingestion_artifact_manifest(payload)

    def test_extra_key_rejected(self) -> None:
        payload = make_valid_manifest_input()
        payload["unexpected"] = "x"

        with pytest.raises(ValueError, match="unexpected fields"):
            build_ingestion_artifact_manifest(payload)

    @pytest.mark.parametrize(
        "field,bad_value",
        [
            ("matter_id", 123),
            ("document_set_version", 123),
            ("ingestion_run_id", 123),
            ("pipeline_version", 123),
            ("schema_version", 123),
            ("source_hash", 123),
        ],
    )
    def test_non_string_scalar_fields_rejected(self, field: str, bad_value: object) -> None:
        payload = make_valid_manifest_input()
        payload[field] = bad_value

        with pytest.raises(TypeError, match=f"field '{field}'"):
            build_ingestion_artifact_manifest(payload)

    @pytest.mark.parametrize(
        "field",
        [
            "matter_id",
            "document_set_version",
            "ingestion_run_id",
            "pipeline_version",
            "schema_version",
            "source_hash",
        ],
    )
    def test_empty_string_scalar_fields_rejected(self, field: str) -> None:
        payload = make_valid_manifest_input()
        payload[field] = ""

        with pytest.raises(ValueError, match=f"field '{field}'.*non-empty string"):
            build_ingestion_artifact_manifest(payload)

    def test_artifact_keys_not_list_rejected(self) -> None:
        payload = make_valid_manifest_input()
        payload["artifact_keys"] = "not a list"

        with pytest.raises(TypeError, match="artifact_keys.*type list"):
            build_ingestion_artifact_manifest(payload)

    def test_empty_artifact_keys_rejected(self) -> None:
        payload = make_valid_manifest_input()
        payload["artifact_keys"] = []

        with pytest.raises(ValueError, match="artifact_keys.*non-empty list"):
            build_ingestion_artifact_manifest(payload)

    def test_non_string_artifact_key_entry_rejected(self) -> None:
        payload = make_valid_manifest_input()
        payload["artifact_keys"] = ["artifacts/a.json", 7]

        with pytest.raises(TypeError, match="entry at index 1"):
            build_ingestion_artifact_manifest(payload)

    def test_empty_string_artifact_key_entry_rejected(self) -> None:
        payload = make_valid_manifest_input()
        payload["artifact_keys"] = ["artifacts/a.json", ""]

        with pytest.raises(ValueError, match="entry at index 1.*non-empty string"):
            build_ingestion_artifact_manifest(payload)
