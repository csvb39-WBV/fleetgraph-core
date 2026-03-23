"""
Test suite for D13-MB5 Batch Ingestion Envelope Builder.
"""

from __future__ import annotations

from copy import deepcopy

import pytest

from fleetgraph_core.ingestion.batch_ingestion_envelope import (
    build_batch_ingestion_envelope,
)


def make_valid_input() -> dict:
    return {
        "batch_id": "batch_001",
        "documents": [
            {
                "document_id": "doc_001",
                "content": "alpha content",
            },
            {
                "document_id": "doc_002",
                "content": "beta content",
            },
            {
                "document_id": "doc_003",
                "content": "",
            },
        ],
    }


class TestValidBatchBuild:
    def test_valid_batch_build(self) -> None:
        result = build_batch_ingestion_envelope(make_valid_input())

        assert result == {
            "batch_id": "batch_001",
            "document_count": 3,
            "documents": [
                {
                    "document_id": "doc_001",
                    "content": "alpha content",
                },
                {
                    "document_id": "doc_002",
                    "content": "beta content",
                },
                {
                    "document_id": "doc_003",
                    "content": "",
                },
            ],
        }

    def test_exact_key_order(self) -> None:
        result = build_batch_ingestion_envelope(make_valid_input())

        assert tuple(result.keys()) == (
            "batch_id",
            "document_count",
            "documents",
        )
        for document in result["documents"]:
            assert tuple(document.keys()) == ("document_id", "content")

    def test_document_count_correctness(self) -> None:
        result = build_batch_ingestion_envelope(make_valid_input())

        assert result["document_count"] == len(result["documents"])

    def test_document_order_preserved(self) -> None:
        result = build_batch_ingestion_envelope(make_valid_input())

        assert [d["document_id"] for d in result["documents"]] == [
            "doc_001",
            "doc_002",
            "doc_003",
        ]


class TestDeterminism:
    def test_deterministic_repeated_calls(self) -> None:
        payload = make_valid_input()

        first = build_batch_ingestion_envelope(payload)
        second = build_batch_ingestion_envelope(payload)

        assert first == second


class TestInputImmutability:
    def test_input_not_mutated(self) -> None:
        payload = make_valid_input()
        before = deepcopy(payload)

        build_batch_ingestion_envelope(payload)

        assert payload == before


class TestMalformedInputRejection:
    def test_missing_keys_rejected(self) -> None:
        payload = make_valid_input()
        del payload["documents"]

        with pytest.raises(ValueError, match="missing required fields"):
            build_batch_ingestion_envelope(payload)

    def test_extra_keys_rejected(self) -> None:
        payload = make_valid_input()
        payload["unexpected"] = "x"

        with pytest.raises(ValueError, match="unexpected fields"):
            build_batch_ingestion_envelope(payload)

    def test_batch_id_not_string_rejected(self) -> None:
        payload = make_valid_input()
        payload["batch_id"] = 123

        with pytest.raises(TypeError, match="batch_id.*str"):
            build_batch_ingestion_envelope(payload)

    def test_batch_id_empty_rejected(self) -> None:
        payload = make_valid_input()
        payload["batch_id"] = ""

        with pytest.raises(ValueError, match="batch_id.*non-empty string"):
            build_batch_ingestion_envelope(payload)

    def test_documents_not_list_rejected(self) -> None:
        payload = make_valid_input()
        payload["documents"] = "not a list"

        with pytest.raises(TypeError, match="documents.*list"):
            build_batch_ingestion_envelope(payload)

    def test_empty_documents_list_rejected(self) -> None:
        payload = make_valid_input()
        payload["documents"] = []

        with pytest.raises(ValueError, match="documents.*non-empty list"):
            build_batch_ingestion_envelope(payload)

    def test_document_entry_not_dict_rejected(self) -> None:
        payload = make_valid_input()
        payload["documents"] = ["not a dict"]

        with pytest.raises(TypeError, match="entry at index 0.*dict"):
            build_batch_ingestion_envelope(payload)

    def test_document_entry_missing_keys_rejected(self) -> None:
        payload = make_valid_input()
        payload["documents"][0] = {"document_id": "doc_001"}

        with pytest.raises(ValueError, match="missing required fields"):
            build_batch_ingestion_envelope(payload)

    def test_document_entry_extra_keys_rejected(self) -> None:
        payload = make_valid_input()
        payload["documents"][0]["extra"] = "x"

        with pytest.raises(ValueError, match="unexpected fields"):
            build_batch_ingestion_envelope(payload)

    def test_document_id_not_string_rejected(self) -> None:
        payload = make_valid_input()
        payload["documents"][0]["document_id"] = 9

        with pytest.raises(TypeError, match="document_id.*str"):
            build_batch_ingestion_envelope(payload)

    def test_document_id_empty_rejected(self) -> None:
        payload = make_valid_input()
        payload["documents"][0]["document_id"] = ""

        with pytest.raises(ValueError, match="document_id.*non-empty string"):
            build_batch_ingestion_envelope(payload)

    def test_content_not_string_rejected(self) -> None:
        payload = make_valid_input()
        payload["documents"][0]["content"] = 99

        with pytest.raises(TypeError, match="content.*str"):
            build_batch_ingestion_envelope(payload)
