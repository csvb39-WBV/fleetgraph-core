"""
Test suite for D16-MB2 usage metering evaluator.
"""

from __future__ import annotations

from copy import deepcopy

import pytest

from fleetgraph_core.commercial.usage_metering import evaluate_usage_metering


def make_valid_input() -> dict:
    return {
        "client_id": "client_001",
        "request_id": "req_001",
        "operation_type": "ingest",
        "document_count": 12,
        "data_processed_bytes": 4096,
    }


class TestValidUsageRecord:
    def test_valid_usage_record(self) -> None:
        result = evaluate_usage_metering(make_valid_input())

        assert result == {
            "status": "recorded",
            "usage_record": {
                "client_id": "client_001",
                "request_id": "req_001",
                "operation_type": "ingest",
                "document_count": 12,
                "data_processed_bytes": 4096,
            },
        }


class TestExactKeyOrder:
    def test_exact_key_order(self) -> None:
        result = evaluate_usage_metering(make_valid_input())

        assert tuple(result.keys()) == ("status", "usage_record")
        assert tuple(result["usage_record"].keys()) == (
            "client_id",
            "request_id",
            "operation_type",
            "document_count",
            "data_processed_bytes",
        )


class TestDeterminism:
    def test_deterministic_repeated_calls(self) -> None:
        payload = make_valid_input()

        first = evaluate_usage_metering(payload)
        second = evaluate_usage_metering(payload)

        assert first == second


class TestInputImmutability:
    def test_input_not_mutated(self) -> None:
        payload = make_valid_input()
        before = deepcopy(payload)

        evaluate_usage_metering(payload)

        assert payload == before


class TestMalformedInputRejection:
    def test_missing_keys_rejected(self) -> None:
        payload = make_valid_input()
        del payload["data_processed_bytes"]

        with pytest.raises(ValueError, match="missing required fields"):
            evaluate_usage_metering(payload)

    def test_extra_keys_rejected(self) -> None:
        payload = make_valid_input()
        payload["unexpected"] = "x"

        with pytest.raises(ValueError, match="unexpected fields"):
            evaluate_usage_metering(payload)

    def test_client_id_not_string_rejected(self) -> None:
        payload = make_valid_input()
        payload["client_id"] = 1

        with pytest.raises(TypeError, match="client_id.*str"):
            evaluate_usage_metering(payload)

    def test_client_id_empty_rejected(self) -> None:
        payload = make_valid_input()
        payload["client_id"] = ""

        with pytest.raises(ValueError, match="client_id.*non-empty string"):
            evaluate_usage_metering(payload)

    def test_request_id_not_string_rejected(self) -> None:
        payload = make_valid_input()
        payload["request_id"] = 2

        with pytest.raises(TypeError, match="request_id.*str"):
            evaluate_usage_metering(payload)

    def test_request_id_empty_rejected(self) -> None:
        payload = make_valid_input()
        payload["request_id"] = ""

        with pytest.raises(ValueError, match="request_id.*non-empty string"):
            evaluate_usage_metering(payload)

    def test_invalid_operation_type_rejected(self) -> None:
        payload = make_valid_input()
        payload["operation_type"] = "export"

        with pytest.raises(ValueError, match="operation_type.*must be one of"):
            evaluate_usage_metering(payload)

    def test_document_count_not_int_rejected(self) -> None:
        payload = make_valid_input()
        payload["document_count"] = "12"

        with pytest.raises(TypeError, match="document_count.*int"):
            evaluate_usage_metering(payload)

    def test_data_processed_bytes_not_int_rejected(self) -> None:
        payload = make_valid_input()
        payload["data_processed_bytes"] = 4096.0

        with pytest.raises(TypeError, match="data_processed_bytes.*int"):
            evaluate_usage_metering(payload)

    def test_bool_document_count_rejected(self) -> None:
        payload = make_valid_input()
        payload["document_count"] = True

        with pytest.raises(TypeError, match="document_count.*int"):
            evaluate_usage_metering(payload)

    def test_bool_data_processed_bytes_rejected(self) -> None:
        payload = make_valid_input()
        payload["data_processed_bytes"] = False

        with pytest.raises(TypeError, match="data_processed_bytes.*int"):
            evaluate_usage_metering(payload)

    def test_negative_document_count_rejected(self) -> None:
        payload = make_valid_input()
        payload["document_count"] = -1

        with pytest.raises(ValueError, match="document_count.*not be negative"):
            evaluate_usage_metering(payload)

    def test_negative_data_processed_bytes_rejected(self) -> None:
        payload = make_valid_input()
        payload["data_processed_bytes"] = -1

        with pytest.raises(ValueError, match="data_processed_bytes.*not be negative"):
            evaluate_usage_metering(payload)
