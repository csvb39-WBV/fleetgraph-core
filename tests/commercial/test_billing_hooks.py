from __future__ import annotations

import pytest
from copy import deepcopy

from fleetgraph_core.commercial.billing_hooks import evaluate_billing_hook


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _valid_ingest_input() -> dict:
    return {
        "usage_record": {
            "client_id": "client-001",
            "request_id": "req-001",
            "operation_type": "ingest",
            "document_count": 10,
            "data_processed_bytes": 4096,
        },
        "billing_enabled": True,
    }


def _valid_retrieve_input() -> dict:
    return {
        "usage_record": {
            "client_id": "client-002",
            "request_id": "req-002",
            "operation_type": "retrieve",
            "document_count": 5,
            "data_processed_bytes": 512,
        },
        "billing_enabled": True,
    }


def _valid_reprocess_input() -> dict:
    return {
        "usage_record": {
            "client_id": "client-003",
            "request_id": "req-003",
            "operation_type": "reprocess",
            "document_count": 7,
            "data_processed_bytes": 2048,
        },
        "billing_enabled": True,
    }


def _valid_non_billable_input() -> dict:
    return {
        "usage_record": {
            "client_id": "client-004",
            "request_id": "req-004",
            "operation_type": "ingest",
            "document_count": 3,
            "data_processed_bytes": 1024,
        },
        "billing_enabled": False,
    }


# ---------------------------------------------------------------------------
# Billable path tests
# ---------------------------------------------------------------------------

class TestBillableIngestPath:
    def test_status_is_billable(self):
        result = evaluate_billing_hook(_valid_ingest_input())
        assert result["status"] == "billable"

    def test_billable_units_equals_document_count(self):
        result = evaluate_billing_hook(_valid_ingest_input())
        assert result["billing_event"]["billable_units"] == 10

    def test_operation_type_preserved(self):
        result = evaluate_billing_hook(_valid_ingest_input())
        assert result["billing_event"]["operation_type"] == "ingest"

    def test_reasons_billing_enabled(self):
        result = evaluate_billing_hook(_valid_ingest_input())
        assert result["reasons"] == ["billing_enabled"]


class TestBillableRetrievePath:
    def test_status_is_billable(self):
        result = evaluate_billing_hook(_valid_retrieve_input())
        assert result["status"] == "billable"

    def test_billable_units_is_one(self):
        result = evaluate_billing_hook(_valid_retrieve_input())
        assert result["billing_event"]["billable_units"] == 1

    def test_operation_type_preserved(self):
        result = evaluate_billing_hook(_valid_retrieve_input())
        assert result["billing_event"]["operation_type"] == "retrieve"

    def test_reasons_billing_enabled(self):
        result = evaluate_billing_hook(_valid_retrieve_input())
        assert result["reasons"] == ["billing_enabled"]


class TestBillableReprocessPath:
    def test_status_is_billable(self):
        result = evaluate_billing_hook(_valid_reprocess_input())
        assert result["status"] == "billable"

    def test_billable_units_equals_document_count(self):
        result = evaluate_billing_hook(_valid_reprocess_input())
        assert result["billing_event"]["billable_units"] == 7

    def test_operation_type_preserved(self):
        result = evaluate_billing_hook(_valid_reprocess_input())
        assert result["billing_event"]["operation_type"] == "reprocess"

    def test_reasons_billing_enabled(self):
        result = evaluate_billing_hook(_valid_reprocess_input())
        assert result["reasons"] == ["billing_enabled"]


class TestNonBillablePath:
    def test_status_is_non_billable(self):
        result = evaluate_billing_hook(_valid_non_billable_input())
        assert result["status"] == "non_billable"

    def test_reasons_billing_disabled(self):
        result = evaluate_billing_hook(_valid_non_billable_input())
        assert result["reasons"] == ["billing_disabled"]

    def test_billing_event_fields_present(self):
        result = evaluate_billing_hook(_valid_non_billable_input())
        event = result["billing_event"]
        assert event["client_id"] == "client-004"
        assert event["request_id"] == "req-004"
        assert event["operation_type"] == "ingest"
        assert event["billable_units"] == 3


# ---------------------------------------------------------------------------
# Key order tests
# ---------------------------------------------------------------------------

class TestKeyOrder:
    def test_top_level_key_order(self):
        result = evaluate_billing_hook(_valid_ingest_input())
        assert tuple(result.keys()) == ("status", "billing_event", "reasons")

    def test_billing_event_key_order(self):
        result = evaluate_billing_hook(_valid_ingest_input())
        assert tuple(result["billing_event"].keys()) == (
            "client_id", "request_id", "operation_type", "billable_units"
        )


# ---------------------------------------------------------------------------
# Determinism tests
# ---------------------------------------------------------------------------

class TestDeterminism:
    def test_repeated_calls_identical(self):
        inp = _valid_ingest_input()
        r1 = evaluate_billing_hook(inp)
        r2 = evaluate_billing_hook(inp)
        assert r1 == r2

    def test_repeated_calls_identical_retrieve(self):
        inp = _valid_retrieve_input()
        r1 = evaluate_billing_hook(inp)
        r2 = evaluate_billing_hook(inp)
        assert r1 == r2


# ---------------------------------------------------------------------------
# Input immutability tests
# ---------------------------------------------------------------------------

class TestInputImmutability:
    def test_original_input_not_mutated(self):
        inp = _valid_ingest_input()
        original = deepcopy(inp)
        evaluate_billing_hook(inp)
        assert inp == original

    def test_usage_record_not_mutated(self):
        inp = _valid_ingest_input()
        original_usage = deepcopy(inp["usage_record"])
        evaluate_billing_hook(inp)
        assert inp["usage_record"] == original_usage


# ---------------------------------------------------------------------------
# Malformed input rejection tests
# ---------------------------------------------------------------------------

class TestMalformedInputRejection:
    def test_non_dict_input_raises(self):
        with pytest.raises(TypeError):
            evaluate_billing_hook("not a dict")

    def test_missing_billing_enabled_raises(self):
        inp = _valid_ingest_input()
        del inp["billing_enabled"]
        with pytest.raises(ValueError):
            evaluate_billing_hook(inp)

    def test_missing_usage_record_raises(self):
        inp = _valid_ingest_input()
        del inp["usage_record"]
        with pytest.raises(ValueError):
            evaluate_billing_hook(inp)

    def test_extra_top_key_raises(self):
        inp = _valid_ingest_input()
        inp["extra_field"] = "unexpected"
        with pytest.raises(ValueError):
            evaluate_billing_hook(inp)

    def test_billing_enabled_not_bool_raises(self):
        inp = _valid_ingest_input()
        inp["billing_enabled"] = 1
        with pytest.raises(TypeError):
            evaluate_billing_hook(inp)

    def test_usage_record_not_dict_raises(self):
        inp = _valid_ingest_input()
        inp["usage_record"] = "not a dict"
        with pytest.raises(TypeError):
            evaluate_billing_hook(inp)

    def test_missing_usage_record_key_raises(self):
        inp = _valid_ingest_input()
        del inp["usage_record"]["document_count"]
        with pytest.raises(ValueError):
            evaluate_billing_hook(inp)

    def test_extra_usage_record_key_raises(self):
        inp = _valid_ingest_input()
        inp["usage_record"]["extra_key"] = "bad"
        with pytest.raises(ValueError):
            evaluate_billing_hook(inp)

    def test_empty_client_id_raises(self):
        inp = _valid_ingest_input()
        inp["usage_record"]["client_id"] = ""
        with pytest.raises(ValueError):
            evaluate_billing_hook(inp)

    def test_non_string_client_id_raises(self):
        inp = _valid_ingest_input()
        inp["usage_record"]["client_id"] = 99
        with pytest.raises(ValueError):
            evaluate_billing_hook(inp)

    def test_empty_request_id_raises(self):
        inp = _valid_ingest_input()
        inp["usage_record"]["request_id"] = ""
        with pytest.raises(ValueError):
            evaluate_billing_hook(inp)

    def test_non_string_request_id_raises(self):
        inp = _valid_ingest_input()
        inp["usage_record"]["request_id"] = None
        with pytest.raises(ValueError):
            evaluate_billing_hook(inp)

    def test_invalid_operation_type_raises(self):
        inp = _valid_ingest_input()
        inp["usage_record"]["operation_type"] = "delete"
        with pytest.raises(ValueError):
            evaluate_billing_hook(inp)

    def test_document_count_not_int_raises(self):
        inp = _valid_ingest_input()
        inp["usage_record"]["document_count"] = "10"
        with pytest.raises(TypeError):
            evaluate_billing_hook(inp)

    def test_document_count_bool_raises(self):
        inp = _valid_ingest_input()
        inp["usage_record"]["document_count"] = True
        with pytest.raises(TypeError):
            evaluate_billing_hook(inp)

    def test_document_count_negative_raises(self):
        inp = _valid_ingest_input()
        inp["usage_record"]["document_count"] = -1
        with pytest.raises(ValueError):
            evaluate_billing_hook(inp)

    def test_data_processed_bytes_not_int_raises(self):
        inp = _valid_ingest_input()
        inp["usage_record"]["data_processed_bytes"] = 4096.0
        with pytest.raises(TypeError):
            evaluate_billing_hook(inp)

    def test_data_processed_bytes_bool_raises(self):
        inp = _valid_ingest_input()
        inp["usage_record"]["data_processed_bytes"] = False
        with pytest.raises(TypeError):
            evaluate_billing_hook(inp)

    def test_data_processed_bytes_negative_raises(self):
        inp = _valid_ingest_input()
        inp["usage_record"]["data_processed_bytes"] = -512
        with pytest.raises(ValueError):
            evaluate_billing_hook(inp)
