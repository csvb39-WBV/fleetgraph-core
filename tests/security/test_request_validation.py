"""
Test suite for D15-MB2 request validation evaluator.
"""

from __future__ import annotations

from copy import deepcopy

import pytest

from fleetgraph_core.security.request_validation import evaluate_request_validation


def make_valid_input() -> dict:
    return {
        "request_id": "req_001",
        "endpoint": "/runtime/summary",
        "content_type": "application/json",
        "payload_size_bytes": 512,
        "max_payload_size_bytes": 1024,
        "has_body": True,
    }


class TestValidPath:
    def test_valid_path(self) -> None:
        result = evaluate_request_validation(make_valid_input())

        assert result == {
            "status": "valid",
            "reasons": ["request_valid"],
        }


class TestIndividualInvalidReasons:
    def test_request_id_missing_path(self) -> None:
        payload = make_valid_input()
        payload["request_id"] = ""

        result = evaluate_request_validation(payload)

        assert result == {
            "status": "invalid",
            "reasons": ["request_id_missing"],
        }

    def test_endpoint_missing_path(self) -> None:
        payload = make_valid_input()
        payload["endpoint"] = ""

        result = evaluate_request_validation(payload)

        assert result == {
            "status": "invalid",
            "reasons": ["endpoint_missing"],
        }

    def test_content_type_missing_path(self) -> None:
        payload = make_valid_input()
        payload["content_type"] = ""

        result = evaluate_request_validation(payload)

        assert result == {
            "status": "invalid",
            "reasons": ["content_type_missing"],
        }

    def test_payload_size_limit_exceeded_path(self) -> None:
        payload = make_valid_input()
        payload["payload_size_bytes"] = 2048

        result = evaluate_request_validation(payload)

        assert result == {
            "status": "invalid",
            "reasons": ["payload_size_limit_exceeded"],
        }


class TestMultiViolationOrder:
    def test_multi_violation_invalid_path_with_deterministic_order(self) -> None:
        payload = make_valid_input()
        payload["request_id"] = ""
        payload["endpoint"] = ""
        payload["content_type"] = ""
        payload["payload_size_bytes"] = 9999

        result = evaluate_request_validation(payload)

        assert result == {
            "status": "invalid",
            "reasons": [
                "request_id_missing",
                "endpoint_missing",
                "content_type_missing",
                "payload_size_limit_exceeded",
            ],
        }


class TestOutputContract:
    def test_exact_output_key_order(self) -> None:
        result = evaluate_request_validation(make_valid_input())

        assert tuple(result.keys()) == ("status", "reasons")

    def test_reasons_is_list(self) -> None:
        result = evaluate_request_validation(make_valid_input())

        assert isinstance(result["reasons"], list)


class TestDeterminism:
    def test_deterministic_repeated_calls(self) -> None:
        payload = make_valid_input()

        first = evaluate_request_validation(payload)
        second = evaluate_request_validation(payload)

        assert first == second


class TestInputImmutability:
    def test_input_not_mutated(self) -> None:
        payload = make_valid_input()
        before = deepcopy(payload)

        evaluate_request_validation(payload)

        assert payload == before


class TestMalformedInputRejection:
    def test_missing_keys_rejected(self) -> None:
        payload = make_valid_input()
        del payload["has_body"]

        with pytest.raises(ValueError, match="missing required fields"):
            evaluate_request_validation(payload)

    def test_extra_keys_rejected(self) -> None:
        payload = make_valid_input()
        payload["unexpected"] = "x"

        with pytest.raises(ValueError, match="unexpected fields"):
            evaluate_request_validation(payload)

    def test_request_id_not_string_rejected(self) -> None:
        payload = make_valid_input()
        payload["request_id"] = 1

        with pytest.raises(TypeError, match="request_id.*str"):
            evaluate_request_validation(payload)

    def test_endpoint_not_string_rejected(self) -> None:
        payload = make_valid_input()
        payload["endpoint"] = 2

        with pytest.raises(TypeError, match="endpoint.*str"):
            evaluate_request_validation(payload)

    def test_content_type_not_string_rejected(self) -> None:
        payload = make_valid_input()
        payload["content_type"] = 3

        with pytest.raises(TypeError, match="content_type.*str"):
            evaluate_request_validation(payload)

    @pytest.mark.parametrize(
        "field,bad_value",
        [
            ("payload_size_bytes", "100"),
            ("max_payload_size_bytes", 100.0),
        ],
    )
    def test_payload_fields_not_int_rejected(self, field: str, bad_value: object) -> None:
        payload = make_valid_input()
        payload[field] = bad_value

        with pytest.raises(TypeError, match=f"field '{field}'.*int"):
            evaluate_request_validation(payload)

    @pytest.mark.parametrize(
        "field,bad_bool",
        [
            ("payload_size_bytes", True),
            ("max_payload_size_bytes", False),
        ],
    )
    def test_bool_values_in_payload_fields_rejected(self, field: str, bad_bool: bool) -> None:
        payload = make_valid_input()
        payload[field] = bad_bool

        with pytest.raises(TypeError, match=f"field '{field}'.*int"):
            evaluate_request_validation(payload)

    @pytest.mark.parametrize(
        "field",
        [
            "payload_size_bytes",
            "max_payload_size_bytes",
        ],
    )
    def test_negative_payload_fields_rejected(self, field: str) -> None:
        payload = make_valid_input()
        payload[field] = -1

        with pytest.raises(ValueError, match=f"field '{field}'.*not be negative"):
            evaluate_request_validation(payload)

    def test_has_body_not_bool_rejected(self) -> None:
        payload = make_valid_input()
        payload["has_body"] = "true"

        with pytest.raises(TypeError, match="has_body.*bool"):
            evaluate_request_validation(payload)
