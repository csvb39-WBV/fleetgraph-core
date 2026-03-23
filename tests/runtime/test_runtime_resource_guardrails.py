"""
Test suite for D13-MB2 Runtime Resource Guardrails Layer.
"""

from __future__ import annotations

from copy import deepcopy

import pytest

from fleetgraph_core.runtime.runtime_resource_guardrails import (
    build_runtime_resource_guardrails_response,
)


def make_valid_input() -> dict:
    return {
        "payload_size_bytes": 512,
        "document_count": 3,
        "requested_graph_depth": 2,
        "max_payload_size_bytes": 1024,
        "max_document_count": 10,
        "max_graph_depth": 4,
    }


class TestAllowDecision:
    def test_exact_allow_decision(self) -> None:
        result = build_runtime_resource_guardrails_response(make_valid_input())

        assert result == {
            "status": "allow",
            "violations": ["within_resource_limits"],
        }


class TestRejectIndividualBreaches:
    def test_reject_when_payload_size_limit_exceeded(self) -> None:
        payload = make_valid_input()
        payload["payload_size_bytes"] = 1025

        result = build_runtime_resource_guardrails_response(payload)

        assert result == {
            "status": "reject",
            "violations": ["payload_size_limit_exceeded"],
        }

    def test_reject_when_document_count_limit_exceeded(self) -> None:
        payload = make_valid_input()
        payload["document_count"] = 11

        result = build_runtime_resource_guardrails_response(payload)

        assert result == {
            "status": "reject",
            "violations": ["document_count_limit_exceeded"],
        }

    def test_reject_when_graph_depth_limit_exceeded(self) -> None:
        payload = make_valid_input()
        payload["requested_graph_depth"] = 5

        result = build_runtime_resource_guardrails_response(payload)

        assert result == {
            "status": "reject",
            "violations": ["graph_depth_limit_exceeded"],
        }


class TestRejectMultipleBreaches:
    def test_multiple_breaches_have_deterministic_violation_order(self) -> None:
        payload = make_valid_input()
        payload["payload_size_bytes"] = 4096
        payload["document_count"] = 999
        payload["requested_graph_depth"] = 99

        result = build_runtime_resource_guardrails_response(payload)

        assert result == {
            "status": "reject",
            "violations": [
                "payload_size_limit_exceeded",
                "document_count_limit_exceeded",
                "graph_depth_limit_exceeded",
            ],
        }


class TestOutputContract:
    def test_exact_output_key_order(self) -> None:
        result = build_runtime_resource_guardrails_response(make_valid_input())

        assert tuple(result.keys()) == ("status", "violations")

    def test_violations_is_list(self) -> None:
        result = build_runtime_resource_guardrails_response(make_valid_input())

        assert isinstance(result["violations"], list)


class TestDeterminism:
    def test_deterministic_repeated_calls(self) -> None:
        payload = make_valid_input()

        first = build_runtime_resource_guardrails_response(payload)
        second = build_runtime_resource_guardrails_response(payload)

        assert first == second


class TestInputImmutability:
    def test_input_not_mutated(self) -> None:
        payload = make_valid_input()
        before = deepcopy(payload)

        build_runtime_resource_guardrails_response(payload)

        assert payload == before


class TestMalformedInputRejection:
    def test_missing_keys_rejected(self) -> None:
        payload = make_valid_input()
        del payload["max_graph_depth"]

        with pytest.raises(ValueError, match="missing required fields"):
            build_runtime_resource_guardrails_response(payload)

    def test_extra_keys_rejected(self) -> None:
        payload = make_valid_input()
        payload["unexpected"] = 1

        with pytest.raises(ValueError, match="unexpected fields"):
            build_runtime_resource_guardrails_response(payload)

    @pytest.mark.parametrize(
        "field,bad_value",
        [
            ("payload_size_bytes", "1024"),
            ("document_count", 3.0),
            ("requested_graph_depth", None),
            ("max_payload_size_bytes", []),
            ("max_document_count", {}),
            ("max_graph_depth", "4"),
        ],
    )
    def test_non_integer_fields_rejected(self, field: str, bad_value: object) -> None:
        payload = make_valid_input()
        payload[field] = bad_value

        with pytest.raises(TypeError, match=f"field '{field}'.*int"):
            build_runtime_resource_guardrails_response(payload)

    @pytest.mark.parametrize(
        "field",
        [
            "payload_size_bytes",
            "document_count",
            "requested_graph_depth",
            "max_payload_size_bytes",
            "max_document_count",
            "max_graph_depth",
        ],
    )
    def test_negative_integer_fields_rejected(self, field: str) -> None:
        payload = make_valid_input()
        payload[field] = -1

        with pytest.raises(ValueError, match=f"field '{field}'.*not be negative"):
            build_runtime_resource_guardrails_response(payload)

    @pytest.mark.parametrize(
        "field,bad_bool",
        [
            ("payload_size_bytes", True),
            ("document_count", False),
            ("requested_graph_depth", True),
            ("max_payload_size_bytes", False),
            ("max_document_count", True),
            ("max_graph_depth", False),
        ],
    )
    def test_bool_values_rejected_for_integer_fields(self, field: str, bad_bool: bool) -> None:
        payload = make_valid_input()
        payload[field] = bad_bool

        with pytest.raises(TypeError, match=f"field '{field}'.*int"):
            build_runtime_resource_guardrails_response(payload)
