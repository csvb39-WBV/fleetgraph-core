"""
Test suite for D13-MB3 Runtime Retrieval Guardrails Layer.
"""

from __future__ import annotations

from copy import deepcopy

import pytest

from fleetgraph_core.runtime.runtime_retrieval_guardrails import (
    build_runtime_retrieval_guardrails_response,
)


def make_valid_input() -> dict:
    return {
        "requested_result_count": 25,
        "requested_relationship_expansion_count": 15,
        "requested_evidence_link_count": 30,
        "max_result_count": 100,
        "max_relationship_expansion_count": 50,
        "max_evidence_link_count": 75,
    }


class TestAllowDecision:
    def test_exact_allow_decision(self) -> None:
        result = build_runtime_retrieval_guardrails_response(make_valid_input())

        assert result == {
            "status": "allow",
            "violations": ["within_retrieval_limits"],
        }


class TestRejectIndividualBreaches:
    def test_reject_when_result_count_limit_exceeded(self) -> None:
        payload = make_valid_input()
        payload["requested_result_count"] = 101

        result = build_runtime_retrieval_guardrails_response(payload)

        assert result == {
            "status": "reject",
            "violations": ["result_count_limit_exceeded"],
        }

    def test_reject_when_relationship_expansion_limit_exceeded(self) -> None:
        payload = make_valid_input()
        payload["requested_relationship_expansion_count"] = 51

        result = build_runtime_retrieval_guardrails_response(payload)

        assert result == {
            "status": "reject",
            "violations": ["relationship_expansion_limit_exceeded"],
        }

    def test_reject_when_evidence_link_limit_exceeded(self) -> None:
        payload = make_valid_input()
        payload["requested_evidence_link_count"] = 76

        result = build_runtime_retrieval_guardrails_response(payload)

        assert result == {
            "status": "reject",
            "violations": ["evidence_link_limit_exceeded"],
        }


class TestRejectMultipleBreaches:
    def test_multiple_breaches_have_deterministic_violation_order(self) -> None:
        payload = make_valid_input()
        payload["requested_result_count"] = 999
        payload["requested_relationship_expansion_count"] = 999
        payload["requested_evidence_link_count"] = 999

        result = build_runtime_retrieval_guardrails_response(payload)

        assert result == {
            "status": "reject",
            "violations": [
                "result_count_limit_exceeded",
                "relationship_expansion_limit_exceeded",
                "evidence_link_limit_exceeded",
            ],
        }


class TestOutputContract:
    def test_exact_output_key_order(self) -> None:
        result = build_runtime_retrieval_guardrails_response(make_valid_input())

        assert tuple(result.keys()) == ("status", "violations")

    def test_violations_is_list(self) -> None:
        result = build_runtime_retrieval_guardrails_response(make_valid_input())

        assert isinstance(result["violations"], list)


class TestDeterminism:
    def test_deterministic_repeated_calls(self) -> None:
        payload = make_valid_input()

        first = build_runtime_retrieval_guardrails_response(payload)
        second = build_runtime_retrieval_guardrails_response(payload)

        assert first == second


class TestInputImmutability:
    def test_input_not_mutated(self) -> None:
        payload = make_valid_input()
        before = deepcopy(payload)

        build_runtime_retrieval_guardrails_response(payload)

        assert payload == before


class TestMalformedInputRejection:
    def test_missing_keys_rejected(self) -> None:
        payload = make_valid_input()
        del payload["max_evidence_link_count"]

        with pytest.raises(ValueError, match="missing required fields"):
            build_runtime_retrieval_guardrails_response(payload)

    def test_extra_keys_rejected(self) -> None:
        payload = make_valid_input()
        payload["unexpected"] = 1

        with pytest.raises(ValueError, match="unexpected fields"):
            build_runtime_retrieval_guardrails_response(payload)

    @pytest.mark.parametrize(
        "field,bad_value",
        [
            ("requested_result_count", "100"),
            ("requested_relationship_expansion_count", 10.5),
            ("requested_evidence_link_count", None),
            ("max_result_count", []),
            ("max_relationship_expansion_count", {}),
            ("max_evidence_link_count", "75"),
        ],
    )
    def test_non_integer_fields_rejected(self, field: str, bad_value: object) -> None:
        payload = make_valid_input()
        payload[field] = bad_value

        with pytest.raises(TypeError, match=f"field '{field}'.*int"):
            build_runtime_retrieval_guardrails_response(payload)

    @pytest.mark.parametrize(
        "field",
        [
            "requested_result_count",
            "requested_relationship_expansion_count",
            "requested_evidence_link_count",
            "max_result_count",
            "max_relationship_expansion_count",
            "max_evidence_link_count",
        ],
    )
    def test_negative_integer_fields_rejected(self, field: str) -> None:
        payload = make_valid_input()
        payload[field] = -1

        with pytest.raises(ValueError, match=f"field '{field}'.*not be negative"):
            build_runtime_retrieval_guardrails_response(payload)

    @pytest.mark.parametrize(
        "field,bad_bool",
        [
            ("requested_result_count", True),
            ("requested_relationship_expansion_count", False),
            ("requested_evidence_link_count", True),
            ("max_result_count", False),
            ("max_relationship_expansion_count", True),
            ("max_evidence_link_count", False),
        ],
    )
    def test_bool_values_rejected_for_integer_fields(self, field: str, bad_bool: bool) -> None:
        payload = make_valid_input()
        payload[field] = bad_bool

        with pytest.raises(TypeError, match=f"field '{field}'.*int"):
            build_runtime_retrieval_guardrails_response(payload)
