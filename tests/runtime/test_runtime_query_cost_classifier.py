"""
Test suite for D13-MB4 Runtime Query Cost Classification Layer.
"""

from __future__ import annotations

from copy import deepcopy

import pytest

from fleetgraph_core.runtime.runtime_query_cost_classifier import (
    build_runtime_query_cost_classification,
)


def make_base_input() -> dict:
    return {
        "requested_result_count": 0,
        "requested_relationship_expansion_count": 0,
        "requested_evidence_link_count": 0,
        "max_result_count": 100,
        "max_relationship_expansion_count": 100,
        "max_evidence_link_count": 100,
    }


class TestCostClassifications:
    def test_low_cost_classification(self) -> None:
        payload = make_base_input()
        payload["requested_result_count"] = 25
        payload["requested_relationship_expansion_count"] = 10
        payload["requested_evidence_link_count"] = 0

        result = build_runtime_query_cost_classification(payload)

        assert result == {
            "classification": "low_cost",
            "reasons": ["within_low_cost_range"],
        }

    def test_medium_cost_classification(self) -> None:
        payload = make_base_input()
        payload["requested_result_count"] = 26
        payload["requested_relationship_expansion_count"] = 50
        payload["requested_evidence_link_count"] = 0

        result = build_runtime_query_cost_classification(payload)

        assert result == {
            "classification": "medium_cost",
            "reasons": ["within_medium_cost_range"],
        }

    def test_high_cost_classification(self) -> None:
        payload = make_base_input()
        payload["requested_result_count"] = 76
        payload["requested_relationship_expansion_count"] = 1
        payload["requested_evidence_link_count"] = 1

        result = build_runtime_query_cost_classification(payload)

        assert result == {
            "classification": "high_cost",
            "reasons": ["within_high_cost_range"],
        }

    def test_boundary_at_75_percent_is_medium_not_high(self) -> None:
        payload = make_base_input()
        payload["requested_result_count"] = 75

        result = build_runtime_query_cost_classification(payload)

        assert result["classification"] == "medium_cost"

    def test_boundary_at_25_percent_is_low_not_medium(self) -> None:
        payload = make_base_input()
        payload["requested_result_count"] = 25

        result = build_runtime_query_cost_classification(payload)

        assert result["classification"] == "low_cost"


class TestRejectPaths:
    def test_reject_when_result_count_exceeds_max(self) -> None:
        payload = make_base_input()
        payload["requested_result_count"] = 101

        result = build_runtime_query_cost_classification(payload)

        assert result == {
            "classification": "reject",
            "reasons": ["result_count_limit_exceeded"],
        }

    def test_reject_when_relationship_expansion_exceeds_max(self) -> None:
        payload = make_base_input()
        payload["requested_relationship_expansion_count"] = 101

        result = build_runtime_query_cost_classification(payload)

        assert result == {
            "classification": "reject",
            "reasons": ["relationship_expansion_limit_exceeded"],
        }

    def test_reject_when_evidence_link_exceeds_max(self) -> None:
        payload = make_base_input()
        payload["requested_evidence_link_count"] = 101

        result = build_runtime_query_cost_classification(payload)

        assert result == {
            "classification": "reject",
            "reasons": ["evidence_link_limit_exceeded"],
        }

    def test_reject_reason_order_is_deterministic(self) -> None:
        payload = make_base_input()
        payload["requested_result_count"] = 101
        payload["requested_relationship_expansion_count"] = 102
        payload["requested_evidence_link_count"] = 103

        result = build_runtime_query_cost_classification(payload)

        assert result == {
            "classification": "reject",
            "reasons": [
                "result_count_limit_exceeded",
                "relationship_expansion_limit_exceeded",
                "evidence_link_limit_exceeded",
            ],
        }


class TestOutputContract:
    def test_exact_key_order(self) -> None:
        result = build_runtime_query_cost_classification(make_base_input())

        assert tuple(result.keys()) == ("classification", "reasons")

    def test_reasons_is_list(self) -> None:
        result = build_runtime_query_cost_classification(make_base_input())

        assert isinstance(result["reasons"], list)


class TestDeterminism:
    def test_deterministic_repeated_calls(self) -> None:
        payload = make_base_input()
        payload["requested_result_count"] = 76

        first = build_runtime_query_cost_classification(payload)
        second = build_runtime_query_cost_classification(payload)

        assert first == second


class TestInputImmutability:
    def test_input_not_mutated(self) -> None:
        payload = make_base_input()
        before = deepcopy(payload)

        build_runtime_query_cost_classification(payload)

        assert payload == before


class TestMalformedInputRejection:
    def test_missing_keys_rejected(self) -> None:
        payload = make_base_input()
        del payload["max_evidence_link_count"]

        with pytest.raises(ValueError, match="missing required fields"):
            build_runtime_query_cost_classification(payload)

    def test_extra_keys_rejected(self) -> None:
        payload = make_base_input()
        payload["unexpected"] = 1

        with pytest.raises(ValueError, match="unexpected fields"):
            build_runtime_query_cost_classification(payload)

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
        payload = make_base_input()
        payload[field] = bad_value

        with pytest.raises(TypeError, match=f"field '{field}'.*int"):
            build_runtime_query_cost_classification(payload)

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
        payload = make_base_input()
        payload[field] = -1

        with pytest.raises(ValueError, match=f"field '{field}'.*not be negative"):
            build_runtime_query_cost_classification(payload)

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
        payload = make_base_input()
        payload[field] = bad_bool

        with pytest.raises(TypeError, match=f"field '{field}'.*int"):
            build_runtime_query_cost_classification(payload)
