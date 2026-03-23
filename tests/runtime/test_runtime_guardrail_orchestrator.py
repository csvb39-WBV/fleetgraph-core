"""Test suite for FG-W17-P17-MB5 runtime guardrail orchestrator."""

from __future__ import annotations

from copy import deepcopy

import pytest

from fleetgraph_core.runtime.runtime_guardrail_orchestrator import (
    build_runtime_guardrail_orchestration,
)


def make_valid_orchestrator_input() -> dict[str, object]:
    return {
        "resource_guardrail_input": {
            "payload_size_bytes": 512,
            "document_count": 3,
            "requested_graph_depth": 2,
            "max_payload_size_bytes": 1024,
            "max_document_count": 10,
            "max_graph_depth": 4,
        },
        "retrieval_guardrail_input": {
            "requested_result_count": 25,
            "requested_relationship_expansion_count": 15,
            "requested_evidence_link_count": 30,
            "max_result_count": 100,
            "max_relationship_expansion_count": 50,
            "max_evidence_link_count": 75,
        },
        "query_cost_input": {
            "requested_result_count": 25,
            "requested_relationship_expansion_count": 15,
            "requested_evidence_link_count": 30,
            "max_result_count": 100,
            "max_relationship_expansion_count": 50,
            "max_evidence_link_count": 75,
        },
    }


class TestStopBehavior:
    def test_stop_on_resource_guardrail_rejection(self) -> None:
        payload = make_valid_orchestrator_input()
        payload["resource_guardrail_input"]["payload_size_bytes"] = 9999

        result = build_runtime_guardrail_orchestration(payload)

        assert result == {
            "status": "stop",
            "stage": "resource_guardrails",
            "reasons": ["payload_size_limit_exceeded"],
        }

    def test_stop_on_retrieval_guardrail_rejection(self) -> None:
        payload = make_valid_orchestrator_input()
        payload["retrieval_guardrail_input"]["requested_result_count"] = 999

        result = build_runtime_guardrail_orchestration(payload)

        assert result == {
            "status": "stop",
            "stage": "retrieval_guardrails",
            "reasons": ["result_count_limit_exceeded"],
        }

    def test_stop_on_query_cost_rejection(self) -> None:
        payload = make_valid_orchestrator_input()
        payload["query_cost_input"]["requested_result_count"] = 999

        result = build_runtime_guardrail_orchestration(payload)

        assert result == {
            "status": "stop",
            "stage": "query_cost",
            "reasons": ["result_count_limit_exceeded"],
        }


class TestContinueBehavior:
    def test_continue_when_all_guardrails_pass(self) -> None:
        result = build_runtime_guardrail_orchestration(make_valid_orchestrator_input())

        assert result == {
            "status": "continue",
            "stage": "complete",
            "reasons": ["guardrail_checks_passed"],
        }


class TestOutputContract:
    def test_exact_output_key_order(self) -> None:
        result = build_runtime_guardrail_orchestration(make_valid_orchestrator_input())

        assert tuple(result.keys()) == ("status", "stage", "reasons")

    def test_stage_values_are_exact(self) -> None:
        stage_values = {
            build_runtime_guardrail_orchestration(make_valid_orchestrator_input())["stage"]
        }

        resource_stop = make_valid_orchestrator_input()
        resource_stop["resource_guardrail_input"]["payload_size_bytes"] = 9999
        stage_values.add(build_runtime_guardrail_orchestration(resource_stop)["stage"])

        retrieval_stop = make_valid_orchestrator_input()
        retrieval_stop["retrieval_guardrail_input"]["requested_result_count"] = 999
        stage_values.add(build_runtime_guardrail_orchestration(retrieval_stop)["stage"])

        query_stop = make_valid_orchestrator_input()
        query_stop["query_cost_input"]["requested_result_count"] = 999
        stage_values.add(build_runtime_guardrail_orchestration(query_stop)["stage"])

        assert stage_values == {
            "resource_guardrails",
            "retrieval_guardrails",
            "query_cost",
            "complete",
        }


class TestDeterminismAndImmutability:
    def test_deterministic_repeated_calls(self) -> None:
        payload = make_valid_orchestrator_input()

        first = build_runtime_guardrail_orchestration(payload)
        second = build_runtime_guardrail_orchestration(payload)

        assert first == second

    def test_input_not_mutated(self) -> None:
        payload = make_valid_orchestrator_input()
        before = deepcopy(payload)

        build_runtime_guardrail_orchestration(payload)

        assert payload == before


class TestMalformedInputRejection:
    def test_missing_top_level_keys_rejected(self) -> None:
        payload = make_valid_orchestrator_input()
        del payload["query_cost_input"]

        with pytest.raises(ValueError, match="missing required fields"):
            build_runtime_guardrail_orchestration(payload)

    def test_extra_top_level_keys_rejected(self) -> None:
        payload = make_valid_orchestrator_input()
        payload["unexpected"] = {}

        with pytest.raises(ValueError, match="unexpected fields"):
            build_runtime_guardrail_orchestration(payload)

    @pytest.mark.parametrize(
        "field,bad_value",
        [
            ("resource_guardrail_input", []),
            ("retrieval_guardrail_input", "bad"),
            ("query_cost_input", 123),
        ],
    )
    def test_top_level_input_fields_must_be_dicts(
        self,
        field: str,
        bad_value: object,
    ) -> None:
        payload = make_valid_orchestrator_input()
        payload[field] = bad_value

        with pytest.raises(TypeError, match=f"field '{field}'.*dict"):
            build_runtime_guardrail_orchestration(payload)