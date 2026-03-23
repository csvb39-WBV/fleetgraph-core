"""Test suite for FG-W17-P17-MB9 runtime control-flow orchestrator."""

from __future__ import annotations

from copy import deepcopy

import pytest

from fleetgraph_core.runtime.runtime_control_flow_orchestrator import (
    build_runtime_control_flow_orchestration,
)
from fleetgraph_core.runtime.runtime_guardrail_orchestrator import (
    build_runtime_guardrail_orchestration,
)
from fleetgraph_core.runtime.runtime_operation_router import route_runtime_operation
from fleetgraph_core.runtime.runtime_request_envelope import (
    build_runtime_request_envelope,
)
from fleetgraph_core.runtime.runtime_response_envelope import (
    build_runtime_response_envelope,
)
from fleetgraph_core.runtime.runtime_security_orchestrator import (
    orchestrate_runtime_security,
)


def make_valid_input() -> dict[str, object]:
    return {
        "request_envelope_input": {
            "request_id": "req-001",
            "client_id": "client-001",
            "api_key": "key-1",
            "operation_type": "retrieve",
            "payload": {"query": "acme"},
            "runtime_limits": {"max_ms": 1000},
            "billing_enabled": True,
        },
        "security_orchestrator_input": {
            "auth_input": {
                "provided_api_key": "key-1",
                "authorized_api_keys": ["key-1", "key-2"],
            },
            "validation_input": {
                "request_id": "req-001",
                "endpoint": "/runtime/execute",
                "content_type": "application/json",
                "payload_size_bytes": 64,
                "max_payload_size_bytes": 1024,
                "has_body": True,
            },
            "rate_limit_input": {
                "client_id": "client-001",
                "request_count_in_window": 1,
                "max_requests_per_window": 10,
                "window_active": True,
            },
        },
        "guardrail_orchestrator_input": {
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
        },
    }


def _build_expected_response_envelope(
    *,
    request_envelope: dict[str, object],
    status: str,
    reasons: list[str],
    result: dict[str, object],
) -> dict[str, object]:
    response_status = "accepted" if status == "accepted" else "failed"
    return build_runtime_response_envelope(
        {
            "request_id": request_envelope["request_id"],
            "client_id": request_envelope["client_id"],
            "operation_type": request_envelope["operation_type"],
            "status": response_status,
            "result": result,
            "errors": reasons,
            "billing_enabled": request_envelope["billing_enabled"],
        }
    )


class TestPaths:
    def test_accepted_path_when_security_and_guardrails_continue(self) -> None:
        payload = make_valid_input()

        result = build_runtime_control_flow_orchestration(payload)

        request_envelope = build_runtime_request_envelope(payload["request_envelope_input"])
        route = route_runtime_operation({"operation_type": request_envelope["operation_type"]})
        security = orchestrate_runtime_security(payload["security_orchestrator_input"])
        guardrails = build_runtime_guardrail_orchestration(payload["guardrail_orchestrator_input"])

        assert result == {
            "status": "accepted",
            "operation_type": "retrieve",
            "reasons": ["control_flow_checks_passed"],
            "result": {
                "request_envelope": request_envelope,
                "route": route,
                "security": security,
                "guardrails": guardrails,
            },
        }

    def test_rejected_path_on_security_stop(self) -> None:
        payload = make_valid_input()
        payload["security_orchestrator_input"]["auth_input"]["provided_api_key"] = "bad-key"

        result = build_runtime_control_flow_orchestration(payload)

        request_envelope = build_runtime_request_envelope(payload["request_envelope_input"])
        route = route_runtime_operation({"operation_type": request_envelope["operation_type"]})
        security = orchestrate_runtime_security(payload["security_orchestrator_input"])

        assert result == {
            "status": "rejected",
            "operation_type": "retrieve",
            "reasons": ["api_key_not_authorized"],
            "result": {
                "request_envelope": request_envelope,
                "route": route,
                "security": security,
                "guardrails": {},
            },
        }

    def test_rejected_path_on_guardrail_stop(self) -> None:
        payload = make_valid_input()
        payload["guardrail_orchestrator_input"]["resource_guardrail_input"]["payload_size_bytes"] = 9999

        result = build_runtime_control_flow_orchestration(payload)

        request_envelope = build_runtime_request_envelope(payload["request_envelope_input"])
        route = route_runtime_operation({"operation_type": request_envelope["operation_type"]})
        security = orchestrate_runtime_security(payload["security_orchestrator_input"])
        guardrails = build_runtime_guardrail_orchestration(payload["guardrail_orchestrator_input"])

        assert result == {
            "status": "rejected",
            "operation_type": "retrieve",
            "reasons": ["payload_size_limit_exceeded"],
            "result": {
                "request_envelope": request_envelope,
                "route": route,
                "security": security,
                "guardrails": guardrails,
            },
        }


class TestOutputContract:
    def test_exact_output_key_order_top_level_and_result_level(self) -> None:
        result = build_runtime_control_flow_orchestration(make_valid_input())

        assert tuple(result.keys()) == (
            "status",
            "operation_type",
            "reasons",
            "result",
        )
        assert tuple(result["result"].keys()) == (
            "request_envelope",
            "route",
            "security",
            "guardrails",
        )

    def test_operation_type_preserved_exactly_from_request_envelope(self) -> None:
        payload = make_valid_input()
        payload["request_envelope_input"]["operation_type"] = "status"

        result = build_runtime_control_flow_orchestration(payload)

        assert result["operation_type"] == "status"

    def test_final_output_comes_through_response_envelope_contract(self) -> None:
        payload = make_valid_input()

        actual = build_runtime_control_flow_orchestration(payload)

        request_envelope = build_runtime_request_envelope(payload["request_envelope_input"])
        route = route_runtime_operation({"operation_type": request_envelope["operation_type"]})
        security = orchestrate_runtime_security(payload["security_orchestrator_input"])
        guardrails = build_runtime_guardrail_orchestration(payload["guardrail_orchestrator_input"])

        expected_response_envelope = _build_expected_response_envelope(
            request_envelope=request_envelope,
            status="accepted",
            reasons=["control_flow_checks_passed"],
            result={
                "request_envelope": request_envelope,
                "route": route,
                "security": security,
                "guardrails": guardrails,
            },
        )

        assert actual == {
            "status": "accepted",
            "operation_type": expected_response_envelope["operation_type"],
            "reasons": expected_response_envelope["errors"],
            "result": expected_response_envelope["result"],
        }


class TestDeterminismAndImmutability:
    def test_deterministic_repeated_calls(self) -> None:
        payload = make_valid_input()

        first = build_runtime_control_flow_orchestration(payload)
        second = build_runtime_control_flow_orchestration(payload)

        assert first == second

    def test_input_immutability(self) -> None:
        payload = make_valid_input()
        before = deepcopy(payload)

        build_runtime_control_flow_orchestration(payload)

        assert payload == before


class TestMalformedTopLevelInputRejection:
    def test_missing_top_level_keys_rejected(self) -> None:
        payload = make_valid_input()
        del payload["guardrail_orchestrator_input"]

        with pytest.raises(ValueError, match="missing required fields"):
            build_runtime_control_flow_orchestration(payload)

    def test_extra_top_level_keys_rejected(self) -> None:
        payload = make_valid_input()
        payload["unexpected"] = {}

        with pytest.raises(ValueError, match="unexpected fields"):
            build_runtime_control_flow_orchestration(payload)

    @pytest.mark.parametrize(
        "field,bad_value",
        [
            ("request_envelope_input", []),
            ("security_orchestrator_input", "bad"),
            ("guardrail_orchestrator_input", 123),
        ],
    )
    def test_top_level_inputs_must_be_dicts(
        self,
        field: str,
        bad_value: object,
    ) -> None:
        payload = make_valid_input()
        payload[field] = bad_value

        with pytest.raises(TypeError, match=f"field '{field}'.*dict"):
            build_runtime_control_flow_orchestration(payload)
