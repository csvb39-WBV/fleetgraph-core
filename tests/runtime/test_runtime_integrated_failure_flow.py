from __future__ import annotations

from copy import deepcopy

from fleetgraph_core.runtime.runtime_control_flow_orchestrator import (
    build_runtime_control_flow_orchestration,
)
from fleetgraph_core.runtime.runtime_guardrail_orchestrator import (
    build_runtime_guardrail_orchestration,
)
from fleetgraph_core.runtime.runtime_security_orchestrator import (
    orchestrate_runtime_security,
)


def _valid_control_flow_input() -> dict[str, object]:
    return {
        "request_envelope_input": {
            "request_id": "req-failure-001",
            "client_id": "client-001",
            "api_key": "key-1",
            "operation_type": "ingest",
            "payload": {
                "documents": [{"id": "d1", "content": "abc"}],
            },
            "runtime_limits": {
                "max_payload_size_bytes": 8192,
            },
            "billing_enabled": True,
        },
        "security_orchestrator_input": {
            "auth_input": {
                "provided_api_key": "key-1",
                "authorized_api_keys": ["key-1", "key-2"],
            },
            "validation_input": {
                "request_id": "req-failure-001",
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


def test_integrated_failure_flow_api_key_authentication_failure() -> None:
    payload = _valid_control_flow_input()
    payload["security_orchestrator_input"]["auth_input"]["provided_api_key"] = "bad-key"
    payload["guardrail_orchestrator_input"]["resource_guardrail_input"]["payload_size_bytes"] = 9999

    expected_security = orchestrate_runtime_security(payload["security_orchestrator_input"])
    result = build_runtime_control_flow_orchestration(payload)

    assert result["status"] == "rejected"
    assert result["result"]["security"]["stage"] == "auth"
    assert result["reasons"] == expected_security["reasons"]
    assert result["result"]["guardrails"] == {}


def test_integrated_failure_flow_request_validation_failure() -> None:
    payload = _valid_control_flow_input()
    payload["security_orchestrator_input"]["validation_input"]["request_id"] = ""
    payload["guardrail_orchestrator_input"]["resource_guardrail_input"]["payload_size_bytes"] = 9999

    expected_security = orchestrate_runtime_security(payload["security_orchestrator_input"])
    result = build_runtime_control_flow_orchestration(payload)

    assert result["status"] == "rejected"
    assert result["result"]["security"]["stage"] == "validation"
    assert result["reasons"] == expected_security["reasons"]
    assert result["result"]["guardrails"] == {}


def test_integrated_failure_flow_rate_limiting_failure() -> None:
    payload = _valid_control_flow_input()
    payload["security_orchestrator_input"]["rate_limit_input"]["request_count_in_window"] = 10
    payload["guardrail_orchestrator_input"]["resource_guardrail_input"]["payload_size_bytes"] = 9999

    expected_security = orchestrate_runtime_security(payload["security_orchestrator_input"])
    result = build_runtime_control_flow_orchestration(payload)

    assert result["status"] == "rejected"
    assert result["result"]["security"]["stage"] == "rate_limit"
    assert result["reasons"] == expected_security["reasons"]
    assert result["result"]["guardrails"] == {}


def test_integrated_failure_flow_resource_guardrail_rejection() -> None:
    payload = _valid_control_flow_input()
    payload["guardrail_orchestrator_input"]["resource_guardrail_input"]["payload_size_bytes"] = 9999

    expected_guardrails = build_runtime_guardrail_orchestration(
        payload["guardrail_orchestrator_input"]
    )
    result = build_runtime_control_flow_orchestration(payload)

    assert result["status"] == "rejected"
    assert result["result"]["guardrails"]["stage"] == "resource_guardrails"
    assert result["reasons"] == expected_guardrails["reasons"]
    assert result["result"]["security"]["stage"] == "complete"


def test_integrated_failure_flow_retrieval_guardrail_rejection() -> None:
    payload = _valid_control_flow_input()
    payload["guardrail_orchestrator_input"]["retrieval_guardrail_input"]["requested_result_count"] = 999

    expected_guardrails = build_runtime_guardrail_orchestration(
        payload["guardrail_orchestrator_input"]
    )
    result = build_runtime_control_flow_orchestration(payload)

    assert result["status"] == "rejected"
    assert result["result"]["guardrails"]["stage"] == "retrieval_guardrails"
    assert result["reasons"] == expected_guardrails["reasons"]
    assert result["result"]["security"]["stage"] == "complete"


def test_integrated_failure_flow_query_cost_rejection() -> None:
    payload = _valid_control_flow_input()
    payload["guardrail_orchestrator_input"]["query_cost_input"]["requested_result_count"] = 999

    expected_guardrails = build_runtime_guardrail_orchestration(
        payload["guardrail_orchestrator_input"]
    )
    result = build_runtime_control_flow_orchestration(payload)

    assert result["status"] == "rejected"
    assert result["result"]["guardrails"]["stage"] == "query_cost"
    assert result["reasons"] == expected_guardrails["reasons"]
    assert result["result"]["security"]["stage"] == "complete"


def test_integrated_failure_flow_output_key_order_determinism_and_immutability() -> None:
    payload = _valid_control_flow_input()
    payload["security_orchestrator_input"]["auth_input"]["provided_api_key"] = "bad-key"
    before = deepcopy(payload)

    first = build_runtime_control_flow_orchestration(payload)
    second = build_runtime_control_flow_orchestration(payload)

    assert tuple(first.keys()) == ("status", "operation_type", "reasons", "result")
    assert tuple(first["result"].keys()) == (
        "request_envelope",
        "route",
        "security",
        "guardrails",
    )
    assert first == second
    assert payload == before
