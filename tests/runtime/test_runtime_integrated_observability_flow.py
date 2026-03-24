"""FG-W18-P18-MB1 deterministic integrated observability flow test.

Validates that the observability surfaces (metrics, readiness, failure policy)
operate correctly under realistic accepted and rejected runtime paths.

Modules under test
------------------
1. runtime_request_envelope
2. runtime_control_flow_orchestrator
3. runtime_metrics_layer
4. runtime_readiness_layer
5. runtime_failure_policy_layer

Pure in-memory; no source modifications.
"""

from __future__ import annotations

from copy import deepcopy

from fleetgraph_core.runtime.runtime_control_flow_orchestrator import (
    build_runtime_control_flow_orchestration,
)
from fleetgraph_core.runtime.runtime_failure_policy_layer import (
    build_failure_policy_response,
)
from fleetgraph_core.runtime.runtime_metrics_layer import (
    build_runtime_metrics_report,
)
from fleetgraph_core.runtime.runtime_readiness_layer import (
    build_runtime_readiness_response,
)
from fleetgraph_core.runtime.runtime_request_envelope import (
    build_runtime_request_envelope,
)


# ── Input helpers ─────────────────────────────────────────────────────────────


def _valid_request_envelope_input() -> dict:
    return {
        "request_id": "req-obs-001",
        "client_id": "client-obs-01",
        "api_key": "key-obs-1",
        "operation_type": "ingest",
        "payload": {"documents": [{"id": "d1", "content": "observability test"}]},
        "runtime_limits": {"max_payload_size_bytes": 8192},
        "billing_enabled": True,
    }


def _valid_control_flow_input() -> dict:
    return {
        "request_envelope_input": _valid_request_envelope_input(),
        "security_orchestrator_input": {
            "auth_input": {
                "provided_api_key": "key-obs-1",
                "authorized_api_keys": ["key-obs-1", "key-obs-2"],
            },
            "validation_input": {
                "request_id": "req-obs-001",
                "endpoint": "/runtime/execute",
                "content_type": "application/json",
                "payload_size_bytes": 64,
                "max_payload_size_bytes": 1024,
                "has_body": True,
            },
            "rate_limit_input": {
                "client_id": "client-obs-01",
                "request_count_in_window": 1,
                "max_requests_per_window": 10,
                "window_active": True,
            },
        },
        "guardrail_orchestrator_input": {
            "resource_guardrail_input": {
                "payload_size_bytes": 64,
                "document_count": 1,
                "requested_graph_depth": 2,
                "max_payload_size_bytes": 1024,
                "max_document_count": 10,
                "max_graph_depth": 4,
            },
            "retrieval_guardrail_input": {
                "requested_result_count": 10,
                "requested_relationship_expansion_count": 5,
                "requested_evidence_link_count": 10,
                "max_result_count": 100,
                "max_relationship_expansion_count": 50,
                "max_evidence_link_count": 75,
            },
            "query_cost_input": {
                "requested_result_count": 10,
                "requested_relationship_expansion_count": 5,
                "requested_evidence_link_count": 10,
                "max_result_count": 100,
                "max_relationship_expansion_count": 50,
                "max_evidence_link_count": 75,
            },
        },
    }


def _valid_completed_boundary_result() -> dict:
    return {
        "boundary_state": "completed",
        "failure_category": None,
        "failure_message": None,
        "schedule_result": {
            "schedule_id": "sched-obs-001",
            "schedule_scope": "ingest",
            "schedule_state": "completed",
            "scheduled_batch_count": 1,
            "completed_batch_count": 1,
            "runtime_results": [],
        },
        "audit_report": {
            "audit_report_id": "audit-obs-001",
            "schedule_id": "sched-obs-001",
            "schedule_scope": "ingest",
            "schedule_state": "completed",
            "scheduled_batch_count": 1,
            "completed_batch_count": 1,
            "runtime_result_count": 0,
            "total_input_record_count": 1,
            "total_output_record_count": 1,
            "audited_run_ids": [],
            "runtime_audit_entries": [],
        },
    }


def _valid_failed_boundary_result() -> dict:
    return {
        "boundary_state": "failed",
        "failure_category": "auth_failure",
        "failure_message": "api_key_unauthorized",
        "schedule_result": None,
        "audit_report": None,
    }


def _valid_readiness_state(*, ready: bool) -> dict:
    return {
        "config_loaded": ready,
        "bootstrap_complete": ready,
    }


def _valid_failure_policy_input(
    failure_type: str,
    attempt_count: int,
    max_retries: int,
) -> dict:
    return {
        "failure_type": failure_type,
        "attempt_count": attempt_count,
        "max_retries": max_retries,
    }


# ── Accepted path ─────────────────────────────────────────────────────────────


def test_observability_accepted_path_control_flow_result_is_accepted() -> None:
    control_flow_input = _valid_control_flow_input()
    result = build_runtime_control_flow_orchestration(control_flow_input)
    assert result["status"] == "accepted"


def test_observability_accepted_path_metrics_are_present_and_deterministic() -> None:
    boundary_result = _valid_completed_boundary_result()
    first = build_runtime_metrics_report(deepcopy(boundary_result))
    second = build_runtime_metrics_report(deepcopy(boundary_result))

    assert first["metrics_state"] == "completed"
    assert first["boundary_state"] == "completed"
    assert first["schedule_id"] == "sched-obs-001"
    assert first["schedule_scope"] == "ingest"
    assert first["runtime_result_count"] == 0
    assert first["completed_batch_count"] == 1
    assert first["scheduled_batch_count"] == 1
    assert first["total_input_record_count"] == 1
    assert first["total_output_record_count"] == 1
    assert first["audited_run_id_count"] == 0
    assert first["failure_category"] is None
    assert first["failure_message"] is None
    assert first == second


def test_observability_accepted_path_readiness_is_ready_and_deterministic() -> None:
    readiness_state = _valid_readiness_state(ready=True)
    first = build_runtime_readiness_response(deepcopy(readiness_state))
    second = build_runtime_readiness_response(deepcopy(readiness_state))

    assert first["status"] == "ready"
    assert first["checks"]["config_loaded"] is True
    assert first["checks"]["bootstrap_complete"] is True
    assert first == second


# ── Rejected path ─────────────────────────────────────────────────────────────


def test_observability_rejected_path_control_flow_result_is_rejected() -> None:
    control_flow_input = _valid_control_flow_input()
    control_flow_input["security_orchestrator_input"]["auth_input"][
        "provided_api_key"
    ] = "bad-key"
    result = build_runtime_control_flow_orchestration(control_flow_input)
    assert result["status"] == "rejected"


def test_observability_rejected_path_failure_policy_validation_error_no_retry() -> None:
    policy_input = _valid_failure_policy_input("VALIDATION_ERROR", 0, 3)
    first = build_failure_policy_response(deepcopy(policy_input))
    second = build_failure_policy_response(deepcopy(policy_input))

    assert first["failure_type"] == "VALIDATION_ERROR"
    assert first["should_retry"] is False
    assert first["retry_decision"] == "do_not_retry"
    assert first == second


def test_observability_rejected_path_failure_policy_timeout_allows_retry() -> None:
    policy_input = _valid_failure_policy_input("TIMEOUT_ERROR", 0, 3)
    result = build_failure_policy_response(deepcopy(policy_input))

    assert result["failure_type"] == "TIMEOUT_ERROR"
    assert result["should_retry"] is True
    assert result["retry_decision"] == "retry"


def test_observability_rejected_path_metrics_failure_classification_is_contract_safe() -> None:
    boundary_result = _valid_failed_boundary_result()
    first = build_runtime_metrics_report(deepcopy(boundary_result))
    second = build_runtime_metrics_report(deepcopy(boundary_result))

    assert first["metrics_state"] == "failed"
    assert first["boundary_state"] == "failed"
    assert first["failure_category"] == "auth_failure"
    assert first["failure_message"] == "api_key_unauthorized"
    assert first["schedule_id"] is None
    assert first["schedule_scope"] is None
    assert first["runtime_result_count"] == 0
    assert first["completed_batch_count"] == 0
    assert first["scheduled_batch_count"] == 0
    assert first == second


def test_observability_not_ready_readiness_state_returns_not_ready() -> None:
    readiness_state = _valid_readiness_state(ready=False)
    result = build_runtime_readiness_response(readiness_state)

    assert result["status"] == "not_ready"
    assert result["checks"]["config_loaded"] is False
    assert result["checks"]["bootstrap_complete"] is False


# ── Exact key order ───────────────────────────────────────────────────────────


def test_observability_metrics_report_completed_key_order() -> None:
    boundary_result = _valid_completed_boundary_result()
    report = build_runtime_metrics_report(boundary_result)

    assert tuple(report.keys()) == (
        "metrics_state",
        "boundary_state",
        "schedule_id",
        "schedule_scope",
        "runtime_result_count",
        "completed_batch_count",
        "scheduled_batch_count",
        "total_input_record_count",
        "total_output_record_count",
        "audited_run_id_count",
        "failure_category",
        "failure_message",
    )


def test_observability_metrics_report_failed_key_order() -> None:
    boundary_result = _valid_failed_boundary_result()
    report = build_runtime_metrics_report(boundary_result)

    assert tuple(report.keys()) == (
        "metrics_state",
        "boundary_state",
        "schedule_id",
        "schedule_scope",
        "runtime_result_count",
        "completed_batch_count",
        "scheduled_batch_count",
        "total_input_record_count",
        "total_output_record_count",
        "audited_run_id_count",
        "failure_category",
        "failure_message",
    )


def test_observability_readiness_response_key_order() -> None:
    readiness_state = _valid_readiness_state(ready=True)
    response = build_runtime_readiness_response(readiness_state)

    assert tuple(response.keys()) == ("status", "checks")
    assert tuple(response["checks"].keys()) == ("config_loaded", "bootstrap_complete")


def test_observability_failure_policy_response_key_order() -> None:
    policy_input = _valid_failure_policy_input("EXECUTION_ERROR", 1, 3)
    response = build_failure_policy_response(policy_input)

    assert tuple(response.keys()) == ("failure_type", "should_retry", "retry_decision")


def test_observability_request_envelope_key_order() -> None:
    envelope_input = _valid_request_envelope_input()
    envelope = build_runtime_request_envelope(deepcopy(envelope_input))

    assert tuple(envelope.keys()) == (
        "request_id",
        "client_id",
        "api_key",
        "operation_type",
        "payload",
        "runtime_limits",
        "billing_enabled",
    )


def test_observability_control_flow_output_key_order() -> None:
    control_flow_input = _valid_control_flow_input()
    result = build_runtime_control_flow_orchestration(control_flow_input)

    assert tuple(result.keys()) == ("status", "operation_type", "reasons", "result")
    assert tuple(result["result"].keys()) == (
        "request_envelope",
        "route",
        "security",
        "guardrails",
    )


# ── Determinism and immutability ───────────────────────────────────────────────


def test_observability_repeated_execution_is_deterministic() -> None:
    control_flow_input = _valid_control_flow_input()
    first = build_runtime_control_flow_orchestration(deepcopy(control_flow_input))
    second = build_runtime_control_flow_orchestration(deepcopy(control_flow_input))
    assert first == second

    boundary_result = _valid_completed_boundary_result()
    assert build_runtime_metrics_report(deepcopy(boundary_result)) == build_runtime_metrics_report(deepcopy(boundary_result))

    readiness_state = _valid_readiness_state(ready=True)
    assert build_runtime_readiness_response(deepcopy(readiness_state)) == build_runtime_readiness_response(deepcopy(readiness_state))

    policy_input = _valid_failure_policy_input("DEPENDENCY_ERROR", 1, 3)
    assert build_failure_policy_response(deepcopy(policy_input)) == build_failure_policy_response(deepcopy(policy_input))


def test_observability_no_mutation_of_inputs() -> None:
    control_flow_input = _valid_control_flow_input()
    before_cf = deepcopy(control_flow_input)

    boundary_result = _valid_completed_boundary_result()
    before_br = deepcopy(boundary_result)

    readiness_state = _valid_readiness_state(ready=True)
    before_rs = deepcopy(readiness_state)

    policy_input = _valid_failure_policy_input("DEPENDENCY_ERROR", 2, 3)
    before_pi = deepcopy(policy_input)

    envelope_input = _valid_request_envelope_input()
    before_ei = deepcopy(envelope_input)

    build_runtime_control_flow_orchestration(control_flow_input)
    build_runtime_metrics_report(boundary_result)
    build_runtime_readiness_response(readiness_state)
    build_failure_policy_response(policy_input)
    build_runtime_request_envelope(envelope_input)

    assert control_flow_input == before_cf
    assert boundary_result == before_br
    assert readiness_state == before_rs
    assert policy_input == before_pi
    assert envelope_input == before_ei
