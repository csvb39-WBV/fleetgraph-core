from __future__ import annotations

from copy import deepcopy

from fleetgraph_core.runtime.runtime_commercial_orchestrator import (
    build_runtime_commercial_orchestration,
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
from fleetgraph_core.runtime.runtime_retrieval_orchestrator import (
    build_runtime_retrieval_orchestration,
)
from fleetgraph_core.runtime.runtime_security_orchestrator import (
    orchestrate_runtime_security,
)


def _request_envelope_input() -> dict[str, object]:
    return {
        "request_id": "req-retrieve-001",
        "client_id": "client-001",
        "api_key": "key-1",
        "operation_type": "retrieve",
        "payload": {
            "query": "show entity neighborhood",
            "matter_id": "matter_001",
        },
        "runtime_limits": {
            "max_payload_size_bytes": 4096,
        },
        "billing_enabled": True,
    }


def _security_orchestrator_input() -> dict[str, object]:
    return {
        "auth_input": {
            "provided_api_key": "key-1",
            "authorized_api_keys": ["key-1", "key-2"],
        },
        "validation_input": {
            "request_id": "req-retrieve-001",
            "endpoint": "/runtime/retrieve",
            "content_type": "application/json",
            "payload_size_bytes": 128,
            "max_payload_size_bytes": 4096,
            "has_body": True,
        },
        "rate_limit_input": {
            "client_id": "client-001",
            "request_count_in_window": 2,
            "max_requests_per_window": 10,
            "window_active": True,
        },
    }


def _guardrail_orchestrator_input() -> dict[str, object]:
    return {
        "resource_guardrail_input": {
            "payload_size_bytes": 128,
            "document_count": 3,
            "requested_graph_depth": 2,
            "max_payload_size_bytes": 4096,
            "max_document_count": 50,
            "max_graph_depth": 4,
        },
        "retrieval_guardrail_input": {
            "requested_result_count": 25,
            "requested_relationship_expansion_count": 12,
            "requested_evidence_link_count": 20,
            "max_result_count": 100,
            "max_relationship_expansion_count": 50,
            "max_evidence_link_count": 75,
        },
        "query_cost_input": {
            "requested_result_count": 25,
            "requested_relationship_expansion_count": 12,
            "requested_evidence_link_count": 20,
            "max_result_count": 100,
            "max_relationship_expansion_count": 50,
            "max_evidence_link_count": 75,
        },
    }


def _retrieval_orchestrator_input(force_reprocess: bool) -> dict[str, object]:
    return {
        "recompute_gate_input": {
            "stored_manifest": {
                "matter_id": "matter_001",
                "document_set_version": "v2026.03.23",
                "ingestion_run_id": "ingest_run_abc123",
                "pipeline_version": "pipeline_v1",
                "schema_version": "schema_v1",
                "source_hash": "sha256:abc123",
                "artifact_keys": [
                    "artifacts/entities/entities.json",
                    "artifacts/events/events.json",
                    "artifacts/graph/graph.json",
                ],
            },
            "requested_state": {
                "document_set_version": "v2026.03.23",
                "pipeline_version": "pipeline_v1",
                "schema_version": "schema_v1",
                "source_hash": "sha256:abc123",
                "force_reprocess": force_reprocess,
            },
        },
        "canonical_store_input": {
            "manifest": {
                "matter_id": "matter_001",
                "document_set_version": "v2026.03.23",
                "ingestion_run_id": "ingest_run_abc123",
                "pipeline_version": "pipeline_v1",
                "schema_version": "schema_v1",
                "source_hash": "sha256:abc123",
                "artifact_keys": [
                    "artifacts/entities/entities.json",
                    "artifacts/events/events.json",
                    "artifacts/graph/graph.json",
                ],
            },
            "artifacts": {
                "entities": [
                    {"entity_id": "e2", "name": "Beta"},
                    {"entity_id": "e1", "name": "Alpha"},
                ],
                "events": [
                    {"event_id": "ev2", "sequence": 2},
                    {"event_id": "ev1", "sequence": 1},
                ],
                "relationships": [
                    {"source": "e2", "target": "e1", "type": "related_to"},
                    {"source": "e1", "target": "e2", "type": "related_to"},
                ],
                "evidence_links": [{"event_id": "ev1", "doc_id": "d1"}],
                "graph_artifacts": [{"node_count": 2, "edge_count": 1}],
            },
        },
    }


def _commercial_orchestrator_input() -> dict[str, object]:
    return {
        "usage_metering_input": {
            "client_id": "client-001",
            "request_id": "req-retrieve-001",
            "operation_type": "retrieve",
            "document_count": 3,
            "data_processed_bytes": 8192,
        },
        "billing_hooks_input": {
            "usage_record": {
                "client_id": "ignored",
                "request_id": "ignored",
                "operation_type": "ingest",
                "document_count": 99,
                "data_processed_bytes": 1,
            },
            "billing_enabled": True,
        },
    }


def _run_integrated_retrieval_flow(force_reprocess: bool) -> dict[str, object]:
    request_envelope_input = _request_envelope_input()
    security_input = _security_orchestrator_input()
    guardrail_input = _guardrail_orchestrator_input()
    retrieval_input = _retrieval_orchestrator_input(force_reprocess=force_reprocess)
    commercial_input = _commercial_orchestrator_input()

    request_envelope = build_runtime_request_envelope(deepcopy(request_envelope_input))
    route = route_runtime_operation({"operation_type": request_envelope["operation_type"]})
    security = orchestrate_runtime_security(deepcopy(security_input))
    guardrails = build_runtime_guardrail_orchestration(deepcopy(guardrail_input))
    retrieval = build_runtime_retrieval_orchestration(deepcopy(retrieval_input))
    commercial = build_runtime_commercial_orchestration(deepcopy(commercial_input))

    result_payload = {
        "request_envelope": request_envelope,
        "route": route,
        "security": security,
        "guardrails": guardrails,
        "retrieval": retrieval,
        "commercial": commercial,
    }

    response_envelope = build_runtime_response_envelope(
        {
            "request_id": request_envelope["request_id"],
            "client_id": request_envelope["client_id"],
            "operation_type": request_envelope["operation_type"],
            "status": "accepted",
            "result": result_payload,
            "errors": [],
            "billing_enabled": request_envelope["billing_enabled"],
        }
    )

    return response_envelope


def test_integrated_retrieval_flow_accepts_reuse_stored_artifacts_path() -> None:
    response = _run_integrated_retrieval_flow(force_reprocess=False)

    assert response["status"] == "accepted"
    assert response["operation_type"] == "retrieve"
    assert response["result"]["retrieval"]["result"]["path"] == "reuse_stored_artifacts"
    assert response["result"]["commercial"]["status"] == "completed"
    assert response["result"]["commercial"]["result"]["billing_event"]["billable_units"] == 1


def test_integrated_retrieval_flow_accepts_recompute_required_path() -> None:
    response = _run_integrated_retrieval_flow(force_reprocess=True)

    assert response["status"] == "accepted"
    assert response["operation_type"] == "retrieve"
    assert response["result"]["retrieval"]["result"]["path"] == "recompute_required"
    assert response["result"]["commercial"]["status"] == "completed"


def test_integrated_retrieval_flow_preserves_delegated_outputs_exactly() -> None:
    request_envelope_input = _request_envelope_input()
    security_input = _security_orchestrator_input()
    guardrail_input = _guardrail_orchestrator_input()
    retrieval_input = _retrieval_orchestrator_input(force_reprocess=False)
    commercial_input = _commercial_orchestrator_input()

    expected_request = build_runtime_request_envelope(deepcopy(request_envelope_input))
    expected_route = route_runtime_operation({"operation_type": expected_request["operation_type"]})
    expected_security = orchestrate_runtime_security(deepcopy(security_input))
    expected_guardrails = build_runtime_guardrail_orchestration(deepcopy(guardrail_input))
    expected_retrieval = build_runtime_retrieval_orchestration(deepcopy(retrieval_input))
    expected_commercial = build_runtime_commercial_orchestration(deepcopy(commercial_input))

    response = _run_integrated_retrieval_flow(force_reprocess=False)

    assert response["result"]["request_envelope"] == expected_request
    assert response["result"]["route"] == expected_route
    assert response["result"]["security"] == expected_security
    assert response["result"]["guardrails"] == expected_guardrails
    assert response["result"]["retrieval"] == expected_retrieval
    assert response["result"]["commercial"] == expected_commercial


def test_integrated_retrieval_flow_exact_response_envelope_key_order() -> None:
    response = _run_integrated_retrieval_flow(force_reprocess=False)

    assert tuple(response.keys()) == (
        "request_id",
        "client_id",
        "operation_type",
        "status",
        "result",
        "errors",
        "billing_enabled",
    )


def test_integrated_retrieval_flow_exact_result_key_order() -> None:
    response = _run_integrated_retrieval_flow(force_reprocess=False)

    assert tuple(response["result"].keys()) == (
        "request_envelope",
        "route",
        "security",
        "guardrails",
        "retrieval",
        "commercial",
    )


def test_integrated_retrieval_flow_is_deterministic_for_repeated_calls() -> None:
    first = _run_integrated_retrieval_flow(force_reprocess=False)
    second = _run_integrated_retrieval_flow(force_reprocess=False)

    assert first == second


def test_integrated_retrieval_flow_does_not_mutate_inputs() -> None:
    request_envelope_input = _request_envelope_input()
    security_input = _security_orchestrator_input()
    guardrail_input = _guardrail_orchestrator_input()
    retrieval_input = _retrieval_orchestrator_input(force_reprocess=False)
    commercial_input = _commercial_orchestrator_input()

    before_request = deepcopy(request_envelope_input)
    before_security = deepcopy(security_input)
    before_guardrail = deepcopy(guardrail_input)
    before_retrieval = deepcopy(retrieval_input)
    before_commercial = deepcopy(commercial_input)

    request_envelope = build_runtime_request_envelope(deepcopy(request_envelope_input))
    route = route_runtime_operation({"operation_type": request_envelope["operation_type"]})
    security = orchestrate_runtime_security(deepcopy(security_input))
    guardrails = build_runtime_guardrail_orchestration(deepcopy(guardrail_input))
    retrieval = build_runtime_retrieval_orchestration(deepcopy(retrieval_input))
    commercial = build_runtime_commercial_orchestration(deepcopy(commercial_input))

    _ = build_runtime_response_envelope(
        {
            "request_id": request_envelope["request_id"],
            "client_id": request_envelope["client_id"],
            "operation_type": request_envelope["operation_type"],
            "status": "accepted",
            "result": {
                "request_envelope": request_envelope,
                "route": route,
                "security": security,
                "guardrails": guardrails,
                "retrieval": retrieval,
                "commercial": commercial,
            },
            "errors": [],
            "billing_enabled": request_envelope["billing_enabled"],
        }
    )

    assert request_envelope_input == before_request
    assert security_input == before_security
    assert guardrail_input == before_guardrail
    assert retrieval_input == before_retrieval
    assert commercial_input == before_commercial
