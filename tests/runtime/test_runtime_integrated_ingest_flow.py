from __future__ import annotations

from copy import deepcopy

from fleetgraph_core.runtime.runtime_commercial_orchestrator import (
    build_runtime_commercial_orchestration,
)
from fleetgraph_core.runtime.runtime_guardrail_orchestrator import (
    build_runtime_guardrail_orchestration,
)
from fleetgraph_core.runtime.runtime_ingest_orchestrator import (
    build_runtime_ingest_orchestration,
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


def _valid_integrated_input() -> dict[str, object]:
    return {
        "request_envelope_input": {
            "request_id": "req-1001",
            "client_id": "client-001",
            "api_key": "key-1",
            "operation_type": "ingest",
            "payload": {
                "batch_input": {
                    "batch_id": "batch-001",
                    "documents": [
                        {"document_id": "doc-001", "content": "content-1"},
                        {"document_id": "doc-002", "content": "content-2"},
                    ],
                },
                "manifest_input": {
                    "matter_id": "matter-001",
                    "document_set_version": "v1",
                    "ingestion_run_id": "run-001",
                    "pipeline_version": "p1",
                    "schema_version": "s1",
                    "source_hash": "hash-001",
                    "artifact_keys": ["events", "entities", "relationships"],
                },
                "artifact_input": {
                    "manifest": {
                        "matter_id": "matter-001",
                        "document_set_version": "v1",
                        "ingestion_run_id": "run-001",
                        "pipeline_version": "p1",
                        "schema_version": "s1",
                        "source_hash": "hash-001",
                        "artifact_keys": ["events", "entities", "relationships"],
                    },
                    "artifacts": {
                        "entities": [{"entity_id": "e2"}, {"entity_id": "e1"}],
                        "events": [{"event_id": "ev2"}, {"event_id": "ev1"}],
                        "relationships": [{"relationship_id": "r2"}, {"relationship_id": "r1"}],
                        "evidence_links": [{"evidence_id": "el1"}],
                        "graph_artifacts": [{"graph_id": "g1"}],
                    },
                },
            },
            "runtime_limits": {
                "max_payload_size_bytes": 8192,
                "max_document_count": 10,
                "max_graph_depth": 4,
                "max_result_count": 100,
                "max_relationship_expansion_count": 50,
                "max_evidence_link_count": 75,
                "max_requests_per_window": 100,
            },
            "billing_enabled": True,
        },
        "security_input": {
            "auth_input": {
                "provided_api_key": "key-1",
                "authorized_api_keys": ["key-1", "key-2"],
            },
            "validation_input": {
                "request_id": "req-1001",
                "endpoint": "/runtime/ingest",
                "content_type": "application/json",
                "payload_size_bytes": 1024,
                "max_payload_size_bytes": 8192,
                "has_body": True,
            },
            "rate_limit_input": {
                "client_id": "client-001",
                "request_count_in_window": 4,
                "max_requests_per_window": 100,
                "window_active": True,
            },
        },
        "guardrail_input": {
            "resource_guardrail_input": {
                "payload_size_bytes": 1024,
                "document_count": 2,
                "requested_graph_depth": 2,
                "max_payload_size_bytes": 8192,
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


def _run_integrated_ingest_flow(test_input: dict[str, object]) -> dict[str, object]:
    request_envelope = build_runtime_request_envelope(
        deepcopy(test_input["request_envelope_input"])
    )

    route_result = route_runtime_operation(
        {"operation_type": request_envelope["operation_type"]}
    )

    security_result = orchestrate_runtime_security(deepcopy(test_input["security_input"]))
    guardrail_result = build_runtime_guardrail_orchestration(
        deepcopy(test_input["guardrail_input"])
    )

    ingest_result = build_runtime_ingest_orchestration(
        {
            "batch_input": deepcopy(request_envelope["payload"]["batch_input"]),
            "manifest_input": deepcopy(request_envelope["payload"]["manifest_input"]),
            "artifact_input": deepcopy(request_envelope["payload"]["artifact_input"]),
        }
    )

    document_count = ingest_result["result"]["batch_envelope"]["document_count"]
    data_processed_bytes = sum(
        len(document["content"])
        for document in request_envelope["payload"]["batch_input"]["documents"]
    )

    commercial_result = build_runtime_commercial_orchestration(
        {
            "usage_metering_input": {
                "client_id": request_envelope["client_id"],
                "request_id": request_envelope["request_id"],
                "operation_type": request_envelope["operation_type"],
                "document_count": document_count,
                "data_processed_bytes": data_processed_bytes,
            },
            "billing_hooks_input": {
                "usage_record": {
                    "client_id": "placeholder",
                    "request_id": "placeholder",
                    "operation_type": "retrieve",
                    "document_count": 1,
                    "data_processed_bytes": 1,
                },
                "billing_enabled": request_envelope["billing_enabled"],
            },
        }
    )

    return build_runtime_response_envelope(
        {
            "request_id": request_envelope["request_id"],
            "client_id": request_envelope["client_id"],
            "operation_type": route_result["route"],
            "status": "accepted",
            "result": {
                "route": route_result,
                "security": security_result,
                "guardrails": guardrail_result,
                "ingest": ingest_result,
                "commercial": commercial_result,
            },
            "errors": [],
            "billing_enabled": request_envelope["billing_enabled"],
        }
    )


def test_runtime_integrated_ingest_flow_is_deterministic_and_non_mutating() -> None:
    test_input = _valid_integrated_input()
    before = deepcopy(test_input)

    first = _run_integrated_ingest_flow(test_input)
    second = _run_integrated_ingest_flow(test_input)

    assert first == second
    assert test_input == before


def test_runtime_integrated_ingest_flow_returns_expected_accepted_response() -> None:
    final_response = _run_integrated_ingest_flow(_valid_integrated_input())

    assert final_response["status"] == "accepted"
    assert final_response["result"]["ingest"]["status"] == "completed"
    assert final_response["result"]["commercial"]["status"] == "completed"
    assert tuple(final_response.keys()) == (
        "request_id",
        "client_id",
        "operation_type",
        "status",
        "result",
        "errors",
        "billing_enabled",
    )
