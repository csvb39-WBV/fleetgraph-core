from __future__ import annotations

import copy
import pathlib
import sys

from fastapi.testclient import TestClient


PROJECT_ROOT = pathlib.Path(__file__).resolve().parents[2]
SRC_ROOT = PROJECT_ROOT / "src"

if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))


import fleetgraph_core.api.api_app as api_app_module
from fleetgraph_core.api.api_app import app


client = TestClient(app)


def test_apply_endpoint_returns_exact_adapter_output(monkeypatch) -> None:
    expected = {
        "ok": True,
        "response": {
            "response_type": "analysis",
            "source_event_id": "EVT-001",
            "opportunity_count": 1,
            "opportunities": [],
        },
    }

    monkeypatch.setattr(
        api_app_module,
        "handle_single_record_request",
        lambda payload: copy.deepcopy(expected),
    )

    response = client.post(
        "/v1/apply",
        json={
            "response_type": "analysis",
            "record": {"event_id": "EVT-001"},
            "limit": None,
            "minimum_priority": None,
        },
    )

    assert response.status_code == 200
    assert response.json() == expected


def test_apply_endpoint_does_not_mutate_request_payload(monkeypatch) -> None:
    payload = {
        "response_type": "analysis",
        "record": {"event_id": "EVT-001"},
        "limit": None,
        "minimum_priority": None,
    }
    snapshot = copy.deepcopy(payload)

    monkeypatch.setattr(
        api_app_module,
        "handle_single_record_request",
        lambda request_payload: {"ok": True, "response": request_payload},
    )

    response = client.post("/v1/apply", json=payload)

    assert response.status_code == 200
    assert payload == snapshot


def test_apply_batch_endpoint_returns_exact_adapter_output(monkeypatch) -> None:
    expected = {
        "request_id": "REQ-001",
        "endpoint_id": "analysis",
        "batch_state": "completed",
        "results": [{"ok": True, "response": {"source_event_id": "EVT-001"}}],
        "record_count": 1,
        "success_count": 1,
        "failure_count": 0,
    }

    monkeypatch.setattr(
        api_app_module,
        "apply_batch_endpoint_request",
        lambda payload: copy.deepcopy(expected),
    )

    response = client.post(
        "/v1/apply/batch",
        json={
            "request_id": "REQ-001",
            "endpoint_id": "analysis",
            "records": [{"event_id": "EVT-001"}],
        },
    )

    assert response.status_code == 200
    assert response.json() == expected


def test_apply_batch_endpoint_uses_mb73_adapter(monkeypatch) -> None:
    captured = {}

    def _fake_apply_batch_endpoint_request(payload):
        captured["payload"] = copy.deepcopy(payload)
        return {
            "request_id": payload["request_id"],
            "endpoint_id": payload["endpoint_id"],
            "batch_state": "completed",
            "results": [],
            "record_count": len(payload["records"]),
            "success_count": len(payload["records"]),
            "failure_count": 0,
        }

    monkeypatch.setattr(
        api_app_module,
        "apply_batch_endpoint_request",
        _fake_apply_batch_endpoint_request,
    )

    payload = {
        "request_id": "REQ-123",
        "endpoint_id": "summary",
        "records": [{"event_id": "EVT-001"}, {"event_id": "EVT-002"}],
    }
    snapshot = copy.deepcopy(payload)

    response = client.post("/v1/apply/batch", json=payload)

    assert response.status_code == 200
    assert captured["payload"] == snapshot
    assert payload == snapshot


def test_health_endpoint_returns_exact_adapter_output(monkeypatch) -> None:
    expected = {
        "status": "ok",
        "system": "fleetgraph-core",
        "timestamp": "2026-03-25T12:00:00+00:00",
    }

    monkeypatch.setattr(
        api_app_module,
        "get_health_status",
        lambda: copy.deepcopy(expected),
    )

    response = client.get("/v1/health")

    assert response.status_code == 200
    assert response.json() == expected


def test_version_endpoint_returns_exact_adapter_output(monkeypatch) -> None:
    expected = {
        "version": "1.0.0",
        "api_version": "v1",
        "build": "cti-w13",
    }

    monkeypatch.setattr(
        api_app_module,
        "get_version_info",
        lambda: copy.deepcopy(expected),
    )

    response = client.get("/v1/version")

    assert response.status_code == 200
    assert response.json() == expected
