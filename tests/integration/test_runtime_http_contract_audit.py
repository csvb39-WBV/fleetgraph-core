from __future__ import annotations

import os
import sys
from pathlib import Path

from fastapi.testclient import TestClient
import pytest


REPO_ROOT = Path(__file__).resolve().parents[2]
SRC_ROOT = REPO_ROOT / "src"

if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))


from fleetgraph_core.runtime.runtime_bootstrap import (
    build_runtime_bootstrap_from_environment,
    build_runtime_bootstrap_summary,
)
from fleetgraph_core.runtime.runtime_external_api import build_runtime_external_api_response
from fleetgraph_core.runtime.runtime_health_api import build_runtime_health_response
import fleetgraph_core.runtime.runtime_http_api as runtime_http_api


client = TestClient(runtime_http_api.app)

SUMMARY_KEYS = {
    "environment",
    "api_host",
    "api_port",
    "debug",
    "log_level",
    "logger_name",
    "logger_level",
}
EXTERNAL_KEYS = {"response_type", "response_schema_version", "runtime"}
HEALTH_KEYS = {"response_type", "response_schema_version", "status", "checks", "runtime"}
HEALTH_CHECK_KEYS = {"config_valid", "logger_ready"}


def _set_runtime_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("FLEETGRAPH_RUNTIME_ENVIRONMENT", "production")
    monkeypatch.setenv("FLEETGRAPH_API_HOST", "10.10.10.10")
    monkeypatch.setenv("FLEETGRAPH_API_PORT", "8123")
    monkeypatch.setenv("FLEETGRAPH_DEBUG", "on")
    monkeypatch.setenv("FLEETGRAPH_LOG_LEVEL", "ERROR")


def test_endpoint_availability(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_runtime_env(monkeypatch)

    assert client.get("/runtime/summary").status_code == 200
    assert client.get("/runtime/external").status_code == 200
    assert client.get("/runtime/health").status_code == 200


def test_endpoint_payloads_match_direct_contract_calls(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_runtime_env(monkeypatch)

    bootstrap = build_runtime_bootstrap_from_environment()
    expected_summary = build_runtime_bootstrap_summary(bootstrap)
    expected_external = build_runtime_external_api_response(bootstrap)
    expected_health = build_runtime_health_response(bootstrap)

    actual_summary = client.get("/runtime/summary").json()
    actual_external = client.get("/runtime/external").json()
    actual_health = client.get("/runtime/health").json()

    assert actual_summary == expected_summary
    assert actual_external == expected_external
    assert actual_health == expected_health


def test_error_handling_returns_locked_500_contract(monkeypatch: pytest.MonkeyPatch) -> None:
    def _raise_error() -> None:
        raise ValueError("forced audit failure")

    monkeypatch.setattr(runtime_http_api, "build_runtime_bootstrap_from_environment", _raise_error)

    for path in ("/runtime/summary", "/runtime/external", "/runtime/health"):
        response = client.get(path)
        assert response.status_code == 500
        assert response.json() == {
            "error": "runtime_http_api_error",
            "message": "forced audit failure",
        }


def test_exact_key_sets_for_all_http_contracts(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_runtime_env(monkeypatch)

    summary = client.get("/runtime/summary").json()
    external = client.get("/runtime/external").json()
    health = client.get("/runtime/health").json()

    assert set(summary.keys()) == SUMMARY_KEYS
    assert set(external.keys()) == EXTERNAL_KEYS
    assert set(external["runtime"].keys()) == SUMMARY_KEYS
    assert set(health.keys()) == HEALTH_KEYS
    assert set(health["checks"].keys()) == HEALTH_CHECK_KEYS
    assert set(health["runtime"].keys()) == SUMMARY_KEYS


def test_http_responses_are_deterministic(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_runtime_env(monkeypatch)

    summary_first = client.get("/runtime/summary").json()
    summary_second = client.get("/runtime/summary").json()
    external_first = client.get("/runtime/external").json()
    external_second = client.get("/runtime/external").json()
    health_first = client.get("/runtime/health").json()
    health_second = client.get("/runtime/health").json()

    assert summary_first == summary_second
    assert external_first == external_second
    assert health_first == health_second


def test_environment_driven_values_pass_through(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_runtime_env(monkeypatch)

    summary = client.get("/runtime/summary").json()
    external = client.get("/runtime/external").json()
    health = client.get("/runtime/health").json()

    assert summary["environment"] == "production"
    assert summary["api_host"] == "10.10.10.10"
    assert summary["api_port"] == 8123
    assert summary["debug"] is True
    assert summary["log_level"] == "ERROR"
    assert external["runtime"] == summary
    assert health["runtime"] == summary


def test_environment_not_mutated_by_http_calls(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_runtime_env(monkeypatch)
    env_before = dict(os.environ)

    client.get("/runtime/summary")
    client.get("/runtime/external")
    client.get("/runtime/health")

    assert dict(os.environ) == env_before
