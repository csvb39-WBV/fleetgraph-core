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
    build_runtime_bootstrap_from_env_file,
    build_runtime_bootstrap_from_environment,
    build_runtime_bootstrap_summary,
)
from fleetgraph_core.runtime.runtime_external_api import build_runtime_external_api_response
from fleetgraph_core.runtime.runtime_health_api import build_runtime_health_response
import fleetgraph_core.runtime.runtime_http_api as runtime_http_api


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


client = TestClient(runtime_http_api.app)


def _set_runtime_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("FLEETGRAPH_RUNTIME_ENVIRONMENT", "development")
    monkeypatch.setenv("FLEETGRAPH_API_HOST", "127.0.0.1")
    monkeypatch.setenv("FLEETGRAPH_API_PORT", "8033")
    monkeypatch.setenv("FLEETGRAPH_DEBUG", "true")
    monkeypatch.setenv("FLEETGRAPH_LOG_LEVEL", "WARNING")


def _write_runtime_env_file(tmp_path: Path) -> Path:
    path = tmp_path / "deployment_readiness.env"
    path.write_text(
        "\n".join(
            [
                "FLEETGRAPH_RUNTIME_ENVIRONMENT=development",
                "FLEETGRAPH_API_HOST=127.0.0.1",
                "FLEETGRAPH_API_PORT=8033",
                "FLEETGRAPH_DEBUG=true",
                "FLEETGRAPH_LOG_LEVEL=WARNING",
            ]
        ),
        encoding="utf-8",
    )
    return path


def test_runtime_boundary_stack_imports_and_executes(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    _set_runtime_env(monkeypatch)
    env_file = _write_runtime_env_file(tmp_path)

    bootstrap_env = build_runtime_bootstrap_from_environment()
    bootstrap_file = build_runtime_bootstrap_from_env_file(env_file)

    summary_env = build_runtime_bootstrap_summary(bootstrap_env)
    summary_file = build_runtime_bootstrap_summary(bootstrap_file)
    external_env = build_runtime_external_api_response(bootstrap_env)
    health_env = build_runtime_health_response(bootstrap_env)

    assert summary_env == summary_file
    assert set(summary_env.keys()) == SUMMARY_KEYS
    assert set(external_env.keys()) == EXTERNAL_KEYS
    assert set(external_env["runtime"].keys()) == SUMMARY_KEYS
    assert set(health_env.keys()) == HEALTH_KEYS
    assert set(health_env["checks"].keys()) == HEALTH_CHECK_KEYS
    assert set(health_env["runtime"].keys()) == SUMMARY_KEYS


def test_runtime_http_app_exists_and_endpoints_return_200(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _set_runtime_env(monkeypatch)

    assert runtime_http_api.app is not None
    assert client.get("/runtime/summary").status_code == 200
    assert client.get("/runtime/external").status_code == 200
    assert client.get("/runtime/health").status_code == 200


def test_endpoint_payloads_align_with_direct_contract_calls(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _set_runtime_env(monkeypatch)

    bootstrap = build_runtime_bootstrap_from_environment()
    expected_summary = build_runtime_bootstrap_summary(bootstrap)
    expected_external = build_runtime_external_api_response(bootstrap)
    expected_health = build_runtime_health_response(bootstrap)

    response_summary = client.get("/runtime/summary").json()
    response_external = client.get("/runtime/external").json()
    response_health = client.get("/runtime/health").json()

    assert response_summary == expected_summary
    assert response_external == expected_external
    assert response_health == expected_health


def test_environment_unchanged_and_responses_deterministic(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _set_runtime_env(monkeypatch)
    env_before = dict(os.environ)

    summary_1 = client.get("/runtime/summary").json()
    summary_2 = client.get("/runtime/summary").json()
    external_1 = client.get("/runtime/external").json()
    external_2 = client.get("/runtime/external").json()
    health_1 = client.get("/runtime/health").json()
    health_2 = client.get("/runtime/health").json()

    assert summary_1 == summary_2
    assert external_1 == external_2
    assert health_1 == health_2
    assert dict(os.environ) == env_before


def test_environment_values_pass_through_exactly(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _set_runtime_env(monkeypatch)

    summary = client.get("/runtime/summary").json()
    external = client.get("/runtime/external").json()
    health = client.get("/runtime/health").json()

    assert summary["environment"] == "development"
    assert summary["api_host"] == "127.0.0.1"
    assert summary["api_port"] == 8033
    assert summary["debug"] is True
    assert summary["log_level"] == "WARNING"
    assert external["runtime"] == summary
    assert health["runtime"] == summary
