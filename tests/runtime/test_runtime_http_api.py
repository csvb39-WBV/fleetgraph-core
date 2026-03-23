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
    build_runtime_bootstrap,
    build_runtime_bootstrap_from_environment,
    build_runtime_bootstrap_summary,
)
from fleetgraph_core.runtime.runtime_external_api import (
    build_runtime_external_api_response,
)
from fleetgraph_core.runtime.runtime_health_api import (
    build_runtime_health_response,
)
from fleetgraph_core.runtime.runtime_metrics_layer import (
    build_runtime_metrics_response,
)
from fleetgraph_core.runtime.runtime_readiness_layer import (
    build_runtime_readiness_response,
)
import fleetgraph_core.runtime.runtime_http_api as runtime_http_api


client = TestClient(runtime_http_api.app)


def _set_runtime_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("FLEETGRAPH_RUNTIME_ENVIRONMENT", "staging")
    monkeypatch.setenv("FLEETGRAPH_API_HOST", "0.0.0.0")
    monkeypatch.setenv("FLEETGRAPH_API_PORT", "8000")
    monkeypatch.setenv("FLEETGRAPH_DEBUG", "false")
    monkeypatch.setenv("FLEETGRAPH_LOG_LEVEL", "INFO")


def test_runtime_summary_endpoint_available(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_runtime_environment(monkeypatch)

    response = client.get("/runtime/summary")

    assert response.status_code == 200


def test_runtime_external_endpoint_available(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_runtime_environment(monkeypatch)

    response = client.get("/runtime/external")

    assert response.status_code == 200


def test_runtime_health_endpoint_available(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_runtime_environment(monkeypatch)

    response = client.get("/runtime/health")

    assert response.status_code == 200


def test_runtime_metrics_endpoint_available(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_runtime_environment(monkeypatch)

    response = client.get("/runtime/metrics")

    assert response.status_code == 200


def test_runtime_readiness_endpoint_available(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_runtime_environment(monkeypatch)

    response = client.get("/runtime/readiness")

    assert response.status_code == 200


def test_summary_response_exact_match_with_contract(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_runtime_environment(monkeypatch)

    expected = build_runtime_bootstrap_summary(build_runtime_bootstrap_from_environment())

    response = client.get("/runtime/summary")

    assert response.status_code == 200
    assert response.json() == expected


def test_external_response_exact_match_with_contract(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_runtime_environment(monkeypatch)

    expected = build_runtime_external_api_response(build_runtime_bootstrap_from_environment())

    response = client.get("/runtime/external")

    assert response.status_code == 200
    assert response.json() == expected


def test_health_response_exact_match_with_contract(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_runtime_environment(monkeypatch)

    expected = build_runtime_health_response(build_runtime_bootstrap_from_environment())

    response = client.get("/runtime/health")

    assert response.status_code == 200
    assert response.json() == expected


def test_metrics_response_exact_match_with_contract(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_runtime_environment(monkeypatch)

    expected = build_runtime_metrics_response(build_runtime_bootstrap_from_environment())

    response = client.get("/runtime/metrics")

    assert response.status_code == 200
    assert response.json() == expected


def test_readiness_response_exact_match_with_contract(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_runtime_environment(monkeypatch)

    response = client.get("/runtime/readiness")

    assert response.status_code == 200
    assert response.json() == {
        "status": "ready",
        "checks": {
            "config_loaded": True,
            "bootstrap_complete": True,
        },
    }


def test_endpoints_are_deterministic(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_runtime_environment(monkeypatch)

    summary_first = client.get("/runtime/summary").json()
    summary_second = client.get("/runtime/summary").json()
    external_first = client.get("/runtime/external").json()
    external_second = client.get("/runtime/external").json()
    health_first = client.get("/runtime/health").json()
    health_second = client.get("/runtime/health").json()
    metrics_first = client.get("/runtime/metrics").json()
    metrics_second = client.get("/runtime/metrics").json()

    assert summary_first == summary_second
    assert external_first == external_second
    assert health_first == health_second
    assert metrics_first == metrics_second


def test_readiness_endpoint_is_deterministic(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_runtime_environment(monkeypatch)

    first = client.get("/runtime/readiness").json()
    second = client.get("/runtime/readiness").json()

    assert first == second


def test_environment_driven_values_reflected(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("FLEETGRAPH_RUNTIME_ENVIRONMENT", "production")
    monkeypatch.setenv("FLEETGRAPH_API_HOST", "10.30.40.50")
    monkeypatch.setenv("FLEETGRAPH_API_PORT", "8123")
    monkeypatch.setenv("FLEETGRAPH_DEBUG", "on")
    monkeypatch.setenv("FLEETGRAPH_LOG_LEVEL", "ERROR")

    summary_response = client.get("/runtime/summary")
    external_response = client.get("/runtime/external")
    health_response = client.get("/runtime/health")
    metrics_response = client.get("/runtime/metrics")

    assert summary_response.json()["environment"] == "production"
    assert summary_response.json()["api_host"] == "10.30.40.50"
    assert summary_response.json()["api_port"] == 8123
    assert summary_response.json()["debug"] is True
    assert summary_response.json()["log_level"] == "ERROR"
    assert external_response.json()["runtime"]["environment"] == "production"
    assert health_response.json()["runtime"]["environment"] == "production"
    assert metrics_response.json()["health_alignment"]["health_endpoint_status"] == "healthy"


def test_readiness_not_ready_path_via_monkeypatch(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_runtime_environment(monkeypatch)

    from fleetgraph_core.runtime import runtime_readiness_layer

    monkeypatch.setattr(
        runtime_readiness_layer,
        "build_runtime_readiness_response",
        lambda _state: {
            "status": "not_ready",
            "checks": {
                "config_loaded": False,
                "bootstrap_complete": True,
            },
        },
    )
    monkeypatch.setattr(
        runtime_http_api,
        "build_runtime_readiness_response",
        lambda _state: {
            "status": "not_ready",
            "checks": {
                "config_loaded": False,
                "bootstrap_complete": True,
            },
        },
    )

    response = client.get("/runtime/readiness")

    assert response.status_code == 200
    assert response.json() == {
        "status": "not_ready",
        "checks": {
            "config_loaded": False,
            "bootstrap_complete": True,
        },
    }


def test_error_propagation_returns_http_500_contract(monkeypatch: pytest.MonkeyPatch) -> None:
    def _raise_error() -> None:
        raise ValueError("forced failure")

    monkeypatch.setattr(runtime_http_api, "build_runtime_bootstrap_from_environment", _raise_error)

    for path in ("/runtime/summary", "/runtime/external", "/runtime/health", "/runtime/metrics", "/runtime/readiness"):
        response = client.get(path)
        assert response.status_code == 500
        assert response.json() == {
            "error": "runtime_http_api_error",
            "message": "forced failure",
        }


def test_bootstrap_not_mutated(monkeypatch: pytest.MonkeyPatch) -> None:
    bootstrap = build_runtime_bootstrap(
        {
            "environment": "development",
            "api_host": "127.0.0.1",
            "api_port": 8000,
            "debug": True,
            "log_level": "DEBUG",
        }
    )

    config_before = bootstrap.config
    logger_name_before = bootstrap.logger.name
    logger_level_before = bootstrap.logger.level

    monkeypatch.setattr(
        runtime_http_api,
        "build_runtime_bootstrap_from_environment",
        lambda: bootstrap,
    )

    client.get("/runtime/summary")
    client.get("/runtime/external")
    client.get("/runtime/health")
    client.get("/runtime/metrics")
    client.get("/runtime/readiness")

    assert bootstrap.config == config_before
    assert bootstrap.logger.name == logger_name_before
    assert bootstrap.logger.level == logger_level_before


def test_environment_not_mutated(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_runtime_environment(monkeypatch)
    env_before = dict(os.environ)

    client.get("/runtime/summary")
    client.get("/runtime/external")
    client.get("/runtime/health")
    client.get("/runtime/metrics")
    client.get("/runtime/readiness")

    assert dict(os.environ) == env_before


def test_contract_integrity_exact_keys(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_runtime_environment(monkeypatch)

    summary = client.get("/runtime/summary").json()
    external = client.get("/runtime/external").json()
    health = client.get("/runtime/health").json()
    metrics = client.get("/runtime/metrics").json()

    assert set(summary.keys()) == {
        "environment",
        "api_host",
        "api_port",
        "debug",
        "log_level",
        "logger_name",
        "logger_level",
    }
    assert set(external.keys()) == {"response_type", "response_schema_version", "runtime"}
    assert set(external["runtime"].keys()) == {
        "environment",
        "api_host",
        "api_port",
        "debug",
        "log_level",
        "logger_name",
        "logger_level",
    }
    assert set(health.keys()) == {
        "response_type",
        "response_schema_version",
        "status",
        "checks",
        "runtime",
    }
    assert set(health["checks"].keys()) == {"config_valid", "logger_ready"}
    assert set(health["runtime"].keys()) == {
        "environment",
        "api_host",
        "api_port",
        "debug",
        "log_level",
        "logger_name",
        "logger_level",
    }
    assert set(metrics.keys()) == {
        "response_type",
        "response_schema_version",
        "runtime_metrics",
        "request_metrics",
        "error_metrics",
        "health_alignment",
    }
    assert set(metrics["runtime_metrics"].keys()) == {
        "startup_success",
        "runtime_status",
    }
    assert set(metrics["request_metrics"].keys()) == {
        "request_count_total",
        "request_success_count",
        "request_failure_count",
    }
    assert set(metrics["error_metrics"].keys()) == {
        "exception_count",
        "failure_event_count",
    }
    assert set(metrics["health_alignment"].keys()) == {
        "health_endpoint_status",
        "health_is_healthy",
    }

    readiness = client.get("/runtime/readiness").json()
    assert set(readiness.keys()) == {"status", "checks"}
    assert set(readiness["checks"].keys()) == {"config_loaded", "bootstrap_complete"}
