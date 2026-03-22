from __future__ import annotations

import sys
from pathlib import Path

import pytest


REPO_ROOT = Path(__file__).resolve().parents[2]
SRC_ROOT = REPO_ROOT / "src"

if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))


from fleetgraph_core.runtime.runtime_bootstrap import build_runtime_bootstrap
from fleetgraph_core.runtime.runtime_external_api import build_runtime_external_api_response
import fleetgraph_core.runtime.runtime_health_api as runtime_health_api


def _build_bootstrap():
    return build_runtime_bootstrap(
        {
            "environment": "development",
            "api_host": "127.0.0.1",
            "api_port": 8000,
            "debug": True,
            "log_level": "DEBUG",
        }
    )


def test_health_response_has_exact_structure() -> None:
    bootstrap = _build_bootstrap()

    response = runtime_health_api.build_runtime_health_response(bootstrap)

    assert tuple(response.keys()) == (
        "response_type",
        "response_schema_version",
        "status",
        "checks",
        "runtime",
    )
    assert tuple(response["checks"].keys()) == ("config_valid", "logger_ready")
    assert tuple(response["runtime"].keys()) == (
        "environment",
        "api_host",
        "api_port",
        "debug",
        "log_level",
        "logger_name",
        "logger_level",
    )


def test_health_response_healthy_case() -> None:
    bootstrap = _build_bootstrap()

    response = runtime_health_api.build_runtime_health_response(bootstrap)

    assert response["response_type"] == "runtime_health_response"
    assert response["response_schema_version"] == "1.0"
    assert response["checks"] == {"config_valid": True, "logger_ready": True}
    assert response["status"] == "healthy"


def test_health_response_degraded_for_invalid_logger_name(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    bootstrap = _build_bootstrap()
    external_response = build_runtime_external_api_response(bootstrap)
    external_response["runtime"]["logger_name"] = ""

    monkeypatch.setattr(
        runtime_health_api,
        "build_runtime_external_api_response",
        lambda _: external_response,
    )

    response = runtime_health_api.build_runtime_health_response(bootstrap)

    assert response["checks"]["config_valid"] is True
    assert response["checks"]["logger_ready"] is False
    assert response["status"] == "degraded"


def test_health_response_degraded_for_invalid_environment_override(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    bootstrap = _build_bootstrap()
    external_response = build_runtime_external_api_response(bootstrap)
    external_response["runtime"]["environment"] = "qa"

    monkeypatch.setattr(
        runtime_health_api,
        "build_runtime_external_api_response",
        lambda _: external_response,
    )

    response = runtime_health_api.build_runtime_health_response(bootstrap)

    assert response["checks"]["config_valid"] is False
    assert response["status"] == "degraded"


def test_health_response_is_deterministic() -> None:
    bootstrap = _build_bootstrap()

    first = runtime_health_api.build_runtime_health_response(bootstrap)
    second = runtime_health_api.build_runtime_health_response(bootstrap)

    assert first == second


def test_health_response_runtime_payload_aligns_with_external_api() -> None:
    bootstrap = _build_bootstrap()

    health_response = runtime_health_api.build_runtime_health_response(bootstrap)
    external_response = build_runtime_external_api_response(bootstrap)

    assert health_response["runtime"] == external_response["runtime"]


def test_health_response_does_not_mutate_bootstrap() -> None:
    bootstrap = _build_bootstrap()

    config_before = bootstrap.config
    logger_name_before = bootstrap.logger.name
    logger_level_before = bootstrap.logger.level

    _ = runtime_health_api.build_runtime_health_response(bootstrap)

    assert bootstrap.config == config_before
    assert bootstrap.logger.name == logger_name_before
    assert bootstrap.logger.level == logger_level_before


def test_health_response_invalid_input_raises_value_error() -> None:
    with pytest.raises(ValueError, match="bootstrap must be a RuntimeBootstrap instance"):
        runtime_health_api.build_runtime_health_response("invalid")  # type: ignore[arg-type]


def test_health_response_logger_level_is_canonical_uppercase() -> None:
    bootstrap = build_runtime_bootstrap(
        {
            "environment": "production",
            "api_host": "0.0.0.0",
            "api_port": 8000,
            "debug": False,
            "log_level": "warning",
        }
    )

    response = runtime_health_api.build_runtime_health_response(bootstrap)

    assert response["runtime"]["logger_level"] == "WARNING"
