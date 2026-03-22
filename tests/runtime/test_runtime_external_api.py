from __future__ import annotations

import os
import sys
from pathlib import Path

import pytest


REPO_ROOT = Path(__file__).resolve().parents[2]
SRC_ROOT = REPO_ROOT / "src"

if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))


from fleetgraph_core.runtime.runtime_bootstrap import (
    build_runtime_bootstrap,
    build_runtime_bootstrap_from_env_file,
    build_runtime_bootstrap_from_environment,
    build_runtime_bootstrap_summary,
)
from fleetgraph_core.runtime.runtime_external_api import (
    build_runtime_external_api_response,
)


def _build_direct_bootstrap():
    return build_runtime_bootstrap(
        {
            "environment": "development",
            "api_host": "127.0.0.1",
            "api_port": 8000,
            "debug": True,
            "log_level": "DEBUG",
        }
    )


def test_environment_summary_returns_exact_locked_field_set(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("FLEETGRAPH_RUNTIME_ENVIRONMENT", "staging")
    monkeypatch.setenv("FLEETGRAPH_API_HOST", "0.0.0.0")
    monkeypatch.setenv("FLEETGRAPH_API_PORT", "8000")
    monkeypatch.setenv("FLEETGRAPH_DEBUG", "false")
    monkeypatch.setenv("FLEETGRAPH_LOG_LEVEL", "INFO")

    bootstrap = build_runtime_bootstrap_from_environment()
    response = build_runtime_external_api_response(bootstrap)

    assert set(response.keys()) == {
        "response_type",
        "response_schema_version",
        "runtime",
    }
    assert set(response["runtime"].keys()) == {
        "environment",
        "api_host",
        "api_port",
        "debug",
        "log_level",
        "logger_name",
        "logger_level",
    }


def test_env_file_summary_returns_exact_locked_field_set(tmp_path: Path) -> None:
    env_file = tmp_path / "runtime.env"
    env_file.write_text(
        "\n".join(
            [
                "FLEETGRAPH_RUNTIME_ENVIRONMENT=production",
                "FLEETGRAPH_API_HOST=0.0.0.0",
                "FLEETGRAPH_API_PORT=8000",
                "FLEETGRAPH_DEBUG=false",
                "FLEETGRAPH_LOG_LEVEL=INFO",
            ]
        ),
        encoding="utf-8",
    )

    bootstrap = build_runtime_bootstrap_from_env_file(env_file)
    response = build_runtime_external_api_response(bootstrap)

    assert set(response.keys()) == {
        "response_type",
        "response_schema_version",
        "runtime",
    }
    assert set(response["runtime"].keys()) == {
        "environment",
        "api_host",
        "api_port",
        "debug",
        "log_level",
        "logger_name",
        "logger_level",
    }


def test_values_are_correct_for_environment_driven_bootstrap(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("FLEETGRAPH_RUNTIME_ENVIRONMENT", "staging")
    monkeypatch.setenv("FLEETGRAPH_API_HOST", "0.0.0.0")
    monkeypatch.setenv("FLEETGRAPH_API_PORT", "8000")
    monkeypatch.setenv("FLEETGRAPH_DEBUG", "false")
    monkeypatch.setenv("FLEETGRAPH_LOG_LEVEL", "INFO")

    bootstrap = build_runtime_bootstrap_from_environment()
    response = build_runtime_external_api_response(bootstrap)

    assert response["response_type"] == "runtime_external_api_response"
    assert response["response_schema_version"] == "1.0"
    assert response["runtime"] == {
        "environment": "staging",
        "api_host": "0.0.0.0",
        "api_port": 8000,
        "debug": False,
        "log_level": "INFO",
        "logger_name": "fleetgraph.runtime.staging",
        "logger_level": "INFO",
    }


def test_values_are_correct_for_env_file_bootstrap(tmp_path: Path) -> None:
    env_file = tmp_path / "runtime.env"
    env_file.write_text(
        "\n".join(
            [
                "FLEETGRAPH_RUNTIME_ENVIRONMENT=development",
                "FLEETGRAPH_API_HOST=127.0.0.1",
                "FLEETGRAPH_API_PORT=8000",
                "FLEETGRAPH_DEBUG=true",
                "FLEETGRAPH_LOG_LEVEL=DEBUG",
            ]
        ),
        encoding="utf-8",
    )

    bootstrap = build_runtime_bootstrap_from_env_file(env_file)
    response = build_runtime_external_api_response(bootstrap)

    assert response["response_type"] == "runtime_external_api_response"
    assert response["response_schema_version"] == "1.0"
    assert response["runtime"] == {
        "environment": "development",
        "api_host": "127.0.0.1",
        "api_port": 8000,
        "debug": True,
        "log_level": "DEBUG",
        "logger_name": "fleetgraph.runtime.development",
        "logger_level": "DEBUG",
    }


def test_source_of_truth_alignment_with_bootstrap_summary() -> None:
    bootstrap = _build_direct_bootstrap()

    response = build_runtime_external_api_response(bootstrap)
    summary = build_runtime_bootstrap_summary(bootstrap)

    assert response["runtime"] == summary


def test_repeated_environment_calls_are_deterministic(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("FLEETGRAPH_RUNTIME_ENVIRONMENT", "production")
    monkeypatch.setenv("FLEETGRAPH_API_HOST", "0.0.0.0")
    monkeypatch.setenv("FLEETGRAPH_API_PORT", "8000")
    monkeypatch.setenv("FLEETGRAPH_DEBUG", "false")
    monkeypatch.setenv("FLEETGRAPH_LOG_LEVEL", "ERROR")

    bootstrap = build_runtime_bootstrap_from_environment()

    first = build_runtime_external_api_response(bootstrap)
    second = build_runtime_external_api_response(bootstrap)

    assert first == second


def test_repeated_env_file_calls_are_deterministic(tmp_path: Path) -> None:
    env_file = tmp_path / "runtime.env"
    env_file.write_text(
        "\n".join(
            [
                "FLEETGRAPH_RUNTIME_ENVIRONMENT=staging",
                "FLEETGRAPH_API_HOST=0.0.0.0",
                "FLEETGRAPH_API_PORT=8000",
                "FLEETGRAPH_DEBUG=false",
                "FLEETGRAPH_LOG_LEVEL=INFO",
            ]
        ),
        encoding="utf-8",
    )

    bootstrap = build_runtime_bootstrap_from_env_file(env_file)

    first = build_runtime_external_api_response(bootstrap)
    second = build_runtime_external_api_response(bootstrap)

    assert first == second


def test_invalid_env_file_path_raises_file_error(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError):
        build_runtime_bootstrap_from_env_file(tmp_path / "missing.env")


def test_invalid_env_file_content_propagates_failure_explicitly(tmp_path: Path) -> None:
    env_file = tmp_path / "runtime.env"
    env_file.write_text("FLEETGRAPH_DEBUG=maybe", encoding="utf-8")

    with pytest.raises(ValueError, match="debug must be one of"):
        build_runtime_bootstrap_from_env_file(env_file)


def test_environment_not_mutated_by_env_file_path_call(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv("FLEETGRAPH_RUNTIME_ENVIRONMENT", "staging")
    env_before = dict(os.environ)

    env_file = tmp_path / "runtime.env"
    env_file.write_text(
        "\n".join(
            [
                "FLEETGRAPH_RUNTIME_ENVIRONMENT=production",
                "FLEETGRAPH_API_HOST=0.0.0.0",
                "FLEETGRAPH_API_PORT=8000",
                "FLEETGRAPH_DEBUG=false",
                "FLEETGRAPH_LOG_LEVEL=INFO",
            ]
        ),
        encoding="utf-8",
    )

    bootstrap = build_runtime_bootstrap_from_env_file(env_file)
    _ = build_runtime_external_api_response(bootstrap)

    assert dict(os.environ) == env_before


def test_logger_level_text_is_canonical_uppercase() -> None:
    bootstrap = build_runtime_bootstrap(
        {
            "environment": "production",
            "api_host": "0.0.0.0",
            "api_port": 8000,
            "debug": False,
            "log_level": "warning",
        }
    )

    response = build_runtime_external_api_response(bootstrap)

    assert response["runtime"]["logger_level"] == "WARNING"


def test_invalid_input_raises_value_error() -> None:
    with pytest.raises(ValueError, match="bootstrap must be a RuntimeBootstrap instance"):
        build_runtime_external_api_response("not-bootstrap")  # type: ignore[arg-type]


def test_bootstrap_not_mutated_after_response_generation() -> None:
    bootstrap = _build_direct_bootstrap()

    config_before = bootstrap.config
    logger_name_before = bootstrap.logger.name
    logger_level_before = bootstrap.logger.level

    _ = build_runtime_external_api_response(bootstrap)

    assert bootstrap.config == config_before
    assert bootstrap.logger.name == logger_name_before
    assert bootstrap.logger.level == logger_level_before
