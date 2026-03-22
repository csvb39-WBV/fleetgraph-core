from __future__ import annotations

import logging
import os
import sys
from pathlib import Path

import pytest


REPO_ROOT = Path(__file__).resolve().parents[2]
SRC_ROOT = REPO_ROOT / "src"

if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))


from fleetgraph_core.runtime.runtime_bootstrap import (
    RuntimeBootstrap,
    build_runtime_bootstrap,
    build_runtime_bootstrap_from_env_file,
    build_runtime_bootstrap_from_environment,
)


def test_build_runtime_bootstrap_returns_expected_object() -> None:
    bootstrap = build_runtime_bootstrap(
        {
            "environment": "development",
            "api_host": "127.0.0.1",
            "api_port": 8000,
            "debug": True,
            "log_level": "debug",
        }
    )

    assert isinstance(bootstrap, RuntimeBootstrap)
    assert bootstrap.config.environment == "development"
    assert bootstrap.config.api_host == "127.0.0.1"
    assert bootstrap.config.api_port == 8000
    assert bootstrap.config.debug is True
    assert bootstrap.config.log_level == "DEBUG"
    assert isinstance(bootstrap.logger, logging.Logger)
    assert bootstrap.logger.name == "fleetgraph.runtime.development"
    assert bootstrap.logger.level == logging.DEBUG


def test_build_runtime_bootstrap_from_environment_succeeds(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("FLEETGRAPH_RUNTIME_ENVIRONMENT", "staging")
    monkeypatch.setenv("FLEETGRAPH_API_HOST", "0.0.0.0")
    monkeypatch.setenv("FLEETGRAPH_API_PORT", "8000")
    monkeypatch.setenv("FLEETGRAPH_DEBUG", "false")
    monkeypatch.setenv("FLEETGRAPH_LOG_LEVEL", "INFO")

    bootstrap = build_runtime_bootstrap_from_environment()

    assert isinstance(bootstrap, RuntimeBootstrap)
    assert bootstrap.config.environment == "staging"
    assert bootstrap.config.api_host == "0.0.0.0"
    assert bootstrap.config.api_port == 8000
    assert bootstrap.config.debug is False
    assert bootstrap.config.log_level == "INFO"
    assert bootstrap.logger.name == "fleetgraph.runtime.staging"
    assert bootstrap.logger.level == logging.INFO


def test_build_runtime_bootstrap_from_env_file_succeeds(tmp_path: Path) -> None:
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

    assert isinstance(bootstrap, RuntimeBootstrap)
    assert bootstrap.config.environment == "production"
    assert bootstrap.config.api_host == "0.0.0.0"
    assert bootstrap.config.api_port == 8000
    assert bootstrap.config.debug is False
    assert bootstrap.config.log_level == "INFO"
    assert bootstrap.logger.name == "fleetgraph.runtime.production"
    assert bootstrap.logger.level == logging.INFO


def test_build_runtime_bootstrap_from_env_file_invalid_config_fails_fast(tmp_path: Path) -> None:
    env_file = tmp_path / "runtime.env"
    env_file.write_text(
        "\n".join(
            [
                "FLEETGRAPH_RUNTIME_ENVIRONMENT=production",
                "FLEETGRAPH_API_HOST=0.0.0.0",
                "FLEETGRAPH_API_PORT=8000",
                "FLEETGRAPH_DEBUG=maybe",
                "FLEETGRAPH_LOG_LEVEL=INFO",
            ]
        ),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="debug must be one of"):
        build_runtime_bootstrap_from_env_file(env_file)


def test_bootstrap_preserves_deterministic_structure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("FLEETGRAPH_RUNTIME_ENVIRONMENT", "development")
    monkeypatch.setenv("FLEETGRAPH_API_HOST", "127.0.0.1")
    monkeypatch.setenv("FLEETGRAPH_API_PORT", "8000")
    monkeypatch.setenv("FLEETGRAPH_DEBUG", "true")
    monkeypatch.setenv("FLEETGRAPH_LOG_LEVEL", "DEBUG")

    bootstrap = build_runtime_bootstrap_from_environment()

    assert set(bootstrap.__dataclass_fields__.keys()) == {"config", "logger"}
    assert set(bootstrap.config.__dataclass_fields__.keys()) == {
        "environment",
        "api_host",
        "api_port",
        "debug",
        "log_level",
    }


def test_bootstrap_does_not_mutate_environment(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv("FLEETGRAPH_RUNTIME_ENVIRONMENT", "staging")
    monkeypatch.setenv("FLEETGRAPH_API_HOST", "0.0.0.0")
    monkeypatch.setenv("FLEETGRAPH_API_PORT", "8000")
    monkeypatch.setenv("FLEETGRAPH_DEBUG", "false")
    monkeypatch.setenv("FLEETGRAPH_LOG_LEVEL", "INFO")

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

    build_runtime_bootstrap_from_env_file(env_file)

    assert dict(os.environ) == env_before


def test_bootstrap_reflects_canonical_config_values(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("FLEETGRAPH_RUNTIME_ENVIRONMENT", "production")
    monkeypatch.setenv("FLEETGRAPH_API_HOST", "10.20.30.40")
    monkeypatch.setenv("FLEETGRAPH_API_PORT", "8123")
    monkeypatch.setenv("FLEETGRAPH_DEBUG", "on")
    monkeypatch.setenv("FLEETGRAPH_LOG_LEVEL", "error")

    bootstrap = build_runtime_bootstrap_from_environment()

    assert bootstrap.config.environment == "production"
    assert bootstrap.config.api_host == "10.20.30.40"
    assert bootstrap.config.api_port == 8123
    assert bootstrap.config.debug is True
    assert bootstrap.config.log_level == "ERROR"
    assert bootstrap.logger.name == "fleetgraph.runtime.production"
    assert bootstrap.logger.level == logging.ERROR


def test_repeated_bootstrap_with_same_inputs_is_deterministic(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("FLEETGRAPH_RUNTIME_ENVIRONMENT", "staging")
    monkeypatch.setenv("FLEETGRAPH_API_HOST", "0.0.0.0")
    monkeypatch.setenv("FLEETGRAPH_API_PORT", "8000")
    monkeypatch.setenv("FLEETGRAPH_DEBUG", "false")
    monkeypatch.setenv("FLEETGRAPH_LOG_LEVEL", "INFO")

    first_bootstrap = build_runtime_bootstrap_from_environment()
    second_bootstrap = build_runtime_bootstrap_from_environment()

    assert first_bootstrap.config == second_bootstrap.config
    assert first_bootstrap.logger is second_bootstrap.logger
    assert first_bootstrap.logger.name == "fleetgraph.runtime.staging"
    assert second_bootstrap.logger.level == logging.INFO
