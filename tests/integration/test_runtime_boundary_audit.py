from __future__ import annotations

import os
import sys
from copy import deepcopy
from pathlib import Path

import pytest


REPO_ROOT = Path(__file__).resolve().parents[2]
SRC_ROOT = REPO_ROOT / "src"

if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))


from fleetgraph_core.config.runtime_config import (
    load_runtime_config,
    load_runtime_config_from_env_file,
)
from fleetgraph_core.runtime.runtime_bootstrap import (
    build_runtime_bootstrap_from_env_file,
    build_runtime_bootstrap_from_environment,
    build_runtime_bootstrap_summary,
)
from fleetgraph_core.runtime.runtime_external_api import build_runtime_external_api_response
from fleetgraph_core.runtime.runtime_health_api import build_runtime_health_response


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
    monkeypatch.setenv("FLEETGRAPH_RUNTIME_ENVIRONMENT", "staging")
    monkeypatch.setenv("FLEETGRAPH_API_HOST", "0.0.0.0")
    monkeypatch.setenv("FLEETGRAPH_API_PORT", "8100")
    monkeypatch.setenv("FLEETGRAPH_DEBUG", "false")
    monkeypatch.setenv("FLEETGRAPH_LOG_LEVEL", "INFO")


def _write_matching_env_file(tmp_path: Path) -> Path:
    env_file = tmp_path / "runtime.env"
    env_file.write_text(
        "\n".join(
            [
                "FLEETGRAPH_RUNTIME_ENVIRONMENT=staging",
                "FLEETGRAPH_API_HOST=0.0.0.0",
                "FLEETGRAPH_API_PORT=8100",
                "FLEETGRAPH_DEBUG=false",
                "FLEETGRAPH_LOG_LEVEL=INFO",
            ]
        ),
        encoding="utf-8",
    )
    return env_file


def _build_chain_from_environment() -> tuple[dict, dict, dict]:
    bootstrap = build_runtime_bootstrap_from_environment()
    summary = build_runtime_bootstrap_summary(bootstrap)
    external = build_runtime_external_api_response(bootstrap)
    health = build_runtime_health_response(bootstrap)
    return summary, external, health


def _build_chain_from_env_file(path: Path) -> tuple[dict, dict, dict]:
    bootstrap = build_runtime_bootstrap_from_env_file(path)
    summary = build_runtime_bootstrap_summary(bootstrap)
    external = build_runtime_external_api_response(bootstrap)
    health = build_runtime_health_response(bootstrap)
    return summary, external, health


def test_environment_and_env_file_boundary_alignment(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    _set_runtime_env(monkeypatch)
    env_file = _write_matching_env_file(tmp_path)

    config_from_environment = load_runtime_config()
    config_from_env_file = load_runtime_config_from_env_file(env_file)

    assert config_from_environment == config_from_env_file

    env_summary, env_external, env_health = _build_chain_from_environment()
    file_summary, file_external, file_health = _build_chain_from_env_file(env_file)

    assert env_summary == file_summary
    assert env_external == file_external
    assert env_health == file_health


def test_exact_field_sets_and_no_extra_keys(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    _set_runtime_env(monkeypatch)
    env_file = _write_matching_env_file(tmp_path)

    summary, external, health = _build_chain_from_env_file(env_file)

    assert set(summary.keys()) == SUMMARY_KEYS
    assert set(external.keys()) == EXTERNAL_KEYS
    assert set(external["runtime"].keys()) == SUMMARY_KEYS
    assert set(health.keys()) == HEALTH_KEYS
    assert set(health["checks"].keys()) == HEALTH_CHECK_KEYS
    assert set(health["runtime"].keys()) == SUMMARY_KEYS


def test_full_chain_alignment_and_passthrough_integrity(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    _set_runtime_env(monkeypatch)
    env_file = _write_matching_env_file(tmp_path)

    summary, external, health = _build_chain_from_env_file(env_file)

    assert external["response_type"] == "runtime_external_api_response"
    assert external["response_schema_version"] == "1.0"
    assert health["response_type"] == "runtime_health_response"
    assert health["response_schema_version"] == "1.0"

    assert external["runtime"] == summary
    assert health["runtime"] == external["runtime"]
    assert health["checks"] == {"config_valid": True, "logger_ready": True}
    assert health["status"] == "healthy"


def test_repeated_chain_execution_is_identical(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    _set_runtime_env(monkeypatch)
    env_file = _write_matching_env_file(tmp_path)

    first = _build_chain_from_env_file(env_file)
    first_snapshot = deepcopy(first)
    second = _build_chain_from_env_file(env_file)

    assert first == second
    assert first == first_snapshot


def test_environment_and_bootstrap_not_mutated(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    _set_runtime_env(monkeypatch)
    env_file = _write_matching_env_file(tmp_path)

    env_before = dict(os.environ)
    bootstrap = build_runtime_bootstrap_from_environment()
    bootstrap_config_before = bootstrap.config
    bootstrap_logger_name_before = bootstrap.logger.name
    bootstrap_logger_level_before = bootstrap.logger.level

    _ = load_runtime_config_from_env_file(env_file)
    _ = build_runtime_bootstrap_summary(bootstrap)
    _ = build_runtime_external_api_response(bootstrap)
    _ = build_runtime_health_response(bootstrap)

    assert dict(os.environ) == env_before
    assert bootstrap.config == bootstrap_config_before
    assert bootstrap.logger.name == bootstrap_logger_name_before
    assert bootstrap.logger.level == bootstrap_logger_level_before
