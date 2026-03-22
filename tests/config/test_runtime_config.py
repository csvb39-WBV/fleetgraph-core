from __future__ import annotations

import sys
from pathlib import Path

import pytest


REPO_ROOT = Path(__file__).resolve().parents[2]
SRC_ROOT = REPO_ROOT / "src"

if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))


from fleetgraph_core.config.runtime_config import (
    DEFAULT_API_HOST,
    DEFAULT_API_PORT,
    DEFAULT_DEBUG,
    DEFAULT_ENVIRONMENT,
    DEFAULT_LOG_LEVEL,
    SUPPORTED_ENVIRONMENTS,
    SUPPORTED_LOG_LEVELS,
    ENV_API_HOST,
    ENV_API_PORT,
    ENV_DEBUG,
    ENV_LOG_LEVEL,
    ENV_RUNTIME_ENVIRONMENT,
    RuntimeConfig,
    load_runtime_config,
)


def test_supported_environments_are_locked() -> None:
    assert SUPPORTED_ENVIRONMENTS == ("development", "staging", "production")


def test_supported_log_levels_are_locked() -> None:
    assert SUPPORTED_LOG_LEVELS == ("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL")


def test_load_runtime_config_returns_immutable_runtime_config(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(ENV_RUNTIME_ENVIRONMENT, "staging")
    monkeypatch.setenv(ENV_API_HOST, "0.0.0.0")
    monkeypatch.setenv(ENV_API_PORT, "9000")
    monkeypatch.setenv(ENV_DEBUG, "true")
    monkeypatch.setenv(ENV_LOG_LEVEL, "debug")

    config = load_runtime_config()

    assert isinstance(config, RuntimeConfig)
    assert config == RuntimeConfig(
        environment="staging",
        api_host="0.0.0.0",
        api_port=9000,
        debug=True,
        log_level="DEBUG",
    )

    with pytest.raises(Exception):
        setattr(config, "environment", "prod")


def test_load_runtime_config_uses_safe_defaults(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv(ENV_RUNTIME_ENVIRONMENT, raising=False)
    monkeypatch.delenv(ENV_API_HOST, raising=False)
    monkeypatch.delenv(ENV_API_PORT, raising=False)
    monkeypatch.delenv(ENV_DEBUG, raising=False)
    monkeypatch.delenv(ENV_LOG_LEVEL, raising=False)

    config = load_runtime_config()

    assert config.environment == DEFAULT_ENVIRONMENT
    assert config.api_host == DEFAULT_API_HOST
    assert config.api_port == DEFAULT_API_PORT
    assert config.debug is DEFAULT_DEBUG
    assert config.log_level == DEFAULT_LOG_LEVEL


def test_load_runtime_config_accepts_valid_environment_overrides(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(ENV_RUNTIME_ENVIRONMENT, "production")
    monkeypatch.setenv(ENV_API_HOST, "10.1.2.3")
    monkeypatch.setenv(ENV_API_PORT, "8088")
    monkeypatch.setenv(ENV_DEBUG, "off")
    monkeypatch.setenv(ENV_LOG_LEVEL, "warning")

    config = load_runtime_config()

    assert config.environment == "production"
    assert config.api_host == "10.1.2.3"
    assert config.api_port == 8088
    assert config.debug is False
    assert config.log_level == "WARNING"


def test_load_runtime_config_rejects_invalid_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(ENV_RUNTIME_ENVIRONMENT, "qa")

    with pytest.raises(ValueError, match="Unsupported environment: qa"):
        load_runtime_config()


def test_load_runtime_config_parses_valid_port(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(ENV_API_PORT, "65535")

    config = load_runtime_config()

    assert config.api_port == 65535


@pytest.mark.parametrize("bad_port", ["0", "65536", "abc", "", "   ", "-1"])
def test_load_runtime_config_rejects_invalid_port(
    monkeypatch: pytest.MonkeyPatch,
    bad_port: str,
) -> None:
    monkeypatch.setenv(ENV_API_PORT, bad_port)

    with pytest.raises(ValueError, match="api_port must be an integer between 1 and 65535"):
        load_runtime_config()


@pytest.mark.parametrize("value", ["1", "true", "yes", "on", "TrUe", " YES "])
def test_load_runtime_config_parses_truthy_debug_values(
    monkeypatch: pytest.MonkeyPatch,
    value: str,
) -> None:
    monkeypatch.setenv(ENV_DEBUG, value)

    config = load_runtime_config()

    assert config.debug is True


@pytest.mark.parametrize("value", ["0", "false", "no", "off", "FaLsE", " OFF "])
def test_load_runtime_config_parses_falsey_debug_values(
    monkeypatch: pytest.MonkeyPatch,
    value: str,
) -> None:
    monkeypatch.setenv(ENV_DEBUG, value)

    config = load_runtime_config()

    assert config.debug is False


def test_load_runtime_config_rejects_invalid_debug_value(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(ENV_DEBUG, "maybe")

    with pytest.raises(ValueError, match="debug must be one of"):
        load_runtime_config()


@pytest.mark.parametrize("value", ["DEBUG", "info", " Warning ", "ERROR", "critical"])
def test_load_runtime_config_accepts_valid_log_levels(
    monkeypatch: pytest.MonkeyPatch,
    value: str,
) -> None:
    monkeypatch.setenv(ENV_LOG_LEVEL, value)

    config = load_runtime_config()

    assert config.log_level in SUPPORTED_LOG_LEVELS


def test_load_runtime_config_rejects_invalid_log_level(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(ENV_LOG_LEVEL, "TRACE")

    with pytest.raises(ValueError, match="Unsupported log_level: TRACE"):
        load_runtime_config()


def test_runtime_config_has_expected_stable_fields(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(ENV_RUNTIME_ENVIRONMENT, "development")
    monkeypatch.setenv(ENV_API_HOST, "127.0.0.1")
    monkeypatch.setenv(ENV_API_PORT, "8000")
    monkeypatch.setenv(ENV_DEBUG, "0")
    monkeypatch.setenv(ENV_LOG_LEVEL, "INFO")

    config = load_runtime_config()

    assert config.__dataclass_fields__.keys() == {
        "environment",
        "api_host",
        "api_port",
        "debug",
        "log_level",
    }


def test_load_runtime_config_isolated_environment_loading(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(ENV_RUNTIME_ENVIRONMENT, "staging")
    first_config = load_runtime_config()

    monkeypatch.setenv(ENV_RUNTIME_ENVIRONMENT, "production")
    second_config = load_runtime_config()

    assert first_config.environment == "staging"
    assert second_config.environment == "production"
