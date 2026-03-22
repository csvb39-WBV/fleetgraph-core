from __future__ import annotations

import os
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
    load_runtime_config_from_env_file,
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


def test_environment_example_files_exist() -> None:
    assert (REPO_ROOT / ".env.development.example").exists()
    assert (REPO_ROOT / ".env.staging.example").exists()
    assert (REPO_ROOT / ".env.production.example").exists()


def test_load_runtime_config_from_development_example_file() -> None:
    config = load_runtime_config_from_env_file(REPO_ROOT / ".env.development.example")

    assert config == RuntimeConfig(
        environment="development",
        api_host="127.0.0.1",
        api_port=8000,
        debug=True,
        log_level="DEBUG",
    )


def test_load_runtime_config_from_staging_example_file() -> None:
    config = load_runtime_config_from_env_file(REPO_ROOT / ".env.staging.example")

    assert config == RuntimeConfig(
        environment="staging",
        api_host="0.0.0.0",
        api_port=8000,
        debug=False,
        log_level="INFO",
    )


def test_load_runtime_config_from_production_example_file() -> None:
    config = load_runtime_config_from_env_file(REPO_ROOT / ".env.production.example")

    assert config == RuntimeConfig(
        environment="production",
        api_host="0.0.0.0",
        api_port=8000,
        debug=False,
        log_level="INFO",
    )


def test_load_runtime_config_from_env_file_returns_runtime_config(tmp_path: Path) -> None:
    env_file = tmp_path / "runtime.env"
    env_file.write_text(
        "\n".join(
            [
                "FLEETGRAPH_RUNTIME_ENVIRONMENT=staging",
                "FLEETGRAPH_API_HOST=10.0.0.8",
                "FLEETGRAPH_API_PORT=8010",
                "FLEETGRAPH_DEBUG=false",
                "FLEETGRAPH_LOG_LEVEL=WARNING",
            ]
        ),
        encoding="utf-8",
    )

    config = load_runtime_config_from_env_file(env_file)

    assert isinstance(config, RuntimeConfig)
    assert config.environment == "staging"
    assert config.api_host == "10.0.0.8"
    assert config.api_port == 8010
    assert config.debug is False
    assert config.log_level == "WARNING"


def test_load_runtime_config_from_env_file_ignores_blank_and_comment_lines(tmp_path: Path) -> None:
    env_file = tmp_path / "runtime.env"
    env_file.write_text(
        "\n".join(
            [
                "",
                "# full line comment",
                "FLEETGRAPH_RUNTIME_ENVIRONMENT=development",
                "",
                "   # indented comment",
                "FLEETGRAPH_API_HOST=127.0.0.1",
                "FLEETGRAPH_API_PORT=8001",
                "FLEETGRAPH_DEBUG=on",
                "FLEETGRAPH_LOG_LEVEL=debug",
                "",
            ]
        ),
        encoding="utf-8",
    )

    config = load_runtime_config_from_env_file(env_file)

    assert config.environment == "development"
    assert config.api_port == 8001
    assert config.debug is True
    assert config.log_level == "DEBUG"


def test_load_runtime_config_from_env_file_trims_key_and_value_whitespace(tmp_path: Path) -> None:
    env_file = tmp_path / "runtime.env"
    env_file.write_text(
        "\n".join(
            [
                "  FLEETGRAPH_RUNTIME_ENVIRONMENT  =  production  ",
                " FLEETGRAPH_API_HOST = 0.0.0.0 ",
                " FLEETGRAPH_API_PORT = 8002 ",
                " FLEETGRAPH_DEBUG = no ",
                " FLEETGRAPH_LOG_LEVEL = info ",
            ]
        ),
        encoding="utf-8",
    )

    config = load_runtime_config_from_env_file(env_file)

    assert config.environment == "production"
    assert config.api_host == "0.0.0.0"
    assert config.api_port == 8002
    assert config.debug is False
    assert config.log_level == "INFO"


def test_load_runtime_config_from_env_file_rejects_malformed_line_without_equals(
    tmp_path: Path,
) -> None:
    env_file = tmp_path / "runtime.env"
    env_file.write_text(
        "\n".join(
            [
                "FLEETGRAPH_RUNTIME_ENVIRONMENT=development",
                "MALFORMED_LINE",
            ]
        ),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="Malformed env file line 2"):
        load_runtime_config_from_env_file(env_file)


def test_load_runtime_config_from_env_file_rejects_duplicate_keys(tmp_path: Path) -> None:
    env_file = tmp_path / "runtime.env"
    env_file.write_text(
        "\n".join(
            [
                "FLEETGRAPH_RUNTIME_ENVIRONMENT=development",
                "FLEETGRAPH_RUNTIME_ENVIRONMENT=staging",
            ]
        ),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="Duplicate env file key: FLEETGRAPH_RUNTIME_ENVIRONMENT"):
        load_runtime_config_from_env_file(env_file)


def test_load_runtime_config_from_env_file_rejects_invalid_environment(tmp_path: Path) -> None:
    env_file = tmp_path / "runtime.env"
    env_file.write_text("FLEETGRAPH_RUNTIME_ENVIRONMENT=qa", encoding="utf-8")

    with pytest.raises(ValueError, match="Unsupported environment: qa"):
        load_runtime_config_from_env_file(env_file)


def test_load_runtime_config_from_env_file_rejects_invalid_port(tmp_path: Path) -> None:
    env_file = tmp_path / "runtime.env"
    env_file.write_text("FLEETGRAPH_API_PORT=90000", encoding="utf-8")

    with pytest.raises(ValueError, match="api_port must be an integer between 1 and 65535"):
        load_runtime_config_from_env_file(env_file)


def test_load_runtime_config_from_env_file_rejects_invalid_debug(tmp_path: Path) -> None:
    env_file = tmp_path / "runtime.env"
    env_file.write_text("FLEETGRAPH_DEBUG=maybe", encoding="utf-8")

    with pytest.raises(ValueError, match="debug must be one of"):
        load_runtime_config_from_env_file(env_file)


def test_load_runtime_config_from_env_file_rejects_invalid_log_level(tmp_path: Path) -> None:
    env_file = tmp_path / "runtime.env"
    env_file.write_text("FLEETGRAPH_LOG_LEVEL=TRACE", encoding="utf-8")

    with pytest.raises(ValueError, match="Unsupported log_level: TRACE"):
        load_runtime_config_from_env_file(env_file)


def test_load_runtime_config_from_env_file_missing_path_raises_file_error(tmp_path: Path) -> None:
    missing_file = tmp_path / "missing.env"

    with pytest.raises(FileNotFoundError):
        load_runtime_config_from_env_file(missing_file)


def test_load_runtime_config_from_env_file_does_not_mutate_environment(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv(ENV_RUNTIME_ENVIRONMENT, "staging")
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

    load_runtime_config_from_env_file(env_file)

    assert dict(os.environ) == env_before


def test_direct_load_runtime_config_behavior_remains_intact(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(ENV_RUNTIME_ENVIRONMENT, "development")
    monkeypatch.setenv(ENV_API_HOST, "127.0.0.1")
    monkeypatch.setenv(ENV_API_PORT, "8000")
    monkeypatch.setenv(ENV_DEBUG, "true")
    monkeypatch.setenv(ENV_LOG_LEVEL, "DEBUG")

    config = load_runtime_config()

    assert config == RuntimeConfig(
        environment="development",
        api_host="127.0.0.1",
        api_port=8000,
        debug=True,
        log_level="DEBUG",
    )
