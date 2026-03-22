from __future__ import annotations

import sys
from pathlib import Path

import pytest


REPO_ROOT = Path(__file__).resolve().parents[2]
SRC_ROOT = REPO_ROOT / "src"

if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))


from fleetgraph_core.config.runtime_config import (
    DEFAULT_LOG_LEVEL,
    SUPPORTED_ENVIRONMENTS,
    RuntimeConfig,
    build_runtime_config,
)


def test_supported_environments_are_locked() -> None:
    assert SUPPORTED_ENVIRONMENTS == ("dev", "test", "prod")


def test_build_runtime_config_returns_immutable_runtime_config() -> None:
    config = build_runtime_config(
        {
            "environment": "dev",
            "aws_region": "us-east-1",
            "storage_bucket": "fleetgraph-dev-bucket",
            "signal_topic": "fleetgraph-dev-topic",
            "log_level": "debug",
        }
    )

    assert isinstance(config, RuntimeConfig)
    assert config == RuntimeConfig(
        environment="dev",
        aws_region="us-east-1",
        storage_bucket="fleetgraph-dev-bucket",
        signal_topic="fleetgraph-dev-topic",
        log_level="DEBUG",
    )

    with pytest.raises(Exception):
        setattr(config, "environment", "prod")


def test_build_runtime_config_uses_default_log_level() -> None:
    config = build_runtime_config(
        {
            "environment": "test",
            "aws_region": "us-west-2",
            "storage_bucket": "fleetgraph-test-bucket",
            "signal_topic": "fleetgraph-test-topic",
        }
    )

    assert config.log_level == DEFAULT_LOG_LEVEL


def test_build_runtime_config_normalizes_environment_and_log_level() -> None:
    config = build_runtime_config(
        {
            "environment": " PROD ",
            "aws_region": "us-east-2",
            "storage_bucket": "fleetgraph-prod-bucket",
            "signal_topic": "fleetgraph-prod-topic",
            "log_level": " warning ",
        }
    )

    assert config.environment == "prod"
    assert config.log_level == "WARNING"


def test_build_runtime_config_rejects_non_mapping_input() -> None:
    with pytest.raises(ValueError, match="config_input must be a mapping"):
        build_runtime_config(["not", "a", "mapping"])  # type: ignore[arg-type]


@pytest.mark.parametrize(
    "missing_key",
    ["environment", "aws_region", "storage_bucket", "signal_topic"],
)
def test_build_runtime_config_rejects_missing_required_keys(missing_key: str) -> None:
    config_input = {
        "environment": "dev",
        "aws_region": "us-east-1",
        "storage_bucket": "fleetgraph-dev-bucket",
        "signal_topic": "fleetgraph-dev-topic",
    }
    del config_input[missing_key]

    with pytest.raises(ValueError, match=f"Missing required config key: {missing_key}"):
        build_runtime_config(config_input)


@pytest.mark.parametrize(
    "blank_key",
    ["environment", "aws_region", "storage_bucket", "signal_topic"],
)
def test_build_runtime_config_rejects_blank_required_values(blank_key: str) -> None:
    config_input = {
        "environment": "dev",
        "aws_region": "us-east-1",
        "storage_bucket": "fleetgraph-dev-bucket",
        "signal_topic": "fleetgraph-dev-topic",
    }
    config_input[blank_key] = "   "

    with pytest.raises(
        ValueError,
        match=f"Config key must be a non-empty string: {blank_key}",
    ):
        build_runtime_config(config_input)


def test_build_runtime_config_rejects_invalid_environment() -> None:
    with pytest.raises(ValueError, match="Unsupported environment: stage"):
        build_runtime_config(
            {
                "environment": "stage",
                "aws_region": "us-east-1",
                "storage_bucket": "fleetgraph-stage-bucket",
                "signal_topic": "fleetgraph-stage-topic",
            }
        )


def test_build_runtime_config_rejects_non_string_log_level() -> None:
    with pytest.raises(ValueError, match="Config key must be a string: log_level"):
        build_runtime_config(
            {
                "environment": "dev",
                "aws_region": "us-east-1",
                "storage_bucket": "fleetgraph-dev-bucket",
                "signal_topic": "fleetgraph-dev-topic",
                "log_level": 10,
            }
        )


def test_build_runtime_config_is_deterministic() -> None:
    config_input = {
        "environment": "test",
        "aws_region": "us-west-1",
        "storage_bucket": "fleetgraph-bucket",
        "signal_topic": "fleetgraph-topic",
        "log_level": "info",
    }

    first_result = build_runtime_config(config_input)
    second_result = build_runtime_config(config_input)

    assert first_result == second_result
