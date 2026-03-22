from __future__ import annotations

import logging
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
)


def test_build_runtime_bootstrap_returns_expected_object() -> None:
    bootstrap = build_runtime_bootstrap(
        {
            "environment": "dev",
            "aws_region": "us-east-1",
            "storage_bucket": "fleetgraph-dev-bucket",
            "signal_topic": "fleetgraph-dev-topic",
            "log_level": "debug",
        }
    )

    assert isinstance(bootstrap, RuntimeBootstrap)
    assert bootstrap.config.environment == "dev"
    assert bootstrap.config.aws_region == "us-east-1"
    assert bootstrap.config.storage_bucket == "fleetgraph-dev-bucket"
    assert bootstrap.config.signal_topic == "fleetgraph-dev-topic"
    assert bootstrap.config.log_level == "DEBUG"
    assert isinstance(bootstrap.logger, logging.Logger)
    assert bootstrap.logger.name == "fleetgraph.runtime.dev"
    assert bootstrap.logger.level == logging.DEBUG


def test_build_runtime_bootstrap_uses_default_log_level_when_omitted() -> None:
    bootstrap = build_runtime_bootstrap(
        {
            "environment": "test",
            "aws_region": "us-west-2",
            "storage_bucket": "fleetgraph-test-bucket",
            "signal_topic": "fleetgraph-test-topic",
        }
    )

    assert bootstrap.config.log_level == "INFO"
    assert bootstrap.logger.name == "fleetgraph.runtime.test"
    assert bootstrap.logger.level == logging.INFO


def test_build_runtime_bootstrap_reuses_same_logger_for_same_environment() -> None:
    first_bootstrap = build_runtime_bootstrap(
        {
            "environment": "prod",
            "aws_region": "us-east-2",
            "storage_bucket": "fleetgraph-prod-bucket",
            "signal_topic": "fleetgraph-prod-topic",
            "log_level": "warning",
        }
    )
    second_bootstrap = build_runtime_bootstrap(
        {
            "environment": "prod",
            "aws_region": "us-east-2",
            "storage_bucket": "fleetgraph-prod-bucket",
            "signal_topic": "fleetgraph-prod-topic",
            "log_level": "warning",
        }
    )

    assert first_bootstrap.logger is second_bootstrap.logger
    assert first_bootstrap.logger.name == "fleetgraph.runtime.prod"
    assert second_bootstrap.logger.level == logging.WARNING


def test_build_runtime_bootstrap_rejects_non_mapping_input() -> None:
    with pytest.raises(ValueError, match="config_input must be a mapping"):
        build_runtime_bootstrap(["not", "a", "mapping"])  # type: ignore[arg-type]


def test_build_runtime_bootstrap_preserves_required_key_validation() -> None:
    with pytest.raises(ValueError, match="Missing required config key: aws_region"):
        build_runtime_bootstrap(
            {
                "environment": "dev",
                "storage_bucket": "fleetgraph-dev-bucket",
                "signal_topic": "fleetgraph-dev-topic",
            }
        )


def test_build_runtime_bootstrap_is_deterministic() -> None:
    config_input = {
        "environment": " dev ",
        "aws_region": "us-east-1",
        "storage_bucket": "fleetgraph-bucket",
        "signal_topic": "fleetgraph-topic",
        "log_level": " info ",
    }

    first_bootstrap = build_runtime_bootstrap(config_input)
    second_bootstrap = build_runtime_bootstrap(config_input)

    assert first_bootstrap.config == second_bootstrap.config
    assert first_bootstrap.logger is second_bootstrap.logger
    assert first_bootstrap.logger.name == "fleetgraph.runtime.dev"
    assert second_bootstrap.logger.level == logging.INFO
