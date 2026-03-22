from __future__ import annotations

import logging
import sys
from pathlib import Path

import pytest


REPO_ROOT = Path(__file__).resolve().parents[2]
SRC_ROOT = REPO_ROOT / "src"

if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))


from fleetgraph_core.infrastructure.observability.logging_config import (
    DEFAULT_DATE_FORMAT,
    DEFAULT_LOG_FORMAT,
    build_application_logger,
)


def test_build_application_logger_returns_logger_with_expected_name() -> None:
    logger = build_application_logger(
        logger_name="fleetgraph.test.logger",
        environment="dev",
        log_level="info",
    )

    assert isinstance(logger, logging.Logger)
    assert logger.name == "fleetgraph.test.logger"


def test_build_application_logger_sets_expected_level_and_propagation() -> None:
    logger = build_application_logger(
        logger_name="fleetgraph.level.logger",
        environment="prod",
        log_level="warning",
    )

    assert logger.level == logging.WARNING
    assert logger.propagate is False


def test_build_application_logger_attaches_single_stream_handler() -> None:
    logger_name = "fleetgraph.single.handler.logger"
    logger = build_application_logger(
        logger_name=logger_name,
        environment="test",
        log_level="debug",
    )

    stream_handlers = [
        handler
        for handler in logger.handlers
        if isinstance(handler, logging.StreamHandler)
    ]

    assert len(stream_handlers) == 1


def test_build_application_logger_does_not_duplicate_handlers_on_repeated_calls() -> None:
    logger_name = "fleetgraph.no.duplicate.logger"

    first_logger = build_application_logger(
        logger_name=logger_name,
        environment="dev",
        log_level="info",
    )
    second_logger = build_application_logger(
        logger_name=logger_name,
        environment="dev",
        log_level="debug",
    )

    first_stream_handlers = [
        handler
        for handler in first_logger.handlers
        if isinstance(handler, logging.StreamHandler)
    ]
    second_stream_handlers = [
        handler
        for handler in second_logger.handlers
        if isinstance(handler, logging.StreamHandler)
    ]

    assert first_logger is second_logger
    assert len(first_stream_handlers) == 1
    assert len(second_stream_handlers) == 1
    assert second_logger.level == logging.DEBUG


def test_build_application_logger_applies_expected_formatter_values() -> None:
    logger = build_application_logger(
        logger_name="fleetgraph.formatter.logger",
        environment="test",
        log_level="error",
    )

    stream_handler = next(
        handler
        for handler in logger.handlers
        if isinstance(handler, logging.StreamHandler)
    )
    formatter = stream_handler.formatter

    assert formatter is not None
    assert formatter._fmt == DEFAULT_LOG_FORMAT
    assert formatter.datefmt == DEFAULT_DATE_FORMAT


def test_build_application_logger_rejects_blank_logger_name() -> None:
    with pytest.raises(ValueError, match="logger_name must be a non-empty string"):
        build_application_logger(
            logger_name="   ",
            environment="dev",
            log_level="info",
        )


def test_build_application_logger_rejects_invalid_environment() -> None:
    with pytest.raises(ValueError, match="Unsupported environment: stage"):
        build_application_logger(
            logger_name="fleetgraph.invalid.environment",
            environment="stage",
            log_level="info",
        )


def test_build_application_logger_rejects_blank_log_level() -> None:
    with pytest.raises(ValueError, match="log_level must be a non-empty string"):
        build_application_logger(
            logger_name="fleetgraph.invalid.loglevel",
            environment="dev",
            log_level="   ",
        )


def test_build_application_logger_is_deterministic() -> None:
    logger_name = "fleetgraph.deterministic.logger"

    first_logger = build_application_logger(
        logger_name=logger_name,
        environment=" PROD ",
        log_level=" warning ",
    )
    second_logger = build_application_logger(
        logger_name=logger_name,
        environment="prod",
        log_level="WARNING",
    )

    assert first_logger is second_logger
    assert second_logger.level == logging.WARNING
