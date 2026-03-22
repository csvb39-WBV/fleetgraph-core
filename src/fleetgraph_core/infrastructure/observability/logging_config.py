from __future__ import annotations

import logging


SUPPORTED_ENVIRONMENTS = ("dev", "test", "prod")
DEFAULT_LOG_FORMAT = "%(asctime)s | %(name)s | %(levelname)s | %(message)s"
DEFAULT_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"


def _validate_logger_name(logger_name: str) -> str:
    if not isinstance(logger_name, str):
        raise ValueError("logger_name must be a string")

    normalized_name = logger_name.strip()
    if not normalized_name:
        raise ValueError("logger_name must be a non-empty string")

    return normalized_name


def _normalize_environment(environment: str) -> str:
    if not isinstance(environment, str):
        raise ValueError("environment must be a string")

    normalized_environment = environment.strip().lower()
    if not normalized_environment:
        raise ValueError("environment must be a non-empty string")

    if normalized_environment not in SUPPORTED_ENVIRONMENTS:
        raise ValueError(
            "Unsupported environment: "
            f"{environment}. Supported environments: "
            + ", ".join(SUPPORTED_ENVIRONMENTS)
        )

    return normalized_environment


def _normalize_log_level(log_level: str) -> str:
    if not isinstance(log_level, str):
        raise ValueError("log_level must be a string")

    normalized_log_level = log_level.strip().upper()
    if not normalized_log_level:
        raise ValueError("log_level must be a non-empty string")

    return normalized_log_level


def build_application_logger(
    logger_name: str,
    environment: str,
    log_level: str,
) -> logging.Logger:
    normalized_logger_name = _validate_logger_name(logger_name)
    _normalize_environment(environment)
    normalized_log_level = _normalize_log_level(log_level)

    logger = logging.getLogger(normalized_logger_name)
    logger.setLevel(normalized_log_level)
    logger.propagate = False

    existing_stream_handlers = [
        handler
        for handler in logger.handlers
        if isinstance(handler, logging.StreamHandler)
    ]

    if not existing_stream_handlers:
        stream_handler = logging.StreamHandler()
        stream_handler.setLevel(normalized_log_level)
        stream_handler.setFormatter(
            logging.Formatter(
                fmt=DEFAULT_LOG_FORMAT,
                datefmt=DEFAULT_DATE_FORMAT,
            )
        )
        logger.addHandler(stream_handler)
    else:
        for handler in existing_stream_handlers:
            handler.setLevel(normalized_log_level)
            handler.setFormatter(
                logging.Formatter(
                    fmt=DEFAULT_LOG_FORMAT,
                    datefmt=DEFAULT_DATE_FORMAT,
                )
            )

    return logger
