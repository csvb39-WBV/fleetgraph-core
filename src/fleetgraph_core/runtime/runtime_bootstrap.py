from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

from fleetgraph_core.config.runtime_config import (
    RuntimeConfig,
    build_runtime_config,
    load_runtime_config,
    load_runtime_config_from_env_file,
)
from fleetgraph_core.infrastructure.observability.logging_config import (
    build_application_logger,
)


@dataclass(frozen=True)
class RuntimeBootstrap:
    config: RuntimeConfig
    logger: logging.Logger


_LOGGING_ENVIRONMENT_BY_RUNTIME_ENVIRONMENT = {
    "development": "dev",
    "staging": "test",
    "production": "prod",
}


def _build_runtime_bootstrap_from_config(runtime_config: RuntimeConfig) -> RuntimeBootstrap:
    logging_environment = _LOGGING_ENVIRONMENT_BY_RUNTIME_ENVIRONMENT.get(
        runtime_config.environment,
        runtime_config.environment,
    )

    logger = build_application_logger(
        logger_name=f"fleetgraph.runtime.{runtime_config.environment}",
        environment=logging_environment,
        log_level=runtime_config.log_level,
    )

    return RuntimeBootstrap(
        config=runtime_config,
        logger=logger,
    )


def build_runtime_bootstrap(config_input: Mapping[str, Any]) -> RuntimeBootstrap:
    runtime_config = build_runtime_config(config_input)
    return _build_runtime_bootstrap_from_config(runtime_config)


def build_runtime_bootstrap_from_environment() -> RuntimeBootstrap:
    runtime_config = load_runtime_config()
    return _build_runtime_bootstrap_from_config(runtime_config)


def build_runtime_bootstrap_from_env_file(path: str | Path) -> RuntimeBootstrap:
    runtime_config = load_runtime_config_from_env_file(path)
    return _build_runtime_bootstrap_from_config(runtime_config)
