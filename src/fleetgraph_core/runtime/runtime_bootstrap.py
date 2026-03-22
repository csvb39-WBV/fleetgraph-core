from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Mapping

from fleetgraph_core.config.runtime_config import RuntimeConfig, build_runtime_config
from fleetgraph_core.infrastructure.observability.logging_config import (
    build_application_logger,
)


@dataclass(frozen=True)
class RuntimeBootstrap:
    config: RuntimeConfig
    logger: logging.Logger


def build_runtime_bootstrap(config_input: Mapping[str, Any]) -> RuntimeBootstrap:
    runtime_config = build_runtime_config(config_input)

    logger = build_application_logger(
        logger_name=f"fleetgraph.runtime.{runtime_config.environment}",
        environment=runtime_config.environment,
        log_level=runtime_config.log_level,
    )

    return RuntimeBootstrap(
        config=runtime_config,
        logger=logger,
    )
