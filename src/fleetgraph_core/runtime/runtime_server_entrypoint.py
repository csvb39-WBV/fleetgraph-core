from __future__ import annotations

import logging

import uvicorn

from fleetgraph_core.runtime.runtime_bootstrap import (
    build_runtime_bootstrap_from_environment,
)
from fleetgraph_core.runtime.runtime_http_api import app


DEFAULT_HOST = "0.0.0.0"
DEFAULT_PORT = 8000
ENTRYPOINT_LOGGER_NAME = "fleetgraph.runtime.entrypoint"


def _build_entrypoint_logger() -> logging.Logger:
    try:
        return build_runtime_bootstrap_from_environment().logger
    except Exception:
        logging.basicConfig(level=logging.INFO)
        fallback_logger = logging.getLogger(ENTRYPOINT_LOGGER_NAME)
        fallback_logger.warning(
            "event=logging_init_fallback entrypoint=%s",
            "fleetgraph_core.runtime.runtime_server_entrypoint",
        )
        return fallback_logger


def launch_runtime_server(host: str = DEFAULT_HOST, port: int = DEFAULT_PORT) -> None:
    logger = _build_entrypoint_logger()
    logger.info(
        "event=runtime_server_starting host=%s port=%s entrypoint=%s",
        host,
        port,
        "fleetgraph_core.runtime.runtime_server_entrypoint",
    )
    try:
        uvicorn.run(app, host=host, port=port)
    except Exception:
        logger.exception(
            "event=runtime_server_startup_failed host=%s port=%s entrypoint=%s",
            host,
            port,
            "fleetgraph_core.runtime.runtime_server_entrypoint",
        )
        raise


def main() -> None:
    launch_runtime_server()


if __name__ == "__main__":
    main()
