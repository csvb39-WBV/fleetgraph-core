import os

import uvicorn
from fastapi import FastAPI

from src.fleetgraph_core.api.relationship_signal_service import create_app


HOST_ENV_VAR = "FLEETGRAPH_API_HOST"
PORT_ENV_VAR = "FLEETGRAPH_API_PORT"
RELOAD_ENV_VAR = "FLEETGRAPH_API_RELOAD"

DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = "8000"
DEFAULT_RELOAD = "false"


def get_host() -> str:
    return os.getenv(HOST_ENV_VAR, DEFAULT_HOST)


def get_port() -> int:
    raw = os.getenv(PORT_ENV_VAR, DEFAULT_PORT)
    try:
        port = int(raw)
    except ValueError:
        raise ValueError("FLEETGRAPH_API_PORT must be a positive integer")
    if port <= 0:
        raise ValueError("FLEETGRAPH_API_PORT must be a positive integer")
    return port


def get_reload() -> bool:
    raw = os.getenv(RELOAD_ENV_VAR, DEFAULT_RELOAD)
    return raw.strip().lower() == "true"


def create_service_app() -> FastAPI:
    return create_app()


def run_relationship_signal_service() -> None:
    uvicorn.run(create_service_app(), host=get_host(), port=get_port(), reload=get_reload())


app = create_service_app()
