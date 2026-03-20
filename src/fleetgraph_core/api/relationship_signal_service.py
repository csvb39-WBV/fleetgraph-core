import os
from typing import Any, Dict, List

from fastapi import FastAPI, HTTPException

from src.fleetgraph_core.api.relationship_signal_api_reader import (
    get_relationship_signal_output_summary as reader_get_relationship_signal_output_summary,
    get_relationship_signal_records as reader_get_relationship_signal_records,
    load_relationship_signal_output as reader_load_relationship_signal_output,
)


OUTPUT_PATH_ENV_VAR = "FLEETGRAPH_RELATIONSHIP_SIGNAL_OUTPUT_PATH"
DEFAULT_OUTPUT_PATH = "relationship_signals_output.json"
OUTPUT_UNAVAILABLE_DETAIL = "relationship signal output unavailable"


def get_output_path() -> str:
    return os.getenv(OUTPUT_PATH_ENV_VAR, DEFAULT_OUTPUT_PATH)


def read_relationship_signal_output() -> Dict[str, Any]:
    return reader_load_relationship_signal_output(get_output_path())


def read_relationship_signal_records() -> List[Dict[str, Any]]:
    payload = read_relationship_signal_output()
    return reader_get_relationship_signal_records(payload)


def read_relationship_signal_summary() -> Dict[str, Any]:
    payload = read_relationship_signal_output()
    return reader_get_relationship_signal_output_summary(payload)


def create_app() -> FastAPI:
    app = FastAPI(docs_url=None, redoc_url=None, openapi_url=None)

    def _load_or_raise(loader):
        try:
            return loader()
        except (OSError, ValueError) as exc:
            raise HTTPException(status_code=500, detail=OUTPUT_UNAVAILABLE_DETAIL) from exc

    @app.get("/health")
    def health() -> Dict[str, str]:
        return {"status": "ok"}

    @app.get("/relationship-signals/output")
    def relationship_signals_output() -> Dict[str, Any]:
        return _load_or_raise(read_relationship_signal_output)

    @app.get("/relationship-signals/records")
    def relationship_signals_records() -> List[Dict[str, Any]]:
        return _load_or_raise(read_relationship_signal_records)

    @app.get("/relationship-signals/summary")
    def relationship_signals_summary() -> Dict[str, Any]:
        return _load_or_raise(read_relationship_signal_summary)

    return app