import pytest
from fastapi import FastAPI
from unittest.mock import patch, call

from src.fleetgraph_core.api.relationship_signal_service_runner import (
    get_host,
    get_port,
    get_reload,
    create_service_app,
    run_relationship_signal_service,
    app,
)


# --- get_host ---

def test_get_host_default(monkeypatch) -> None:
    monkeypatch.delenv("FLEETGRAPH_API_HOST", raising=False)
    assert get_host() == "127.0.0.1"


def test_get_host_env_override(monkeypatch) -> None:
    monkeypatch.setenv("FLEETGRAPH_API_HOST", "0.0.0.0")
    assert get_host() == "0.0.0.0"


# --- get_port ---

def test_get_port_default(monkeypatch) -> None:
    monkeypatch.delenv("FLEETGRAPH_API_PORT", raising=False)
    assert get_port() == 8000


def test_get_port_env_override(monkeypatch) -> None:
    monkeypatch.setenv("FLEETGRAPH_API_PORT", "9000")
    assert get_port() == 9000


def test_get_port_invalid_string(monkeypatch) -> None:
    monkeypatch.setenv("FLEETGRAPH_API_PORT", "abc")
    with pytest.raises(ValueError, match="FLEETGRAPH_API_PORT must be a positive integer"):
        get_port()


def test_get_port_zero_rejected(monkeypatch) -> None:
    monkeypatch.setenv("FLEETGRAPH_API_PORT", "0")
    with pytest.raises(ValueError, match="FLEETGRAPH_API_PORT must be a positive integer"):
        get_port()


def test_get_port_negative_rejected(monkeypatch) -> None:
    monkeypatch.setenv("FLEETGRAPH_API_PORT", "-1")
    with pytest.raises(ValueError, match="FLEETGRAPH_API_PORT must be a positive integer"):
        get_port()


# --- get_reload ---

def test_get_reload_default(monkeypatch) -> None:
    monkeypatch.delenv("FLEETGRAPH_API_RELOAD", raising=False)
    assert get_reload() is False


def test_get_reload_true_override(monkeypatch) -> None:
    monkeypatch.setenv("FLEETGRAPH_API_RELOAD", "true")
    assert get_reload() is True


def test_get_reload_true_uppercase(monkeypatch) -> None:
    monkeypatch.setenv("FLEETGRAPH_API_RELOAD", "TRUE")
    assert get_reload() is True


def test_get_reload_other_values_false(monkeypatch) -> None:
    monkeypatch.setenv("FLEETGRAPH_API_RELOAD", "yes")
    assert get_reload() is False


# --- create_service_app ---

def test_create_service_app_returns_fastapi() -> None:
    result = create_service_app()
    assert isinstance(result, FastAPI)


# --- module-level app ---

def test_module_level_app_exposed() -> None:
    assert isinstance(app, FastAPI)


# --- run_relationship_signal_service ---

def test_run_service_calls_uvicorn(monkeypatch) -> None:
    monkeypatch.delenv("FLEETGRAPH_API_HOST", raising=False)
    monkeypatch.delenv("FLEETGRAPH_API_PORT", raising=False)
    monkeypatch.delenv("FLEETGRAPH_API_RELOAD", raising=False)

    with patch("src.fleetgraph_core.api.relationship_signal_service_runner.uvicorn.run") as mock_run:
        run_relationship_signal_service()
        assert mock_run.call_count == 1
        _, kwargs = mock_run.call_args
        assert kwargs["host"] == "127.0.0.1"
        assert kwargs["port"] == 8000
        assert kwargs["reload"] is False


def test_run_service_no_extra_kwargs(monkeypatch) -> None:
    monkeypatch.delenv("FLEETGRAPH_API_HOST", raising=False)
    monkeypatch.delenv("FLEETGRAPH_API_PORT", raising=False)
    monkeypatch.delenv("FLEETGRAPH_API_RELOAD", raising=False)

    with patch("src.fleetgraph_core.api.relationship_signal_service_runner.uvicorn.run") as mock_run:
        run_relationship_signal_service()
        _, kwargs = mock_run.call_args
        assert set(kwargs.keys()) == {"host", "port", "reload"}
