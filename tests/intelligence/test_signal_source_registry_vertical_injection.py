"""Tests for vertical-aware signal source registry injection layer."""

from __future__ import annotations

import pathlib
import sys

import pytest


PROJECT_ROOT = pathlib.Path(__file__).resolve().parents[2]
SRC_ROOT = PROJECT_ROOT / "src"

if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))


from fleetgraph_core.intelligence import signal_source_registry
from fleetgraph_core.intelligence.fleet_signal_source_registry import (
    get_fleet_signal_source,
    get_fleet_signal_source_names,
    get_fleet_signal_source_registry,
)
from fleetgraph_core.intelligence.construction_signal_source_registry import (
    get_construction_signal_source,
    get_construction_signal_source_names,
    get_construction_signal_source_registry,
)


def test_get_signal_source_registry_none_defaults_to_fleet() -> None:
    assert signal_source_registry.get_signal_source_registry(None) == get_fleet_signal_source_registry()


def test_get_signal_source_registry_empty_config_defaults_to_fleet() -> None:
    assert signal_source_registry.get_signal_source_registry({}) == get_fleet_signal_source_registry()


def test_get_signal_source_names_none_defaults_to_fleet() -> None:
    assert signal_source_registry.get_signal_source_names(None) == get_fleet_signal_source_names()


def test_get_signal_source_none_defaults_to_fleet_lookup() -> None:
    assert signal_source_registry.get_signal_source("permit", None) == get_fleet_signal_source("permit")


def test_has_signal_source_none_defaults_to_fleet_lookup() -> None:
    assert signal_source_registry.has_signal_source("permit", None) is True


def test_get_signal_source_registry_construction_vertical() -> None:
    runtime_config = {"vertical": "construction_audit_litigation"}
    assert (
        signal_source_registry.get_signal_source_registry(runtime_config)
        == get_construction_signal_source_registry()
    )


def test_get_signal_source_names_construction_vertical() -> None:
    runtime_config = {"vertical": "construction_audit_litigation"}
    assert (
        signal_source_registry.get_signal_source_names(runtime_config)
        == get_construction_signal_source_names()
    )


def test_get_signal_source_construction_vertical_lookup() -> None:
    runtime_config = {"vertical": "construction_audit_litigation"}
    assert (
        signal_source_registry.get_signal_source("court_dockets", runtime_config)
        == get_construction_signal_source("court_dockets")
    )


def test_has_signal_source_construction_vertical_lookup() -> None:
    runtime_config = {"vertical": "construction_audit_litigation"}
    assert signal_source_registry.has_signal_source("court_dockets", runtime_config) is True


def test_cross_vertical_permit_in_fleet_only() -> None:
    assert signal_source_registry.has_signal_source("permit", None) is True
    assert (
        signal_source_registry.has_signal_source(
            "permit",
            {"vertical": "construction_audit_litigation"},
        )
        is False
    )


def test_cross_vertical_court_dockets_in_construction_only() -> None:
    assert signal_source_registry.has_signal_source("court_dockets", None) is False
    assert (
        signal_source_registry.has_signal_source(
            "court_dockets",
            {"vertical": "construction_audit_litigation"},
        )
        is True
    )


def test_invalid_runtime_config_propagates_error() -> None:
    with pytest.raises(ValueError, match="runtime_config must be a mapping"):
        signal_source_registry.get_signal_source_registry("not-a-mapping")


def test_invalid_vertical_propagates_error() -> None:
    with pytest.raises(ValueError, match="is not supported"):
        signal_source_registry.get_signal_source_registry({"vertical": "unknown_vertical"})


def test_invalid_source_name_propagates_error_from_active_registry() -> None:
    with pytest.raises(ValueError, match="not a supported fleet signal source"):
        signal_source_registry.get_signal_source("unknown_source", None)


def test_non_string_source_name_propagates_error_from_active_registry() -> None:
    with pytest.raises(ValueError, match="source_name must be a string"):
        signal_source_registry.get_signal_source(123, None)


def test_has_signal_source_non_string_returns_false() -> None:
    assert signal_source_registry.has_signal_source(123, None) is False


def test_unsupported_route_target_raises_deterministic_value_error(monkeypatch: pytest.MonkeyPatch) -> None:
    def _bad_resolve_routes(runtime_config=None):
        return {"signal_source_registry": "unexpected_target"}

    monkeypatch.setattr(signal_source_registry.vertical_router, "resolve_routes", _bad_resolve_routes)

    with pytest.raises(
        ValueError,
        match="unsupported signal source registry route target: unexpected_target",
    ):
        signal_source_registry.get_signal_source_registry({"vertical": "construction_audit_litigation"})


def test_registry_copy_safety_in_fleet_mode() -> None:
    registry = signal_source_registry.get_signal_source_registry(None)
    registry["fake_source"] = {
        "source_name": "fake_source",
        "signal_category": "fake",
        "signal_tier": "tier_9",
        "entity_type": "fake",
    }

    fresh_registry = signal_source_registry.get_signal_source_registry(None)
    assert "fake_source" not in fresh_registry


def test_source_copy_safety_in_fleet_mode() -> None:
    source = signal_source_registry.get_signal_source("permit", None)
    source["signal_category"] = "modified"

    fresh_source = signal_source_registry.get_signal_source("permit", None)
    assert fresh_source["signal_category"] == "demand"


def test_registry_copy_safety_in_construction_mode() -> None:
    runtime_config = {"vertical": "construction_audit_litigation"}
    registry = signal_source_registry.get_signal_source_registry(runtime_config)
    registry["fake_source"] = {
        "source_name": "fake_source",
        "signal_category": "fake",
        "signal_tier": "tier_9",
        "entity_type": "fake",
    }

    fresh_registry = signal_source_registry.get_signal_source_registry(runtime_config)
    assert "fake_source" not in fresh_registry


def test_source_copy_safety_in_construction_mode() -> None:
    runtime_config = {"vertical": "construction_audit_litigation"}
    source = signal_source_registry.get_signal_source("court_dockets", runtime_config)
    source["signal_category"] = "modified"

    fresh_source = signal_source_registry.get_signal_source("court_dockets", runtime_config)
    assert fresh_source["signal_category"] != "modified"
