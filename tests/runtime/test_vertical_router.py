"""Tests for deterministic vertical routing behavior."""

from __future__ import annotations

from collections.abc import Mapping
import importlib
import sys
import types

import pytest

FLEET_ROUTES = {
    "signal_source_registry": "fleet_signal_source_registry",
    "scoring_engine": "fleet_scoring_engine",
    "opportunity_engine": "fleet_opportunity_engine",
    "api_surface": "fleet_api_surface",
}

CONSTRUCTION_ROUTES = {
    "signal_source_registry": "construction_signal_source_registry",
    "scoring_engine": "construction_scoring_engine",
    "opportunity_engine": "construction_opportunity_engine",
    "api_surface": "construction_api_surface",
}


@pytest.fixture(autouse=True)
def install_canonical_vertical_config_stub(monkeypatch: pytest.MonkeyPatch) -> None:
    """Install a canonical config stub at fleetgraph_core.config.vertical_config."""

    config_module = types.ModuleType("fleetgraph_core.config")
    vertical_config_module = types.ModuleType("fleetgraph_core.config.vertical_config")

    supported_verticals = {"fleet", "construction_audit_litigation"}

    def validate_vertical(vertical: str) -> str:
        if not isinstance(vertical, str):
            raise ValueError("vertical must be a string")

        normalized_vertical = vertical.strip()
        if normalized_vertical == "":
            raise ValueError("vertical cannot be empty or whitespace-only")

        if normalized_vertical not in supported_verticals:
            raise ValueError(f"vertical '{normalized_vertical}' is not supported")

        return normalized_vertical

    def get_active_vertical(runtime_config: Mapping[str, object] | None = None) -> str:
        if runtime_config is None or runtime_config == {}:
            return "fleet"

        if not isinstance(runtime_config, Mapping):
            raise ValueError("runtime_config must be a mapping")

        vertical_value = runtime_config.get("vertical", "fleet")
        return validate_vertical(vertical_value)

    vertical_config_module.validate_vertical = validate_vertical
    vertical_config_module.get_active_vertical = get_active_vertical

    config_module.vertical_config = vertical_config_module

    monkeypatch.setitem(sys.modules, "fleetgraph_core.config", config_module)
    monkeypatch.setitem(sys.modules, "fleetgraph_core.config.vertical_config", vertical_config_module)


def _vertical_router_module():
    module_name = "fleetgraph_core.runtime.vertical_router"
    if module_name in sys.modules:
        del sys.modules[module_name]
    return importlib.import_module(module_name)


class TestSupportedRouteDomains:
    def test_returns_exact_supported_route_domains(self):
        vertical_router = _vertical_router_module()

        assert vertical_router.get_supported_route_domains() == (
            "signal_source_registry",
            "scoring_engine",
            "opportunity_engine",
            "api_surface",
        )

    def test_returns_immutable_tuple(self):
        vertical_router = _vertical_router_module()
        domains = vertical_router.get_supported_route_domains()

        assert isinstance(domains, tuple)

        with pytest.raises(TypeError):
            domains[0] = "other"


class TestFleetRouteResolution:
    def test_get_vertical_routes_for_fleet(self):
        vertical_router = _vertical_router_module()

        assert vertical_router.get_vertical_routes("fleet") == FLEET_ROUTES

    def test_resolve_routes_with_none_defaults_to_fleet(self):
        vertical_router = _vertical_router_module()

        assert vertical_router.resolve_routes(None) == FLEET_ROUTES

    def test_resolve_routes_with_empty_mapping_defaults_to_fleet(self):
        vertical_router = _vertical_router_module()

        assert vertical_router.resolve_routes({}) == FLEET_ROUTES

    def test_resolve_routes_with_explicit_fleet(self):
        vertical_router = _vertical_router_module()

        assert vertical_router.resolve_routes({"vertical": "fleet"}) == FLEET_ROUTES


class TestConstructionRouteResolution:
    def test_get_vertical_routes_for_construction(self):
        vertical_router = _vertical_router_module()

        assert vertical_router.get_vertical_routes("construction_audit_litigation") == CONSTRUCTION_ROUTES

    def test_resolve_routes_with_explicit_construction(self):
        vertical_router = _vertical_router_module()

        assert vertical_router.resolve_routes({"vertical": "construction_audit_litigation"}) == CONSTRUCTION_ROUTES

    def test_whitespace_padded_vertical_is_resolved_by_canonical_config(self):
        vertical_router = _vertical_router_module()

        assert vertical_router.get_vertical_routes("  construction_audit_litigation  ") == CONSTRUCTION_ROUTES


class TestRouteTargetLookup:
    @pytest.mark.parametrize(
        ("route_domain", "expected_target"),
        [
            ("signal_source_registry", "fleet_signal_source_registry"),
            ("scoring_engine", "fleet_scoring_engine"),
            ("opportunity_engine", "fleet_opportunity_engine"),
            ("api_surface", "fleet_api_surface"),
        ],
    )
    def test_route_target_lookup_for_fleet(self, route_domain: str, expected_target: str):
        vertical_router = _vertical_router_module()

        assert vertical_router.get_route_target(route_domain, "fleet") == expected_target

    @pytest.mark.parametrize(
        ("route_domain", "expected_target"),
        [
            ("signal_source_registry", "construction_signal_source_registry"),
            ("scoring_engine", "construction_scoring_engine"),
            ("opportunity_engine", "construction_opportunity_engine"),
            ("api_surface", "construction_api_surface"),
        ],
    )
    def test_route_target_lookup_for_construction(self, route_domain: str, expected_target: str):
        vertical_router = _vertical_router_module()

        assert vertical_router.get_route_target(route_domain, "construction_audit_litigation") == expected_target


class TestRouteDomainFailures:
    def test_unsupported_route_domain_rejected(self):
        vertical_router = _vertical_router_module()

        with pytest.raises(
            ValueError,
            match="invalid route domain: unsupported route domain: unknown_domain",
        ):
            vertical_router.get_route_target("unknown_domain", "fleet")

    def test_empty_route_domain_rejected(self):
        vertical_router = _vertical_router_module()

        with pytest.raises(ValueError, match="invalid route domain: route_domain must not be blank"):
            vertical_router.get_route_target("", "fleet")

    def test_whitespace_route_domain_rejected(self):
        vertical_router = _vertical_router_module()

        with pytest.raises(ValueError, match="invalid route domain: route_domain must not be blank"):
            vertical_router.get_route_target("   ", "fleet")

    def test_non_string_route_domain_rejected(self):
        vertical_router = _vertical_router_module()

        with pytest.raises(ValueError, match="invalid route domain: route_domain must be a string"):
            vertical_router.get_route_target(123, "fleet")  # type: ignore[arg-type]


class TestVerticalFailurePropagation:
    def test_invalid_vertical_string_propagates(self):
        vertical_router = _vertical_router_module()

        with pytest.raises(ValueError, match="vertical 'invalid_vertical' is not supported"):
            vertical_router.get_vertical_routes("invalid_vertical")

    def test_blank_vertical_propagates(self):
        vertical_router = _vertical_router_module()

        with pytest.raises(ValueError, match="vertical cannot be empty or whitespace-only"):
            vertical_router.get_vertical_routes("   ")

    def test_non_string_vertical_propagates(self):
        vertical_router = _vertical_router_module()

        with pytest.raises(ValueError, match="vertical must be a string"):
            vertical_router.get_vertical_routes(123)  # type: ignore[arg-type]

    def test_invalid_runtime_config_propagates(self):
        vertical_router = _vertical_router_module()

        with pytest.raises(ValueError, match="runtime_config must be a mapping"):
            vertical_router.resolve_routes(runtime_config=["fleet"])  # type: ignore[arg-type]


class TestCopySafety:
    def test_modifying_returned_routes_does_not_mutate_internal_state(self):
        vertical_router = _vertical_router_module()
        returned_routes = vertical_router.get_vertical_routes("fleet")
        returned_routes["signal_source_registry"] = "changed"

        assert vertical_router.get_vertical_routes("fleet") == FLEET_ROUTES

    def test_repeated_calls_remain_stable(self):
        vertical_router = _vertical_router_module()
        first_call = vertical_router.get_vertical_routes("construction_audit_litigation")
        second_call = vertical_router.get_vertical_routes("construction_audit_litigation")

        assert first_call == CONSTRUCTION_ROUTES
        assert second_call == CONSTRUCTION_ROUTES
        assert first_call is not second_call
