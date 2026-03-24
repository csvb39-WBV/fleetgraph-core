"""Deterministic vertical route selection for runtime component domains."""

from __future__ import annotations

from fleetgraph_core.config import vertical_config
from typing import Any, Mapping

_ROUTE_DOMAINS: tuple[str, ...] = (
    "signal_source_registry",
    "scoring_engine",
    "opportunity_engine",
    "api_surface",
)

_VERTICAL_ROUTE_TABLE: dict[str, dict[str, str]] = {
    "fleet": {
        "signal_source_registry": "fleet_signal_source_registry",
        "scoring_engine": "fleet_scoring_engine",
        "opportunity_engine": "fleet_opportunity_engine",
        "api_surface": "fleet_api_surface",
    },
    "construction_audit_litigation": {
        "signal_source_registry": "construction_signal_source_registry",
        "scoring_engine": "construction_scoring_engine",
        "opportunity_engine": "construction_opportunity_engine",
        "api_surface": "construction_api_surface",
    },
}


def _validate_vertical(vertical: str) -> str:
    return vertical_config.validate_vertical(vertical)


def _resolve_active_vertical(runtime_config: Mapping[str, Any] | None) -> str:
    return vertical_config.get_active_vertical(runtime_config)


def _normalize_route_domain(route_domain: str) -> str:
    if not isinstance(route_domain, str):
        raise ValueError("invalid route domain: route_domain must be a string")

    normalized_route_domain = route_domain.strip()
    if normalized_route_domain == "":
        raise ValueError("invalid route domain: route_domain must not be blank")

    if normalized_route_domain not in _ROUTE_DOMAINS:
        raise ValueError(
            f"invalid route domain: unsupported route domain: {normalized_route_domain}"
        )

    return normalized_route_domain


def get_supported_route_domains() -> tuple[str, ...]:
    return _ROUTE_DOMAINS


def get_vertical_routes(vertical: str) -> dict[str, str]:
    validated_vertical = _validate_vertical(vertical)
    route_map = _VERTICAL_ROUTE_TABLE[validated_vertical]
    return dict(route_map)


def get_route_target(route_domain: str, vertical: str) -> str:
    normalized_route_domain = _normalize_route_domain(route_domain)
    vertical_routes = get_vertical_routes(vertical)
    return vertical_routes[normalized_route_domain]


def resolve_routes(runtime_config: Mapping[str, Any] | None = None) -> dict[str, str]:
    active_vertical = _resolve_active_vertical(runtime_config)
    return get_vertical_routes(active_vertical)


__all__ = [
    "get_supported_route_domains",
    "get_vertical_routes",
    "get_route_target",
    "resolve_routes",
]
