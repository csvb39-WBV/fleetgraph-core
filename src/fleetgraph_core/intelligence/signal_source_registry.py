"""Deterministic vertical-aware signal source registry injection layer."""

from __future__ import annotations

from typing import Any, Callable, Mapping, TypedDict

from fleetgraph_core.runtime import vertical_router
from fleetgraph_core.intelligence.fleet_signal_source_registry import (
    get_fleet_signal_source,
    get_fleet_signal_source_names,
    get_fleet_signal_source_registry,
    has_fleet_signal_source,
)
from fleetgraph_core.intelligence.construction_signal_source_registry import (
    get_construction_signal_source,
    get_construction_signal_source_names,
    get_construction_signal_source_registry,
    has_construction_signal_source,
)


class _RegistryAdapter(TypedDict):
    get_registry: Callable[[], dict[str, dict[str, str]]]
    get_source: Callable[[Any], dict[str, str]]
    has_source: Callable[[Any], bool]
    get_source_names: Callable[[], tuple[str, ...]]


_ROUTE_DOMAIN = "signal_source_registry"
_FLEET_ROUTE_TARGET = "fleet_signal_source_registry"
_CONSTRUCTION_ROUTE_TARGET = "construction_signal_source_registry"

_REGISTRY_ADAPTERS: dict[str, _RegistryAdapter] = {
    _FLEET_ROUTE_TARGET: {
        "get_registry": get_fleet_signal_source_registry,
        "get_source": get_fleet_signal_source,
        "has_source": has_fleet_signal_source,
        "get_source_names": get_fleet_signal_source_names,
    },
    _CONSTRUCTION_ROUTE_TARGET: {
        "get_registry": get_construction_signal_source_registry,
        "get_source": get_construction_signal_source,
        "has_source": has_construction_signal_source,
        "get_source_names": get_construction_signal_source_names,
    },
}


def _resolve_registry_adapter(
    runtime_config: Mapping[str, Any] | None,
) -> _RegistryAdapter:
    route_target = vertical_router.resolve_routes(runtime_config)[_ROUTE_DOMAIN]
    adapter = _REGISTRY_ADAPTERS.get(route_target)
    if adapter is None:
        raise ValueError(
            "unsupported signal source registry route target: "
            f"{route_target}"
        )
    return adapter


def get_signal_source_registry(
    runtime_config: Mapping[str, Any] | None = None,
) -> dict[str, dict[str, str]]:
    """Return the active vertical signal source registry as a copy-safe dict."""
    adapter = _resolve_registry_adapter(runtime_config)
    return adapter["get_registry"]()


def get_signal_source(
    source_name: str,
    runtime_config: Mapping[str, Any] | None = None,
) -> dict[str, str]:
    """Return a single source record from the active vertical registry."""
    adapter = _resolve_registry_adapter(runtime_config)
    return adapter["get_source"](source_name)


def has_signal_source(
    source_name: str,
    runtime_config: Mapping[str, Any] | None = None,
) -> bool:
    """Check source presence in the active vertical registry."""
    adapter = _resolve_registry_adapter(runtime_config)
    return adapter["has_source"](source_name)


def get_signal_source_names(
    runtime_config: Mapping[str, Any] | None = None,
) -> tuple[str, ...]:
    """Return deterministic source names for the active vertical registry."""
    adapter = _resolve_registry_adapter(runtime_config)
    return adapter["get_source_names"]()


def list_signal_sources(
    runtime_config: Mapping[str, Any] | None = None,
) -> tuple[str, ...]:
    """Backward-compatible alias for active source-name listing."""
    return get_signal_source_names(runtime_config)


def resolve_signal_source(
    *,
    source_name: object,
    source_type: object,
    runtime_config: Mapping[str, Any] | None = None,
) -> dict[str, object]:
    """Backward-compatible source resolver using active registry records.

    This keeps the historical shape (`source_class` and `valid`) while
    deferring source-name validation to the selected registry authority.
    """
    if not isinstance(source_type, str):
        raise ValueError("source_type must be a non-empty string")

    normalized_source_type = source_type.strip().lower()
    if not normalized_source_type:
        raise ValueError("source_type must be a non-empty string")

    source_record = get_signal_source(source_name, runtime_config)
    expected_source_class = str(source_record["entity_type"]).strip().lower()
    if normalized_source_type != expected_source_class:
        raise ValueError(
            "source_type does not match registry classification for "
            f"{source_record['source_name']}: expected {expected_source_class}"
        )

    return {
        "source_name": source_record["source_name"],
        "signal_tier": source_record["signal_tier"],
        "signal_category": source_record["signal_category"],
        "source_class": source_record["entity_type"],
        "valid": True,
    }


__all__ = [
    "get_signal_source_registry",
    "get_signal_source",
    "has_signal_source",
    "get_signal_source_names",
    "list_signal_sources",
    "resolve_signal_source",
]