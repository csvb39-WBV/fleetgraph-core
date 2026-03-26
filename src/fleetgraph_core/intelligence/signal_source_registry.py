"""Deterministic signal source registry with legacy and vertical-aware surfaces."""

from __future__ import annotations

from copy import deepcopy
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
_LEGACY_SENTINEL = object()

_LEGACY_SIGNAL_SOURCE_REGISTRY: dict[str, dict[str, object]] = {
    "job_postings": {
        "source_name": "job_postings",
        "signal_tier": 3,
        "signal_category": "OPERATIONS",
        "source_class": "company",
    },
    "news_rss_feeds": {
        "source_name": "news_rss_feeds",
        "signal_tier": 1,
        "signal_category": "MEDIA",
        "source_class": "media",
    },
    "patent_filings": {
        "source_name": "patent_filings",
        "signal_tier": 2,
        "signal_category": "INTELLECTUAL_PROPERTY",
        "source_class": "filing",
    },
    "sec_filings": {
        "source_name": "sec_filings",
        "signal_tier": 0,
        "signal_category": "COMPLIANCE",
        "source_class": "filing",
    },
}

_LEGACY_SOURCE_NAMES: tuple[str, ...] = tuple(sorted(_LEGACY_SIGNAL_SOURCE_REGISTRY.keys()))

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


def _is_explicit_fleet_default(runtime_config: Mapping[str, Any] | None) -> bool:
    return runtime_config is None or runtime_config == {}


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


def _normalize_legacy_source_name(source_name: object) -> str:
    if not isinstance(source_name, str):
        raise ValueError("source_name must be a non-empty string")

    normalized_source_name = source_name.strip().lower()
    if normalized_source_name == "":
        raise ValueError("source_name must be a non-empty string")

    return normalized_source_name


def _normalize_legacy_source_type(source_type: object) -> str:
    if not isinstance(source_type, str):
        raise ValueError("source_type must be a non-empty string")

    normalized_source_type = source_type.strip().lower()
    if normalized_source_type == "":
        raise ValueError("source_type must be a non-empty string")

    return normalized_source_type


def _get_legacy_signal_source(source_name: object) -> dict[str, object]:
    normalized_source_name = _normalize_legacy_source_name(source_name)
    if normalized_source_name not in _LEGACY_SIGNAL_SOURCE_REGISTRY:
        raise ValueError(f"unknown source_name: {normalized_source_name}")
    return deepcopy(_LEGACY_SIGNAL_SOURCE_REGISTRY[normalized_source_name])


def get_signal_source_registry(
    runtime_config: Mapping[str, Any] | None = _LEGACY_SENTINEL,
) -> dict[str, dict[str, object]]:
    """Return either the legacy default registry or an active vertical registry."""
    if runtime_config is _LEGACY_SENTINEL:
        return deepcopy(_LEGACY_SIGNAL_SOURCE_REGISTRY)
    if _is_explicit_fleet_default(runtime_config):
        return get_fleet_signal_source_registry()

    adapter = _resolve_registry_adapter(runtime_config)
    return adapter["get_registry"]()


def get_signal_source(
    source_name: str,
    runtime_config: Mapping[str, Any] | None = _LEGACY_SENTINEL,
) -> dict[str, object]:
    """Return a source record from the legacy or active vertical registry."""
    if runtime_config is _LEGACY_SENTINEL:
        return _get_legacy_signal_source(source_name)
    if _is_explicit_fleet_default(runtime_config):
        return get_fleet_signal_source(source_name)

    adapter = _resolve_registry_adapter(runtime_config)
    return adapter["get_source"](source_name)


def has_signal_source(
    source_name: str,
    runtime_config: Mapping[str, Any] | None = _LEGACY_SENTINEL,
) -> bool:
    """Check source presence in the legacy or active vertical registry."""
    if runtime_config is _LEGACY_SENTINEL:
        if not isinstance(source_name, str):
            return False
        normalized_source_name = source_name.strip().lower()
        if normalized_source_name == "":
            return False
        return normalized_source_name in _LEGACY_SIGNAL_SOURCE_REGISTRY
    if _is_explicit_fleet_default(runtime_config):
        return has_fleet_signal_source(source_name)

    adapter = _resolve_registry_adapter(runtime_config)
    return adapter["has_source"](source_name)


def get_signal_source_names(
    runtime_config: Mapping[str, Any] | None = _LEGACY_SENTINEL,
) -> tuple[str, ...]:
    """Return deterministic source names for the legacy or active vertical registry."""
    if runtime_config is _LEGACY_SENTINEL:
        return _LEGACY_SOURCE_NAMES
    if _is_explicit_fleet_default(runtime_config):
        return get_fleet_signal_source_names()

    adapter = _resolve_registry_adapter(runtime_config)
    return adapter["get_source_names"]()


def list_signal_sources(
    runtime_config: Mapping[str, Any] | None = _LEGACY_SENTINEL,
) -> tuple[str, ...]:
    """Backward-compatible alias for source-name listing."""
    return get_signal_source_names(runtime_config)


def resolve_signal_source(
    *,
    source_name: object,
    source_type: object,
    runtime_config: Mapping[str, Any] | None = _LEGACY_SENTINEL,
) -> dict[str, object]:
    """Resolve a source record using the legacy or active vertical registry."""
    normalized_source_type = _normalize_legacy_source_type(source_type)

    if runtime_config is _LEGACY_SENTINEL:
        source_record = _get_legacy_signal_source(source_name)
        expected_source_class = str(source_record["source_class"]).strip().lower()
        if normalized_source_type != expected_source_class:
            raise ValueError(
                "source_type does not match registry classification for "
                f"{source_record['source_name']}: expected {expected_source_class}"
            )

        return {
            "source_name": source_record["source_name"],
            "signal_tier": source_record["signal_tier"],
            "signal_category": source_record["signal_category"],
            "source_class": source_record["source_class"],
            "valid": True,
        }

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
