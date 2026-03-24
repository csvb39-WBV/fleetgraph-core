"""Authoritative deterministic fleet signal source registry."""

from __future__ import annotations

from copy import deepcopy
from typing import Any, Final


# Canonical fleet signal source registry
_FLEET_SIGNAL_SOURCE_REGISTRY: Final[dict[str, dict[str, str]]] = {
    "permit": {
        "source_name": "permit",
        "signal_category": "demand",
        "signal_tier": "tier_1",
        "entity_type": "company",
    },
    "rfp": {
        "source_name": "rfp",
        "signal_category": "procurement",
        "signal_tier": "tier_1",
        "entity_type": "company",
    },
    "company": {
        "source_name": "company",
        "signal_category": "entity",
        "signal_tier": "tier_2",
        "entity_type": "company",
    },
    "partner": {
        "source_name": "partner",
        "signal_category": "relationship",
        "signal_tier": "tier_2",
        "entity_type": "company",
    },
    "telematics": {
        "source_name": "telematics",
        "signal_category": "usage",
        "signal_tier": "tier_3",
        "entity_type": "asset",
    },
}

# Immutable ordered tuple of source names
_FLEET_SOURCE_NAMES: Final[tuple[str, ...]] = tuple(_FLEET_SIGNAL_SOURCE_REGISTRY.keys())


def get_fleet_signal_source_registry() -> dict[str, dict[str, str]]:
    """Return a copy-safe fleet signal source registry.
    
    Returns:
        dict[str, dict[str, str]]: Dictionary keyed by source name,
            values are dictionaries with signal source metadata.
    """
    return deepcopy(_FLEET_SIGNAL_SOURCE_REGISTRY)


def get_fleet_signal_source(source_name: Any) -> dict[str, str]:
    """Retrieve a single fleet signal source by name.
    
    Args:
        source_name: Name of the signal source to retrieve.
    
    Returns:
        dict[str, str]: Dictionary containing signal source metadata
            with keys: source_name, signal_category, signal_tier, entity_type.
    
    Raises:
        ValueError: If source_name is not a string, is empty/whitespace-only,
                   or is not a supported fleet source.
    """
    if not isinstance(source_name, str):
        raise ValueError("source_name must be a string")
    
    normalized_name = source_name.strip()
    
    if not normalized_name:
        raise ValueError("source_name cannot be empty or whitespace-only")
    
    if normalized_name not in _FLEET_SIGNAL_SOURCE_REGISTRY:
        raise ValueError(f"source_name '{normalized_name}' is not a supported fleet signal source")
    
    return deepcopy(_FLEET_SIGNAL_SOURCE_REGISTRY[normalized_name])


def has_fleet_signal_source(source_name: Any) -> bool:
    """Check if a source name exists in the fleet registry.
    
    Args:
        source_name: Value to check.
    
    Returns:
        bool: True if the source name exists in the fleet registry
            (after whitespace normalization), False otherwise.
            Never raises an exception.
    """
    if not isinstance(source_name, str):
        return False
    
    normalized_name = source_name.strip()
    return normalized_name in _FLEET_SIGNAL_SOURCE_REGISTRY


def get_fleet_signal_source_names() -> tuple[str, ...]:
    """Return the immutable tuple of fleet signal source names.
    
    Returns:
        tuple[str, ...]: Ordered tuple of supported fleet source names.
    """
    return _FLEET_SOURCE_NAMES
