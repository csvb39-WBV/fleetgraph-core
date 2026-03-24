"""Deterministic construction vertical signal-source registry authority."""

from __future__ import annotations

from copy import deepcopy
from typing import Final


_CONSTRUCTION_SIGNAL_SOURCE_REGISTRY: Final[dict[str, dict[str, str]]] = {
    "court_dockets": {
        "source_name": "court_dockets",
        "signal_category": "litigation",
        "signal_tier": "tier_1",
        "entity_type": "company",
    },
    "regulatory_enforcement": {
        "source_name": "regulatory_enforcement",
        "signal_category": "enforcement",
        "signal_tier": "tier_1",
        "entity_type": "company",
    },
    "osha_citations": {
        "source_name": "osha_citations",
        "signal_category": "safety",
        "signal_tier": "tier_1",
        "entity_type": "company",
    },
    "mechanics_liens": {
        "source_name": "mechanics_liens",
        "signal_category": "payment_risk",
        "signal_tier": "tier_2",
        "entity_type": "project",
    },
    "bond_claims": {
        "source_name": "bond_claims",
        "signal_category": "financial_risk",
        "signal_tier": "tier_2",
        "entity_type": "project",
    },
}


def _normalize_source_name_for_lookup(source_name: str) -> str:
    if not isinstance(source_name, str):
        raise ValueError("source_name must be a string")

    normalized_source_name = source_name.strip()
    if normalized_source_name == "":
        raise ValueError("source_name cannot be empty or whitespace-only")

    return normalized_source_name


def get_construction_signal_source_registry() -> dict[str, dict[str, str]]:
    return deepcopy(_CONSTRUCTION_SIGNAL_SOURCE_REGISTRY)


def get_construction_signal_source(source_name: str) -> dict[str, str]:
    normalized_source_name = _normalize_source_name_for_lookup(source_name)

    source_record = _CONSTRUCTION_SIGNAL_SOURCE_REGISTRY.get(normalized_source_name)
    if source_record is None:
        raise ValueError(
            f"construction signal source '{normalized_source_name}' is not supported"
        )

    return deepcopy(source_record)


def has_construction_signal_source(source_name: str) -> bool:
    if not isinstance(source_name, str):
        return False

    normalized_source_name = source_name.strip()
    if normalized_source_name == "":
        return False

    return normalized_source_name in _CONSTRUCTION_SIGNAL_SOURCE_REGISTRY


def get_construction_signal_source_names() -> tuple[str, ...]:
    return tuple(_CONSTRUCTION_SIGNAL_SOURCE_REGISTRY.keys())


__all__ = [
    "get_construction_signal_source_registry",
    "get_construction_signal_source",
    "has_construction_signal_source",
    "get_construction_signal_source_names",
]
