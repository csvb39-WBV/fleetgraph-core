"""Authoritative deterministic signal source registry for intelligence inputs."""

from __future__ import annotations

from copy import deepcopy
from typing import Final


SignalSourceRecord = dict[str, object]

_SIGNAL_SOURCE_REGISTRY: Final[dict[str, SignalSourceRecord]] = {
    "sec_filings": {
        "signal_tier": 0,
        "signal_category": "COMPLIANCE",
        "source_class": "filing",
    },
    "state_business_filings": {
        "signal_tier": 0,
        "signal_category": "COMPLIANCE",
        "source_class": "filing",
    },
    "federal_contract_awards": {
        "signal_tier": 1,
        "signal_category": "PROCUREMENT",
        "source_class": "award",
    },
    "county_permit_bulletins": {
        "signal_tier": 1,
        "signal_category": "EXPANSION",
        "source_class": "permit",
    },
    "press_releases": {
        "signal_tier": 1,
        "signal_category": "EXPANSION",
        "source_class": "web",
    },
    "company_careers_pages": {
        "signal_tier": 2,
        "signal_category": "EXPANSION",
        "source_class": "web",
    },
    "job_boards": {
        "signal_tier": 2,
        "signal_category": "OPERATIONS",
        "source_class": "web",
    },
    "environmental_disclosures": {
        "signal_tier": 2,
        "signal_category": "ESG",
        "source_class": "disclosure",
    },
    "safety_incident_reports": {
        "signal_tier": 2,
        "signal_category": "RISK",
        "source_class": "incident",
    },
    "news_rss_feeds": {
        "signal_tier": 3,
        "signal_category": "OPERATIONS",
        "source_class": "media",
    },
    "web_traffic_signals": {
        "signal_tier": 3,
        "signal_category": "OPERATIONS",
        "source_class": "telemetry",
    },
    "satellite_imagery_feeds": {
        "signal_tier": 3,
        "signal_category": "EXPANSION",
        "source_class": "imagery",
    },
}


def _normalize_required_string(value: object, field_name: str) -> str:
    if not isinstance(value, str):
        raise ValueError(f"{field_name} must be a non-empty string")

    normalized = value.strip().lower()
    if not normalized:
        raise ValueError(f"{field_name} must be a non-empty string")

    return normalized


def _validate_registry_shape() -> None:
    for source_name, record in _SIGNAL_SOURCE_REGISTRY.items():
        if not isinstance(source_name, str) or source_name.strip() == "":
            raise ValueError("registry source_name must be a non-empty string")

        if not isinstance(record, dict):
            raise ValueError(f"registry record for {source_name} must be a dictionary")

        required_fields = {"signal_tier", "signal_category", "source_class"}
        missing_fields = sorted(required_fields - set(record.keys()))
        if missing_fields:
            raise ValueError(
                f"registry record for {source_name} is missing fields: "
                + ", ".join(missing_fields)
            )

        signal_tier = record["signal_tier"]
        if not isinstance(signal_tier, int) or signal_tier not in (0, 1, 2, 3):
            raise ValueError(f"registry record for {source_name} has invalid signal_tier")

        for field_name in ("signal_category", "source_class"):
            field_value = record[field_name]
            if not isinstance(field_value, str) or field_value.strip() == "":
                raise ValueError(
                    f"registry record for {source_name} has invalid {field_name}"
                )


_validate_registry_shape()


def list_signal_sources() -> tuple[str, ...]:
    """Return deterministic list of known source names."""
    return tuple(sorted(_SIGNAL_SOURCE_REGISTRY.keys()))


def get_signal_source_registry() -> dict[str, SignalSourceRecord]:
    """Return a defensive copy of the source registry."""
    return deepcopy(_SIGNAL_SOURCE_REGISTRY)


def resolve_signal_source(
    *,
    source_name: object,
    source_type: object,
) -> SignalSourceRecord:
    """Resolve and validate source metadata for deterministic downstream use."""
    normalized_source_name = _normalize_required_string(source_name, "source_name")
    normalized_source_type = _normalize_required_string(source_type, "source_type")

    registry_record = _SIGNAL_SOURCE_REGISTRY.get(normalized_source_name)
    if registry_record is None:
        raise ValueError(f"unknown source_name: {normalized_source_name}")

    expected_source_class = str(registry_record["source_class"]).strip().lower()
    if normalized_source_type != expected_source_class:
        raise ValueError(
            "source_type does not match registry classification for "
            f"{normalized_source_name}: expected {expected_source_class}"
        )

    return {
        "source_name": normalized_source_name,
        "signal_tier": registry_record["signal_tier"],
        "signal_category": registry_record["signal_category"],
        "source_class": registry_record["source_class"],
        "valid": True,
    }


__all__ = [
    "SignalSourceRecord",
    "get_signal_source_registry",
    "list_signal_sources",
    "resolve_signal_source",
]