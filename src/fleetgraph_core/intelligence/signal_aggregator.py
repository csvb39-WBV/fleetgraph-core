"""Signal aggregation and normalization for downstream intelligence layers."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any


NormalizedSignal = dict[str, object]


def _validate_non_empty_string(value: object, field_name: str) -> str:
    if not isinstance(value, str):
        raise ValueError(f"{field_name} must be a non-empty string")

    normalized = value.strip()
    if not normalized:
        raise ValueError(f"{field_name} must be a non-empty string")

    return normalized


def _normalize_data(data: object) -> dict[str, object]:
    if isinstance(data, Mapping):
        normalized_items = sorted(
            ((str(key), value) for key, value in data.items()),
            key=lambda item: item[0],
        )
        return {key: value for key, value in normalized_items}

    if isinstance(data, str):
        return {"raw": data.strip()}

    return {"raw": data}


def _normalize_signal(signal: object) -> NormalizedSignal:
    if not isinstance(signal, Mapping):
        return {
            "source": "unknown",
            "normalized_data": {"raw": signal},
            "valid": False,
            "error": "signal must be a mapping",
        }

    source = signal.get("source")
    data = signal.get("data")

    try:
        normalized_source = _validate_non_empty_string(source, "source")
    except ValueError:
        return {
            "source": "unknown",
            "normalized_data": _normalize_data(data),
            "valid": False,
            "error": "source must be a non-empty string",
        }

    if data is None:
        return {
            "source": normalized_source,
            "normalized_data": {},
            "valid": False,
            "error": "data is required",
        }

    return {
        "source": normalized_source,
        "normalized_data": _normalize_data(data),
        "valid": True,
    }


def aggregate_signals(signals: object) -> dict[str, list[NormalizedSignal]]:
    """Aggregate heterogeneous signals into a deterministic normalized structure."""
    if not isinstance(signals, list):
        raise ValueError("signals must be a list")

    aggregated_signals = [_normalize_signal(signal) for signal in signals]

    # Deterministic ordering by source, validity, and error text keeps aggregation stable.
    aggregated_signals.sort(
        key=lambda item: (
            str(item.get("source", "")),
            not bool(item.get("valid", False)),
            str(item.get("error", "")),
        )
    )

    return {"aggregated_signals": aggregated_signals}


__all__ = ["aggregate_signals"]