
from __future__ import annotations

from copy import deepcopy
from typing import Any


class _SignalSourceRegistry:
    def __init__(self) -> None:
        self._registry = {
            "fleet": {
                "permit": {"source_name": "permit"},
                "rfp": {"source_name": "rfp"},
                "partner": {"source_name": "partner"},
            },
            "construction_audit_litigation": {
                "court_dockets": {"source_name": "court_dockets"},
                "bond_claims": {"source_name": "bond_claims"},
            },
        }

    def get_signal_source_registry(
        self,
        runtime_config: dict[str, Any] | None = None,
    ) -> dict[str, dict[str, str]]:
        active_vertical = _resolve_vertical(runtime_config)
        return deepcopy(self._registry[active_vertical])

    def get_signal_source(
        self,
        source_name: Any,
        runtime_config: dict[str, Any] | None = None,
    ) -> dict[str, str]:
        normalized_source = _normalize_source_name(source_name)
        registry = self.get_signal_source_registry(runtime_config)
        if normalized_source not in registry:
            raise KeyError(normalized_source)
        return deepcopy(registry[normalized_source])

    def has_signal_source(
        self,
        source_name: Any,
        runtime_config: dict[str, Any] | None = None,
    ) -> bool:
        try:
            normalized_source = _normalize_source_name(source_name)
        except ValueError:
            return False
        registry = self.get_signal_source_registry(runtime_config)
        return normalized_source in registry

    def get_signal_source_names(
        self,
        runtime_config: dict[str, Any] | None = None,
    ) -> tuple[str, ...]:
        registry = self.get_signal_source_registry(runtime_config)
        return tuple(registry.keys())


signal_source_registry = _SignalSourceRegistry()


def _normalize_source_name(source_name: Any) -> str:
    if not isinstance(source_name, str):
        raise ValueError("source_name is required")
    normalized = source_name.strip().lower()
    if normalized == "":
        raise ValueError("source_name is required")
    return normalized


def _resolve_vertical(runtime_config: dict[str, Any] | None = None) -> str:
    resolved_vertical = "fleet"

    if runtime_config is not None:
        if not isinstance(runtime_config, dict):
            raise ValueError("runtime_config must be a mapping")

        if "vertical" in runtime_config:
            resolved_vertical = runtime_config["vertical"]

    if not isinstance(resolved_vertical, str):
        raise ValueError("vertical must be a string")

    normalized_vertical = resolved_vertical.strip()
    if normalized_vertical == "":
        raise ValueError("vertical cannot be empty or whitespace-only")

    if normalized_vertical not in {"fleet", "construction_audit_litigation"}:
        raise ValueError(f"vertical '{normalized_vertical}' is not supported")

    return normalized_vertical


def _normalize_simple_data(data: Any) -> dict[str, Any]:
    if isinstance(data, dict):
        return {key: data[key] for key in sorted(data.keys())}
    if isinstance(data, str):
        return {"raw": data.strip()}
    return {"raw": data}


def _aggregate_simple_signals(signal_records: list[Any]) -> dict[str, Any]:
    aggregated_signals: list[dict[str, Any]] = []

    for item in signal_records:
        if not isinstance(item, dict):
            aggregated_signals.append(
                {
                    "source": "unknown",
                    "normalized_data": {"raw": str(item)},
                    "valid": False,
                    "error": "signal must be a mapping",
                }
            )
            continue

        raw_source = item.get("source")
        if not isinstance(raw_source, str) or raw_source.strip() == "":
            aggregated_signals.append(
                {
                    "source": "unknown",
                    "normalized_data": _normalize_simple_data(item.get("data", {})),
                    "valid": False,
                    "error": "source must be a non-empty string",
                }
            )
            continue

        normalized_source = raw_source.strip().lower()
        if "data" not in item:
            aggregated_signals.append(
                {
                    "source": normalized_source,
                    "normalized_data": {},
                    "valid": False,
                    "error": "data is required",
                }
            )
            continue

        aggregated_signals.append(
            {
                "source": normalized_source,
                "normalized_data": _normalize_simple_data(item["data"]),
                "valid": True,
            }
        )

    aggregated_signals.sort(
        key=lambda signal: (
            signal["source"],
            repr(signal["normalized_data"]),
            signal.get("error", ""),
        )
    )

    return {"aggregated_signals": aggregated_signals}


def _aggregate_vertical_signals(
    signal_records: list[Any],
    *,
    runtime_config: dict[str, Any] | None = None,
) -> dict[str, Any]:
    active_vertical = _resolve_vertical(runtime_config)

    accepted_records: list[dict[str, Any]] = []
    rejected_records: list[dict[str, Any]] = []
    accepted_sources: list[str] = []
    rejected_sources: list[str] = []

    for item in signal_records:
        if not isinstance(item, dict):
            rejected_records.append(
                {
                    "record": item,
                    "reason": "record must be a dictionary",
                }
            )
            continue

        try:
            normalized_source = _normalize_source_name(item.get("source_name"))
        except ValueError:
            rejected_records.append(
                {
                    "record": deepcopy(item),
                    "reason": "source_name is required",
                }
            )
            continue

        if not signal_source_registry.has_signal_source(normalized_source, runtime_config):
            rejected_records.append(
                {
                    "record": deepcopy({**item, "source_name": normalized_source}),
                    "reason": "source_name is not supported for active vertical",
                }
            )
            if normalized_source not in rejected_sources:
                rejected_sources.append(normalized_source)
            continue

        record_copy = deepcopy(item)
        record_copy["source_name"] = normalized_source
        accepted_records.append(record_copy)

        if normalized_source not in accepted_sources:
            accepted_sources.append(normalized_source)

    return {
        "ok": True,
        "vertical": active_vertical,
        "accepted_sources": accepted_sources,
        "rejected_sources": rejected_sources,
        "accepted_records": accepted_records,
        "rejected_records": rejected_records,
        "accepted_count": len(accepted_records),
        "rejected_count": len(rejected_records),
    }


def aggregate_signals(
    signal_records: object,
    *,
    vertical: str | None = None,
    runtime_config: dict[str, Any] | None = None,
) -> dict[str, Any]:
    if not isinstance(signal_records, list):
        if isinstance(signal_records, dict) and "records" in signal_records:
            raise ValueError("signal_records must be a list")
        raise ValueError("signals must be a list")

    if runtime_config is None and vertical is not None:
        runtime_config = {"vertical": vertical}

    simple_mode = False
    for item in signal_records:
        if isinstance(item, dict) and ("source" in item or "data" in item):
            simple_mode = True
            break

    if simple_mode:
        return _aggregate_simple_signals(signal_records)

    return _aggregate_vertical_signals(signal_records, runtime_config=runtime_config)


__all__ = ["aggregate_signals", "signal_source_registry"]