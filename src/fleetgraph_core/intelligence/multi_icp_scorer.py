"""Multi-ICP scoring orchestration using the demand translation engine."""

from __future__ import annotations

from collections.abc import Mapping

from fleetgraph_core.intelligence.demand_translation_engine import (
    ICP_TRANSLATION_REGISTRY,
    translate_demand,
)


SUPPORTED_ICPS = tuple(sorted(ICP_TRANSLATION_REGISTRY.keys()))


def _validate_non_empty_string(value: object, field_name: str) -> str:
    if not isinstance(value, str):
        raise ValueError(f"{field_name} must be a non-empty string")

    normalized = value.strip()
    if not normalized:
        raise ValueError(f"{field_name} must be a non-empty string")

    return normalized


def score_multi_icp(payload: object) -> dict[str, object]:
    """Score one company's signal set across all supported ICPs."""
    if not isinstance(payload, Mapping):
        raise ValueError("input must be a mapping")

    company_id = _validate_non_empty_string(payload.get("company_id"), "company_id")

    signals = payload.get("signals")
    if not isinstance(signals, list):
        raise ValueError("signals must be a list")

    opportunities = []
    for icp in SUPPORTED_ICPS:
        opportunity = translate_demand(
            {
                "company_id": company_id,
                "signals": signals,
                "icp_type": icp,
            }
        )
        opportunities.append(opportunity)

    opportunities.sort(
        key=lambda item: (-float(item["opportunity_score"]), str(item["icp"])),
    )

    return {
        "company_id": company_id,
        "opportunities": opportunities,
    }


__all__ = ["SUPPORTED_ICPS", "score_multi_icp"]