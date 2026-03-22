"""Deterministic demand translation from normalized signals to ICP opportunity output."""

from __future__ import annotations

from collections.abc import Mapping


SignalCategoryWeights = dict[str, float]

ICP_TRANSLATION_REGISTRY: dict[str, SignalCategoryWeights] = {
    "DLR": {
        "EXPANSION": 1.0,
        "HIRING": 0.9,
        "PERMITS": 0.8,
        "OPERATIONS": 0.7,
    },
    "UPFITTER": {
        "PERMITS": 1.0,
        "EXPANSION": 0.8,
        "PROCUREMENT": 0.8,
        "RISK": 0.5,
    },
    "LEASING": {
        "EXPANSION": 1.0,
        "OPERATIONS": 0.9,
        "ESG": 0.6,
        "PROCUREMENT": 0.7,
    },
    "OEM": {
        "PROCUREMENT": 1.0,
        "EXPANSION": 0.9,
        "PERMITS": 0.7,
        "COMPLIANCE": 0.6,
    },
}


def _validate_non_empty_string(value: object, field_name: str) -> str:
    if not isinstance(value, str):
        raise ValueError(f"{field_name} must be a non-empty string")

    normalized = value.strip()
    if not normalized:
        raise ValueError(f"{field_name} must be a non-empty string")

    return normalized


def _validate_input(payload: object) -> Mapping[str, object]:
    if not isinstance(payload, Mapping):
        raise ValueError("input must be a mapping")

    company_id = _validate_non_empty_string(payload.get("company_id"), "company_id")
    del company_id

    icp_type = _validate_non_empty_string(payload.get("icp_type"), "icp_type")
    del icp_type

    signals = payload.get("signals")
    if not isinstance(signals, list):
        raise ValueError("signals must be a list")

    return payload


def _validate_signal_structure(signal: object, index: int) -> Mapping[str, object]:
    if not isinstance(signal, Mapping):
        raise ValueError(f"signals[{index}] must be a mapping")

    required_fields = ("signal_category", "signal_value", "valid")
    for field_name in required_fields:
        if field_name not in signal:
            raise ValueError(f"signals[{index}] missing required field: {field_name}")

    category = _validate_non_empty_string(
        signal.get("signal_category"),
        f"signals[{index}].signal_category",
    )
    del category

    valid = signal.get("valid")
    if not isinstance(valid, bool):
        raise ValueError(f"signals[{index}].valid must be a boolean")

    signal_value = signal.get("signal_value")
    if not isinstance(signal_value, (str, int, float, bool)):
        raise ValueError(
            f"signals[{index}].signal_value must be string, int, float, or bool"
        )

    return signal


def translate_demand(payload: object) -> dict[str, object]:
    """Translate normalized company signals into an ICP-specific opportunity output."""
    parsed_payload = _validate_input(payload)

    icp_input = _validate_non_empty_string(parsed_payload.get("icp_type"), "icp_type")
    icp_key = icp_input.upper()
    if icp_key not in ICP_TRANSLATION_REGISTRY:
        raise ValueError(f"unsupported icp_type: {icp_input}")

    signals = parsed_payload["signals"]
    assert isinstance(signals, list)

    matching_weights = ICP_TRANSLATION_REGISTRY[icp_key]
    matched_categories: list[str] = []

    for index, signal in enumerate(signals):
        parsed_signal = _validate_signal_structure(signal, index)
        if parsed_signal["valid"] is False:
            continue

        category = _validate_non_empty_string(
            parsed_signal.get("signal_category"),
            f"signals[{index}].signal_category",
        ).upper()
        if category in matching_weights and category not in matched_categories:
            matched_categories.append(category)

    matched_weight = sum(matching_weights[category] for category in matched_categories)
    total_possible_weight = sum(matching_weights.values())

    opportunity_score = 0.0
    if total_possible_weight > 0:
        opportunity_score = round((matched_weight / total_possible_weight) * 100.0, 2)

    if matched_categories:
        ordered_categories = [
            category.title() for category in matching_weights.keys() if category in matched_categories
        ]
        reason = " + ".join(ordered_categories)
    else:
        reason = "No ICP-aligned signals"

    return {
        "icp": icp_input,
        "opportunity_score": opportunity_score,
        "reason": reason,
    }


__all__ = ["translate_demand"]