"""Deterministic ICP weighting engine for opportunity relevance scoring."""

from __future__ import annotations

from collections.abc import Mapping


REQUIRED_FIELDS = ("industry", "location", "company_size", "revenue")
FIELD_LABELS = {
    "industry": "Industry",
    "location": "Location",
    "company_size": "Company Size",
    "revenue": "Revenue",
}


def _validate_mapping(value: object, field_name: str) -> Mapping[str, object]:
    if not isinstance(value, Mapping):
        raise ValueError(f"{field_name} must be a mapping")
    return value


def _validate_required_fields(mapping: Mapping[str, object], field_name: str) -> None:
    missing_fields = [required for required in REQUIRED_FIELDS if required not in mapping]
    if missing_fields:
        raise ValueError(
            f"{field_name} is missing required fields: " + ", ".join(missing_fields)
        )


def _validate_weight(value: object, key: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"icp_matrix[{key}] must be a numeric weight")

    weight = float(value)
    if weight < 0:
        raise ValueError(f"icp_matrix[{key}] must be greater than or equal to 0")

    return weight


def _is_signal_value_present(value: object) -> bool:
    if value is None:
        return False
    if isinstance(value, str):
        return value.strip() != ""
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value > 0
    return True


def score_opportunity(
    *,
    icp_matrix: object,
    signal_data: object,
) -> dict[str, object]:
    """Score opportunity relevance using weighted ICP field alignment."""
    parsed_icp_matrix = _validate_mapping(icp_matrix, "icp_matrix")
    parsed_signal_data = _validate_mapping(signal_data, "signal_data")

    _validate_required_fields(parsed_icp_matrix, "icp_matrix")
    _validate_required_fields(parsed_signal_data, "signal_data")

    weighted_sum = 0.0
    total_weight = 0.0
    matched_labels: list[str] = []

    for field_name in REQUIRED_FIELDS:
        weight = _validate_weight(parsed_icp_matrix[field_name], field_name)
        total_weight += weight

        signal_value = parsed_signal_data[field_name]
        if _is_signal_value_present(signal_value):
            weighted_sum += weight
            matched_labels.append(FIELD_LABELS[field_name])

    opportunity_score = 0.0
    if total_weight > 0:
        opportunity_score = round((weighted_sum / total_weight) * 100.0, 2)

    reason = "No ICP alignment"
    if matched_labels:
        reason = " + ".join(matched_labels)

    industry_value = parsed_signal_data["industry"]
    icp_value = industry_value.strip() if isinstance(industry_value, str) else str(industry_value)

    return {
        "icp": icp_value,
        "opportunity_score": opportunity_score,
        "reason": reason,
    }


__all__ = ["score_opportunity"]