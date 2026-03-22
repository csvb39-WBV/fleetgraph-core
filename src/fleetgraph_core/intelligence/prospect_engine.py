"""Convert opportunity outputs into sales-ready prospect records."""

from __future__ import annotations

from collections.abc import Mapping


_PRIORITY_ORDER = {
    "HIGH": 0,
    "MEDIUM": 1,
    "LOW": 2,
}


def _validate_non_empty_string(value: object, field_name: str) -> str:
    if not isinstance(value, str):
        raise ValueError(f"{field_name} must be a non-empty string")

    normalized = value.strip()
    if not normalized:
        raise ValueError(f"{field_name} must be a non-empty string")

    return normalized


def _validate_score(value: object, field_name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"{field_name} must be numeric")
    return float(value)


def _priority_for_score(score: float) -> str:
    if score >= 80.0:
        return "HIGH"
    if score >= 50.0:
        return "MEDIUM"
    return "LOW"


def _validate_opportunity(opportunity: object, index: int) -> Mapping[str, object]:
    if not isinstance(opportunity, Mapping):
        raise ValueError(f"opportunities[{index}] must be a mapping")

    required_fields = ("icp", "opportunity_score", "reason")
    for field_name in required_fields:
        if field_name not in opportunity:
            raise ValueError(
                f"opportunities[{index}] missing required field: {field_name}"
            )

    _validate_non_empty_string(opportunity.get("icp"), f"opportunities[{index}].icp")
    _validate_score(opportunity.get("opportunity_score"), f"opportunities[{index}].opportunity_score")
    _validate_non_empty_string(
        opportunity.get("reason"),
        f"opportunities[{index}].reason",
    )

    return opportunity


def build_prospects(payload: object) -> dict[str, object]:
    """Build deterministic prospect records from opportunity inputs."""
    if not isinstance(payload, Mapping):
        raise ValueError("input must be a mapping")

    company_id = _validate_non_empty_string(payload.get("company_id"), "company_id")

    opportunities = payload.get("opportunities")
    if not isinstance(opportunities, list):
        raise ValueError("opportunities must be a list")

    prospects: list[dict[str, object]] = []
    for index, opportunity in enumerate(opportunities):
        parsed = _validate_opportunity(opportunity, index)

        icp = _validate_non_empty_string(parsed["icp"], f"opportunities[{index}].icp")
        score = _validate_score(
            parsed["opportunity_score"],
            f"opportunities[{index}].opportunity_score",
        )
        reason = _validate_non_empty_string(
            parsed["reason"],
            f"opportunities[{index}].reason",
        )

        prospects.append(
            {
                "company_id": company_id,
                "icp": icp,
                "priority": _priority_for_score(score),
                "opportunity_score": score,
                "reason": reason,
            }
        )

    prospects.sort(
        key=lambda item: (
            _PRIORITY_ORDER[str(item["priority"])],
            -float(item["opportunity_score"]),
            str(item["icp"]),
        )
    )

    return {
        "company_id": company_id,
        "prospects": prospects,
    }


__all__ = ["build_prospects"]