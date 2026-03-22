"""Deterministic outreach message generation from prospect intelligence."""

from __future__ import annotations

from collections.abc import Mapping


_VALID_PRIORITIES = {"HIGH", "MEDIUM", "LOW"}


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


def _validate_prospect(prospect: object, index: int, company_id: str) -> Mapping[str, object]:
    if not isinstance(prospect, Mapping):
        raise ValueError(f"prospects[{index}] must be a mapping")

    required_fields = ("company_id", "icp", "priority", "opportunity_score", "reason")
    for field_name in required_fields:
        if field_name not in prospect:
            raise ValueError(f"prospects[{index}] missing required field: {field_name}")

    prospect_company_id = _validate_non_empty_string(
        prospect.get("company_id"),
        f"prospects[{index}].company_id",
    )
    if prospect_company_id != company_id:
        raise ValueError(
            f"prospects[{index}].company_id must match top-level company_id"
        )

    _validate_non_empty_string(prospect.get("icp"), f"prospects[{index}].icp")

    priority = _validate_non_empty_string(
        prospect.get("priority"),
        f"prospects[{index}].priority",
    ).upper()
    if priority not in _VALID_PRIORITIES:
        raise ValueError(f"prospects[{index}].priority must be one of: HIGH, MEDIUM, LOW")

    _validate_score(
        prospect.get("opportunity_score"),
        f"prospects[{index}].opportunity_score",
    )
    _validate_non_empty_string(prospect.get("reason"), f"prospects[{index}].reason")

    return prospect


def _build_subject(icp: str, priority: str, reason: str) -> str:
    return f"[{priority}] {icp} opportunity: {reason}"


def _build_message(company_id: str, icp: str, priority: str, score: float, reason: str) -> str:
    return (
        f"Company {company_id} shows a {priority} {icp} opportunity "
        f"(score {score:.2f}) driven by: {reason}. "
        f"Recommended next step: align outreach to {icp} priorities around {reason}."
    )


def _build_talk_track(icp: str, priority: str, reason: str) -> str:
    return (
        f"Talk track for {icp} ({priority}): open with the signal evidence ({reason}), "
        f"confirm current priorities, and propose a concrete next meeting tied to {reason}."
    )


def generate_outreach(payload: object) -> dict[str, object]:
    """Generate deterministic outreach artifacts from prospect records."""
    if not isinstance(payload, Mapping):
        raise ValueError("input must be a mapping")

    company_id = _validate_non_empty_string(payload.get("company_id"), "company_id")

    prospects = payload.get("prospects")
    if not isinstance(prospects, list):
        raise ValueError("prospects must be a list")

    outreach_items: list[dict[str, object]] = []
    for index, prospect in enumerate(prospects):
        parsed = _validate_prospect(prospect, index, company_id)

        icp = _validate_non_empty_string(parsed["icp"], f"prospects[{index}].icp")
        priority = _validate_non_empty_string(
            parsed["priority"],
            f"prospects[{index}].priority",
        ).upper()
        score = _validate_score(
            parsed["opportunity_score"],
            f"prospects[{index}].opportunity_score",
        )
        reason = _validate_non_empty_string(parsed["reason"], f"prospects[{index}].reason")

        outreach_items.append(
            {
                "icp": icp,
                "priority": priority,
                "subject": _build_subject(icp, priority, reason),
                "message": _build_message(company_id, icp, priority, score, reason),
                "talk_track": _build_talk_track(icp, priority, reason),
            }
        )

    return {
        "company_id": company_id,
        "outreach": outreach_items,
    }


__all__ = ["generate_outreach"]