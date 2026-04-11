from __future__ import annotations

from typing import Any

from fleetgraph.quality.email_validation import validate_contact_email
from fleetgraph.quality.role_confidence import score_role_confidence

__all__ = [
    "score_contact_quality",
]


def _normalize_text(value: object) -> str:
    if not isinstance(value, str):
        return ""
    return " ".join(value.strip().lower().split())


def _company_domain_consistency(company_id: str, email: str) -> float:
    if company_id == "" or email == "":
        return 0.0
    company_tokens = [token for token in company_id.lower().replace("_", "-").split("-") if token not in {"company", "services", "group", "inc", "llc", "co"}]
    email_domain = email.split("@", 1)[1]
    if company_tokens == []:
        return 0.5
    if any(token in email_domain for token in company_tokens):
        return 1.0
    return 0.35


def score_contact_quality(contact: dict[str, Any]) -> dict[str, Any]:
    email_result = validate_contact_email(contact.get("email"))
    role_result = score_role_confidence(contact.get("title"))

    completeness_score = 0.0
    if _normalize_text(contact.get("name")) != "":
        completeness_score += 0.34
    if _normalize_text(contact.get("title")) != "":
        completeness_score += 0.33
    if email_result["is_valid"] is True:
        completeness_score += 0.33

    email_score = 1.0 if email_result["is_valid"] is True else 0.0
    domain_consistency_score = _company_domain_consistency(
        _normalize_text(contact.get("company_id")),
        str(email_result["email"]),
    )
    quality_score = round(
        (email_score * 0.4)
        + (float(role_result["role_score"]) * 0.35)
        + (completeness_score * 0.15)
        + (domain_consistency_score * 0.10),
        4,
    )
    if quality_score >= 0.8:
        confidence_level = "HIGH"
    elif quality_score >= 0.55:
        confidence_level = "MEDIUM"
    else:
        confidence_level = "LOW"
    return {
        "quality_score": quality_score,
        "confidence_level": confidence_level,
        "role_confidence": str(role_result["role_confidence"]),
        "role_score": float(role_result["role_score"]),
        "email_validation_reason": str(email_result["reason"]),
        "email": str(email_result["email"]),
        "is_valid_email": bool(email_result["is_valid"]),
    }
