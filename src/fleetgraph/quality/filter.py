from __future__ import annotations

from copy import deepcopy
from typing import Any

from fleetgraph.prospects.prospect_assembly import (
    assemble_prospects,
    normalize_enrichment_contacts,
)
from fleetgraph.quality.contact_scoring import score_contact_quality
from fleetgraph.quality.deduplication import deduplicate_contacts

__all__ = [
    "build_high_quality_prospects",
    "filter_enrichment_contacts",
    "select_best_contacts",
]

_ROLE_CONFIDENCE_ORDER = {
    "HIGH": 0,
    "MEDIUM": 1,
    "LOW": 2,
}


def _normalize_non_empty_string(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    normalized_value = value.strip()
    if normalized_value == "":
        return None
    return normalized_value


def _normalized_contact(contact: dict[str, Any]) -> dict[str, Any]:
    scoring = score_contact_quality(contact)
    return {
        "company_id": str(contact.get("company_id", "")).strip(),
        "name": str(contact.get("name", "")).strip(),
        "title": str(contact.get("title", "")).strip(),
        "email": str(scoring["email"]),
        "priority_rank": int(contact.get("priority_rank", 99)),
        "quality_score": float(scoring["quality_score"]),
        "confidence_level": str(scoring["confidence_level"]),
        "role_confidence": str(scoring["role_confidence"]),
        "role_score": float(scoring["role_score"]),
        "email_validation_reason": str(scoring["email_validation_reason"]),
        "is_valid_email": bool(scoring["is_valid_email"]),
    }


def _contact_sort_key(contact: dict[str, Any]) -> tuple[float, float, int, str, str]:
    return (
        -float(contact["quality_score"]),
        -float(contact["role_score"]),
        int(contact["priority_rank"]),
        str(contact["email"]).lower(),
        str(contact["name"]).lower(),
    )


def filter_enrichment_contacts(
    contacts: list[dict[str, Any]],
    *,
    company_id: str,
    min_quality_score: float = 0.55,
    minimum_role_confidence: str = "MEDIUM",
) -> dict[str, list[dict[str, Any]]]:
    if minimum_role_confidence not in _ROLE_CONFIDENCE_ORDER:
        raise ValueError("minimum_role_confidence is invalid")

    normalized_company_id = _normalize_non_empty_string(company_id)
    if normalized_company_id is None:
        raise ValueError("company_id must be a non-empty string")

    filtered_contacts: list[dict[str, Any]] = []
    rejected_contacts: list[dict[str, Any]] = []
    minimum_role_rank = _ROLE_CONFIDENCE_ORDER[minimum_role_confidence]

    for raw_contact in contacts:
        normalized_contact = _normalized_contact(raw_contact)
        if normalized_contact["company_id"] != normalized_company_id:
            rejected_contacts.append({**deepcopy(normalized_contact), "rejection_reason": "inconsistent_company_mapping"})
            continue
        if normalized_contact["is_valid_email"] is not True:
            rejected_contacts.append({**deepcopy(normalized_contact), "rejection_reason": str(normalized_contact["email_validation_reason"])})
            continue
        if float(normalized_contact["quality_score"]) < float(min_quality_score):
            rejected_contacts.append({**deepcopy(normalized_contact), "rejection_reason": "low_quality_score"})
            continue
        if _ROLE_CONFIDENCE_ORDER[str(normalized_contact["role_confidence"])] > minimum_role_rank:
            rejected_contacts.append({**deepcopy(normalized_contact), "rejection_reason": "low_role_confidence"})
            continue
        filtered_contacts.append(normalized_contact)

    deduplicated_contacts = deduplicate_contacts(filtered_contacts)
    deduped_emails = {str(contact["email"]).lower() for contact in deduplicated_contacts}
    for contact in filtered_contacts:
        if str(contact["email"]).lower() not in deduped_emails:
            rejected_contacts.append({**deepcopy(contact), "rejection_reason": "duplicate_contact"})
    deduplicated_contacts.sort(key=_contact_sort_key)
    rejected_contacts.sort(key=lambda contact: (str(contact.get("rejection_reason", "")), str(contact.get("email", "")), str(contact.get("name", ""))))
    return {
        "filtered_contacts": deepcopy(deduplicated_contacts),
        "rejected_contacts": deepcopy(rejected_contacts),
    }


def select_best_contacts(
    contacts: list[dict[str, Any]],
    *,
    max_contacts_per_company: int = 3,
) -> list[dict[str, Any]]:
    if max_contacts_per_company <= 0:
        raise ValueError("max_contacts_per_company must be greater than 0")
    sorted_contacts = [deepcopy(contact) for contact in contacts]
    sorted_contacts.sort(key=_contact_sort_key)
    return deepcopy(sorted_contacts[:max_contacts_per_company])


def build_high_quality_prospects(
    selected_leads: list[object],
    enrichment_records: list[object],
    *,
    max_contacts_per_company: int = 3,
    min_quality_score: float = 0.55,
    minimum_role_confidence: str = "MEDIUM",
) -> dict[str, list[dict[str, Any]]]:
    normalized_contacts = normalize_enrichment_contacts(enrichment_records)

    contacts_by_company_id: dict[str, list[dict[str, Any]]] = {}
    for contact in normalized_contacts:
        contacts_by_company_id.setdefault(str(contact["company_id"]), []).append(contact)

    selected_quality_contacts: list[dict[str, Any]] = []
    rejected_contacts: list[dict[str, Any]] = []
    for lead in selected_leads:
        if not isinstance(lead, dict):
            raise ValueError("selected_leads must contain dict records")
        company_id = _normalize_non_empty_string(lead.get("company_id"))
        if company_id is None:
            raise ValueError("selected lead company_id must be a non-empty string")
        quality_result = filter_enrichment_contacts(
            contacts_by_company_id.get(company_id, []),
            company_id=company_id,
            min_quality_score=min_quality_score,
            minimum_role_confidence=minimum_role_confidence,
        )
        best_contacts = select_best_contacts(
            quality_result["filtered_contacts"],
            max_contacts_per_company=max_contacts_per_company,
        )
        selected_quality_contacts.extend(deepcopy(best_contacts))
        rejected_contacts.extend(deepcopy(quality_result["rejected_contacts"]))

    prospect_input_contacts = [
        {
            "company_id": contact["company_id"],
            "name": contact["name"],
            "title": contact["title"],
            "email": contact["email"],
            "priority_rank": contact["priority_rank"],
        }
        for contact in selected_quality_contacts
    ]
    prospects = assemble_prospects(
        selected_leads,
        prospect_input_contacts,
        max_contacts_per_company=max_contacts_per_company,
    )
    return {
        "prospects": deepcopy(prospects),
        "filtered_contacts": deepcopy(selected_quality_contacts),
        "rejected_contacts": deepcopy(rejected_contacts),
    }
