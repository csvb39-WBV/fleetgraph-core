from __future__ import annotations

import hashlib
import json
import re
from copy import deepcopy
from datetime import date, datetime
from typing import Any

__all__ = [
    "assemble_prospects",
    "normalize_enrichment_contacts",
]

_REQUIRED_LEAD_FIELDS = (
    "company_id",
    "company_name",
    "selected_bucket",
    "signal_type",
    "signal_detail",
    "event_date",
    "source_url",
)
_DEFAULT_MAX_CONTACTS_PER_COMPANY = 3
_EMAIL_PATTERN = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
_TITLE_PRIORITY_RULES = (
    (("owner", "president", "founder", "ceo", "chief executive officer"), 1),
    (("cfo", "chief financial officer", "controller", "finance"), 2),
    (("project executive", "operations", "coo", "chief operating officer"), 3),
    (("managing director", "principal", "partner", "vice president", "vp ", "director", "general manager", "business development"), 4),
)


def _normalize_non_empty_string(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    normalized_value = value.strip()
    if normalized_value == "":
        return None
    return normalized_value


def _normalize_optional_string(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    normalized_value = value.strip()
    if normalized_value == "":
        return None
    return normalized_value


def _coerce_event_date(value: object, *, field_name: str) -> date | datetime:
    if isinstance(value, datetime):
        return value
    if isinstance(value, date):
        return value
    raise ValueError(f"{field_name} must be a date or datetime")


def _normalize_email(value: object) -> str | None:
    normalized_email = _normalize_non_empty_string(value)
    if normalized_email is None:
        return None
    lowered_email = normalized_email.lower()
    if _EMAIL_PATTERN.match(lowered_email) is None:
        return None
    return lowered_email


def _normalize_title_for_rank(title: str) -> str:
    return " ".join(title.strip().lower().split())


def _contact_priority_rank(title: str) -> int:
    normalized_title = _normalize_title_for_rank(title)
    for patterns, rank in _TITLE_PRIORITY_RULES:
        if any(pattern in normalized_title for pattern in patterns):
            return rank
    return 99


def _contact_sort_key(contact: dict[str, Any]) -> tuple[int, str, str, str]:
    return (
        int(contact["priority_rank"]),
        _normalize_title_for_rank(str(contact["title"])),
        str(contact["name"]).strip().lower(),
        str(contact["email"]).strip().lower(),
    )


def _resolve_company_id(record: dict[str, Any]) -> str | None:
    return _normalize_non_empty_string(record.get("company_id"))


def _resolve_contact_name(record: dict[str, Any]) -> str | None:
    for field_name in ("name", "full_name", "contact_name"):
        normalized_name = _normalize_non_empty_string(record.get(field_name))
        if normalized_name is not None:
            return normalized_name
    return None


def _resolve_contact_title(record: dict[str, Any]) -> str | None:
    for field_name in ("title", "job_title", "contact_title", "role"):
        normalized_title = _normalize_non_empty_string(record.get(field_name))
        if normalized_title is not None:
            return normalized_title
    return None


def _resolve_contact_email(record: dict[str, Any]) -> str | None:
    for field_name in ("email", "contact_email", "email_address"):
        normalized_email = _normalize_email(record.get(field_name))
        if normalized_email is not None:
            return normalized_email
    return None


def normalize_enrichment_contacts(
    enrichment_records: list[object],
) -> list[dict[str, Any]]:
    if not isinstance(enrichment_records, list):
        raise ValueError("enrichment_records must be a list")

    normalized_contacts: list[dict[str, Any]] = []
    for index, record in enumerate(enrichment_records):
        if not isinstance(record, dict):
            raise ValueError(f"enrichment_records[{index}] must be a dict")

        company_id = _resolve_company_id(record)
        name = _resolve_contact_name(record)
        title = _resolve_contact_title(record)
        email = _resolve_contact_email(record)

        if company_id is None:
            continue
        if name is None:
            continue
        if title is None:
            continue
        if email is None:
            continue

        normalized_contacts.append(
            {
                "company_id": company_id,
                "name": name,
                "title": title,
                "email": email,
                "priority_rank": _contact_priority_rank(title),
            }
        )

    normalized_contacts.sort(key=_contact_sort_key)
    return normalized_contacts


def _validate_selected_lead(lead: object, index: int) -> dict[str, Any]:
    if not isinstance(lead, dict):
        raise ValueError(f"selected_leads[{index}] must be a dict")

    normalized_lead: dict[str, Any] = {}
    for field_name in _REQUIRED_LEAD_FIELDS:
        if field_name not in lead:
            raise ValueError(f"selected_leads[{index}] missing required field: {field_name}")

    for field_name in (
        "company_id",
        "company_name",
        "selected_bucket",
        "signal_type",
        "signal_detail",
    ):
        normalized_value = _normalize_non_empty_string(lead.get(field_name))
        if normalized_value is None:
            raise ValueError(f"selected_leads[{index}].{field_name} must be a non-empty string")
        normalized_lead[field_name] = normalized_value

    normalized_lead["event_date"] = _coerce_event_date(
        lead.get("event_date"),
        field_name=f"selected_leads[{index}].event_date",
    )
    source_url = lead.get("source_url")
    if source_url is not None and not isinstance(source_url, str):
        raise ValueError(f"selected_leads[{index}].source_url must be a string or None")
    normalized_lead["source_url"] = _normalize_optional_string(source_url)
    return normalized_lead


def _prospect_id(lead: dict[str, Any], contacts: list[dict[str, Any]]) -> str:
    digest_source = json.dumps(
        {
            "company_id": lead["company_id"],
            "selected_bucket": lead["selected_bucket"],
            "signal_type": lead["signal_type"],
            "signal_detail": lead["signal_detail"],
            "event_date": lead["event_date"].isoformat(),
            "contacts": [
                {
                    "name": contact["name"],
                    "title": contact["title"],
                    "email": contact["email"],
                    "priority_rank": contact["priority_rank"],
                }
                for contact in contacts
            ],
        },
        ensure_ascii=True,
        separators=(",", ":"),
    )
    digest = hashlib.sha256(digest_source.encode("utf-8")).hexdigest()[:16]
    return f"prospect:{lead['company_id']}:{digest}"


def _deduplicate_contacts(contacts: list[dict[str, Any]]) -> list[dict[str, Any]]:
    deduplicated_by_email: dict[str, dict[str, Any]] = {}
    for contact in sorted(contacts, key=_contact_sort_key):
        email = str(contact["email"])
        if email not in deduplicated_by_email:
            deduplicated_by_email[email] = contact
    deduplicated_contacts = list(deduplicated_by_email.values())
    deduplicated_contacts.sort(key=_contact_sort_key)
    return deduplicated_contacts


def assemble_prospects(
    selected_leads: list[object],
    enrichment_records: list[object],
    *,
    max_contacts_per_company: int = _DEFAULT_MAX_CONTACTS_PER_COMPANY,
) -> list[dict[str, Any]]:
    if not isinstance(selected_leads, list):
        raise ValueError("selected_leads must be a list")
    if max_contacts_per_company <= 0:
        raise ValueError("max_contacts_per_company must be greater than 0")

    validated_leads = [
        _validate_selected_lead(lead, index)
        for index, lead in enumerate(selected_leads)
    ]
    normalized_contacts = normalize_enrichment_contacts(enrichment_records)

    contacts_by_company_id: dict[str, list[dict[str, Any]]] = {}
    for contact in normalized_contacts:
        contacts_by_company_id.setdefault(str(contact["company_id"]), []).append(contact)

    prospect_records: list[dict[str, Any]] = []
    for lead in validated_leads:
        company_contacts = _deduplicate_contacts(
            contacts_by_company_id.get(str(lead["company_id"]), [])
        )
        selected_contacts = company_contacts[:max_contacts_per_company]
        if selected_contacts == []:
            continue

        preserved_lead = deepcopy(lead)
        prospect_records.append(
            {
                "prospect_id": _prospect_id(preserved_lead, selected_contacts),
                "company_id": preserved_lead["company_id"],
                "company_name": preserved_lead["company_name"],
                "selected_bucket": preserved_lead["selected_bucket"],
                "signal_type": preserved_lead["signal_type"],
                "signal_detail": preserved_lead["signal_detail"],
                "event_date": preserved_lead["event_date"],
                "source_url": preserved_lead["source_url"],
                "contacts": [
                    {
                        "name": str(contact["name"]),
                        "title": str(contact["title"]),
                        "email": str(contact["email"]),
                        "priority_rank": int(contact["priority_rank"]),
                    }
                    for contact in selected_contacts
                ],
            }
        )

    return prospect_records
