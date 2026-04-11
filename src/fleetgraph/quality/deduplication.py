from __future__ import annotations

import re
from copy import deepcopy
from typing import Any

__all__ = [
    "deduplicate_contacts",
]


def _normalize_name(value: object) -> str:
    if not isinstance(value, str):
        return ""
    normalized_value = re.sub(r"[^a-z0-9]+", " ", value.strip().lower())
    return " ".join(normalized_value.split())


def _normalize_title(value: object) -> str:
    if not isinstance(value, str):
        return ""
    return " ".join(value.strip().lower().split())


def _contact_sort_key(contact: dict[str, Any]) -> tuple[float, float, str, str, str]:
    return (
        -float(contact.get("quality_score", 0.0)),
        -float(contact.get("role_score", 0.0)),
        str(contact.get("email", "")).lower(),
        _normalize_name(contact.get("name")),
        _normalize_title(contact.get("title")),
    )


def _similarity_key(contact: dict[str, Any]) -> tuple[str, str]:
    name_tokens = _normalize_name(contact.get("name")).split(" ")
    primary_name = " ".join(name_tokens[:2])
    return (primary_name, _normalize_title(contact.get("title")))


def deduplicate_contacts(contacts: list[dict[str, Any]]) -> list[dict[str, Any]]:
    deduplicated_by_email: dict[str, dict[str, Any]] = {}
    for contact in sorted((deepcopy(contact) for contact in contacts), key=_contact_sort_key):
        email = str(contact.get("email", "")).lower()
        if email == "":
            continue
        if email not in deduplicated_by_email:
            deduplicated_by_email[email] = contact

    best_by_similarity: dict[tuple[str, str], dict[str, Any]] = {}
    for contact in sorted(deduplicated_by_email.values(), key=_contact_sort_key):
        similarity_key = _similarity_key(contact)
        if similarity_key not in best_by_similarity:
            best_by_similarity[similarity_key] = contact

    deduplicated_contacts = list(best_by_similarity.values())
    deduplicated_contacts.sort(key=_contact_sort_key)
    return deepcopy(deduplicated_contacts)
