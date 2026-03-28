from __future__ import annotations

import json
import re

from fleetgraph.watchlist.contact_classifier import classify_email, classify_phone


_EMAIL_PATTERN = re.compile(r"\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}\b", re.IGNORECASE)
_PHONE_PATTERN = re.compile(r"(?:\+?1[-.\s]?)?(?:\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4})")
_STREET_ADDRESS_PATTERN = re.compile(
    r"\b\d{1,5}\s+[A-Za-z0-9.\- ]+\s(?:Street|St|Avenue|Ave|Road|Rd|Boulevard|Blvd|Drive|Dr|Lane|Ln|Way|Court|Ct|Suite|Ste)\b",
    re.IGNORECASE,
)
_CITY_STATE_PATTERN = re.compile(r"\b[A-Z][a-zA-Z]+(?:\s+[A-Z][a-zA-Z]+)*,\s*[A-Z]{2}(?:\s+\d{5})?\b")
_CONTACT_PAGE_TERMS = ("contact", "about", "team", "leadership", "executive")
_LEADERSHIP_PAGE_TERMS = ("leadership", "team", "executive")
_LEADING_ADDRESS_SUFFIXES = {"street", "st", "avenue", "ave", "road", "rd", "boulevard", "blvd", "drive", "dr", "lane", "ln", "way", "court", "ct", "suite", "ste"}


def _collapse_whitespace(value: str) -> str:
    return " ".join(value.split())


def _dedupe_dict_items(items: list[dict[str, object]]) -> list[dict[str, object]]:
    unique_items: list[dict[str, object]] = []
    seen_keys: set[str] = set()
    for item in items:
        item_key = json.dumps(item, sort_keys=True, separators=(",", ":"))
        if item_key not in seen_keys:
            seen_keys.add(item_key)
            unique_items.append(item)
    return unique_items


def _dedupe_strings(values: list[str]) -> list[str]:
    unique_values: list[str] = []
    for value in values:
        if value not in unique_values:
            unique_values.append(value)
    return unique_values


def _normalize_city_state_line(value: str) -> str:
    normalized_value = _collapse_whitespace(value)
    words = normalized_value.split()
    while len(words) > 1 and words[0].lower() in _LEADING_ADDRESS_SUFFIXES:
        words = words[1:]
    return " ".join(words)


def extract_classified_emails(
    search_results: object,
    *,
    website_domain: str | None,
) -> dict[str, list[dict[str, object]]]:
    direct_emails: list[dict[str, object]] = []
    general_emails: list[dict[str, object]] = []
    if not isinstance(search_results, list):
        return {
            "published_emails": [],
            "general_emails": [],
        }
    for result_item in search_results:
        if not isinstance(result_item, dict):
            continue
        combined_text = " ".join(
            str(result_item.get(field_name, ""))
            for field_name in ("title", "snippet", "url")
        )
        emails = sorted({match.group(0).lower() for match in _EMAIL_PATTERN.finditer(combined_text)})
        for email in emails:
            classification = classify_email(email, website_domain=website_domain)
            entry = {
                "email": email,
                "source_url": str(result_item.get("url", "")),
                "confidence": str(classification["confidence"]),
                "type": str(classification["type"]),
                "is_direct": bool(classification["is_direct"]),
            }
            if entry["type"] == "general_email":
                general_emails.append(entry)
            else:
                direct_emails.append(entry)
    return {
        "published_emails": _dedupe_dict_items(direct_emails),
        "general_emails": _dedupe_dict_items(general_emails),
    }


def extract_direct_phones(search_results: object) -> list[dict[str, object]]:
    phones: list[dict[str, object]] = []
    if not isinstance(search_results, list):
        return []
    for result_item in search_results:
        if not isinstance(result_item, dict):
            continue
        combined_text = " ".join(
            str(result_item.get(field_name, ""))
            for field_name in ("title", "snippet")
        )
        for match in _PHONE_PATTERN.finditer(combined_text):
            normalized_phone = _collapse_whitespace(match.group(0).strip())
            classification = classify_phone(normalized_phone)
            phones.append(
                {
                    "phone": normalized_phone,
                    "source_url": str(result_item.get("url", "")),
                    "confidence": str(classification["confidence"]),
                    "type": str(classification["type"]),
                    "is_direct": bool(classification["is_direct"]),
                }
            )
    return _dedupe_dict_items(phones)


def extract_contact_pages(search_results: object) -> list[str]:
    pages: list[str] = []
    if not isinstance(search_results, list):
        return []
    for result_item in search_results:
        if not isinstance(result_item, dict):
            continue
        url = str(result_item.get("url", "")).strip()
        normalized_url = url.lower()
        if url != "" and any(term in normalized_url for term in _CONTACT_PAGE_TERMS):
            pages.append(url)
    return _dedupe_strings(pages)


def extract_leadership_pages(search_results: object) -> list[str]:
    pages: list[str] = []
    if not isinstance(search_results, list):
        return []
    for result_item in search_results:
        if not isinstance(result_item, dict):
            continue
        url = str(result_item.get("url", "")).strip()
        normalized_url = url.lower()
        if url != "" and any(term in normalized_url for term in _LEADERSHIP_PAGE_TERMS):
            pages.append(url)
    return _dedupe_strings(pages)


def extract_address_lines(search_results: object) -> list[str]:
    address_lines: list[str] = []
    if not isinstance(search_results, list):
        return []
    for result_item in search_results:
        if not isinstance(result_item, dict):
            continue
        combined_text = " ".join(
            str(result_item.get(field_name, ""))
            for field_name in ("title", "snippet")
        )
        for match in _STREET_ADDRESS_PATTERN.finditer(combined_text):
            normalized_line = _collapse_whitespace(match.group(0).strip(" ,.;:-"))
            if normalized_line != "":
                address_lines.append(normalized_line)
        for match in _CITY_STATE_PATTERN.finditer(combined_text):
            normalized_line = _normalize_city_state_line(match.group(0).strip(" ,.;:-"))
            if normalized_line != "":
                address_lines.append(normalized_line)
    return _dedupe_strings(address_lines)
