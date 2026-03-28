from __future__ import annotations

import json
import re
from urllib import parse


_EVENT_TERMS = (
    "sued",
    "lawsuit",
    "filed",
    "investigation",
    "subpoena",
    "default",
    "delay",
    "dispute",
    "terminated",
    "halted",
    "ordered",
    "claim",
    "audit",
)
_EMAIL_PATTERN = re.compile(r"\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}\b", re.IGNORECASE)


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


def _normalize_email_pattern(website_domain: str | None) -> str | None:
    if website_domain is None or website_domain == "":
        return None
    return f"first.last@{website_domain}"


def _extract_public_emails(search_results: list[dict[str, str]]) -> list[dict[str, object]]:
    emails: list[dict[str, object]] = []
    for result_item in search_results:
        combined_text = " ".join((result_item["title"], result_item["snippet"], result_item["url"]))
        for email in sorted({match.group(0).lower() for match in _EMAIL_PATTERN.finditer(combined_text)}):
            emails.append(
                {
                    "email": email,
                    "source_url": result_item["url"],
                    "confidence": "high",
                }
            )
    return _dedupe_dict_items(emails)


def _extract_recent_signals(search_results: list[dict[str, str]]) -> list[dict[str, object]]:
    signals: list[dict[str, object]] = []
    for result_item in search_results:
        combined_text = f"{result_item['title']} {result_item['snippet']}".lower()
        if not any(term in combined_text for term in _EVENT_TERMS):
            continue
        signals.append(
            {
                "event_summary": result_item["title"],
                "source_url": result_item["url"],
                "source_provider": result_item["source_provider"],
                "confidence": "medium",
            }
        )
    return _dedupe_dict_items(signals)


def _extract_recent_projects(search_results: list[dict[str, str]]) -> list[dict[str, object]]:
    projects: list[dict[str, object]] = []
    for result_item in search_results:
        combined_text = f"{result_item['title']} {result_item['snippet']}".lower()
        if "project" not in combined_text:
            continue
        projects.append(
            {
                "project_summary": result_item["title"],
                "source_url": result_item["url"],
                "confidence": "low",
            }
        )
    return _dedupe_dict_items(projects)


def _seed_people(watchlist_entity: dict[str, object]) -> list[dict[str, object]]:
    seed_people: list[dict[str, object]] = []
    source_url = watchlist_entity["sources"][0] if len(watchlist_entity["sources"]) > 0 else watchlist_entity["website"] or ""
    for field_name, title in (
        ("ceo_name", "CEO"),
        ("cfo_name", "CFO"),
        ("chief_risk_officer_name", "Chief Risk Officer"),
    ):
        person_name = watchlist_entity[field_name]
        if isinstance(person_name, str) and person_name.strip() != "":
            seed_people.append(
                {
                    "name": person_name,
                    "title": title,
                    "source_url": source_url,
                    "confidence": "high",
                }
            )
    return seed_people


def _confidence_level(key_people: list[dict[str, object]], published_emails: list[dict[str, object]], recent_signals: list[dict[str, object]]) -> str:
    if len(published_emails) > 0 or len(recent_signals) >= 3:
        return "high"
    if len(key_people) > 0 or len(recent_signals) > 0:
        return "medium"
    return "low"


def build_enrichment_record(
    watchlist_entity: dict[str, object],
    search_results: list[dict[str, str]],
    *,
    run_date: str,
) -> dict[str, object]:
    website = watchlist_entity["website"]
    website_domain = watchlist_entity["website_domain"]
    key_people = _seed_people(watchlist_entity)
    published_emails = _extract_public_emails(search_results)
    recent_signals = _extract_recent_signals(search_results)
    recent_projects = _extract_recent_projects(search_results)
    source_links = _dedupe_strings(
        list(watchlist_entity["sources"]) + [result_item["url"] for result_item in search_results]
    )
    email_pattern_guess = None
    if len(published_emails) == 0:
        email_pattern_guess = _normalize_email_pattern(website_domain)

    return {
        "company_name": watchlist_entity["company_name"],
        "website": website,
        "main_phone": watchlist_entity["main_phone"],
        "hq_city": watchlist_entity["hq_city"],
        "hq_state": watchlist_entity["hq_state"],
        "priority_tier": watchlist_entity["priority_tier"],
        "category": watchlist_entity["category"],
        "segment": watchlist_entity["segment"],
        "key_people": key_people,
        "published_emails": published_emails,
        "email_pattern_guess": email_pattern_guess,
        "recent_signals": recent_signals,
        "recent_projects": recent_projects,
        "source_links": source_links,
        "last_enriched_at": run_date,
        "confidence_level": _confidence_level(key_people, published_emails, recent_signals),
    }
