from __future__ import annotations

import json
import re

from fleetgraph.watchlist.contact_extractor import (
    extract_address_lines,
    extract_classified_emails,
    extract_contact_pages,
    extract_direct_phones,
    extract_leadership_pages,
)


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


def _score_reachability(
    *,
    published_emails: list[dict[str, object]],
    general_emails: list[dict[str, object]],
    direct_phones: list[dict[str, object]],
    contact_pages: list[str],
    leadership_pages: list[str],
) -> int:
    score = 0
    if len(published_emails) > 0:
        score += 40
    elif len(general_emails) > 0:
        score += 20
    if len(direct_phones) > 0:
        score += 15
    if len(contact_pages) > 0:
        score += 10
    if len(leadership_pages) > 0:
        score += 5
    return max(0, min(score, 100))


def _contact_confidence_level(
    *,
    published_emails: list[dict[str, object]],
    general_emails: list[dict[str, object]],
    direct_phones: list[dict[str, object]],
) -> str:
    if len(published_emails) > 0:
        return "high"
    if len(general_emails) > 0 or len(direct_phones) > 0:
        return "medium"
    return "low"


def _contact_sources(
    *,
    published_emails: list[dict[str, object]],
    general_emails: list[dict[str, object]],
    direct_phones: list[dict[str, object]],
    contact_pages: list[str],
    leadership_pages: list[str],
) -> list[str]:
    sources = [item["source_url"] for item in published_emails]
    sources.extend(item["source_url"] for item in general_emails)
    sources.extend(item["source_url"] for item in direct_phones)
    sources.extend(contact_pages)
    sources.extend(leadership_pages)
    return _dedupe_strings([str(source) for source in sources if str(source).strip() != ""])


def build_enrichment_record(
    watchlist_entity: dict[str, object],
    search_results: list[dict[str, str]],
    *,
    run_date: str,
) -> dict[str, object]:
    website = watchlist_entity["website"]
    website_domain = watchlist_entity["website_domain"]
    key_people = _seed_people(watchlist_entity)
    extracted_emails = extract_classified_emails(search_results, website_domain=website_domain)
    published_emails = extracted_emails["published_emails"]
    general_emails = extracted_emails["general_emails"]
    direct_phones = extract_direct_phones(search_results)
    contact_pages = extract_contact_pages(search_results)
    leadership_pages = extract_leadership_pages(search_results)
    address_lines = extract_address_lines(search_results)
    recent_signals = _extract_recent_signals(search_results)
    recent_projects = _extract_recent_projects(search_results)
    contact_sources = _contact_sources(
        published_emails=published_emails,
        general_emails=general_emails,
        direct_phones=direct_phones,
        contact_pages=contact_pages,
        leadership_pages=leadership_pages,
    )
    source_links = _dedupe_strings(
        list(watchlist_entity["sources"]) + [result_item["url"] for result_item in search_results]
    )
    email_pattern_guess = None
    if len(published_emails) == 0:
        email_pattern_guess = _normalize_email_pattern(website_domain)
    reachability_score = _score_reachability(
        published_emails=published_emails,
        general_emails=general_emails,
        direct_phones=direct_phones,
        contact_pages=contact_pages,
        leadership_pages=leadership_pages,
    )

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
        "direct_phones": direct_phones,
        "general_emails": general_emails,
        "published_emails": published_emails,
        "contact_pages": contact_pages,
        "leadership_pages": leadership_pages,
        "address_lines": address_lines,
        "contact_sources": contact_sources,
        "email_pattern_guess": email_pattern_guess,
        "contact_confidence_level": _contact_confidence_level(
            published_emails=published_emails,
            general_emails=general_emails,
            direct_phones=direct_phones,
        ),
        "reachability_score": reachability_score,
        "recent_signals": recent_signals,
        "recent_projects": recent_projects,
        "source_links": source_links,
        "last_enriched_at": run_date,
        "confidence_level": _confidence_level(key_people, published_emails, recent_signals),
    }
