
from __future__ import annotations

import re
from copy import deepcopy
from typing import Any, Optional


__all__ = ["extract_signal", "get_signal_rejection_reason"]


_DATE_PATTERN = re.compile(r"\b(20\d{2}-\d{2}-\d{2})\b")
_NAMED_MONTH_DATE_PATTERN = re.compile(
    r"\b("
    r"January|February|March|April|May|June|July|August|September|October|November|December"
    r") (\d{1,2}), (20\d{2})\b"
)
_MONTH_NUMBER_BY_NAME = {
    "January": "01",
    "February": "02",
    "March": "03",
    "April": "04",
    "May": "05",
    "June": "06",
    "July": "07",
    "August": "08",
    "September": "09",
    "October": "10",
    "November": "11",
    "December": "12",
}

_COMPANY_STOP_WORDS = (
    " on ",
    " after ",
    " over ",
    " amid ",
    " due to ",
    " because ",
    " in ",
    " filed",
    " began",
    " opened",
    " reported",
    " disclosed",
    " complaint",
)

_CAPITALIZED_COMPANY_PATTERN = re.compile(
    r"\b([A-Z][A-Za-z0-9&]*(?: [A-Z][A-Za-z0-9&]*){1,5})\b"
)


def _build_raw_text(result_item: dict[str, str]) -> str:
    title = result_item.get("title", "")
    snippet = result_item.get("snippet", "")
    return f"{title} {snippet}".strip()


def _extract_date(text: str) -> Optional[str]:
    iso_match = _DATE_PATTERN.search(text)
    if iso_match:
        return iso_match.group(1)

    named_month_match = _NAMED_MONTH_DATE_PATTERN.search(text)
    if named_month_match:
        month_name = named_month_match.group(1)
        day = named_month_match.group(2).zfill(2)
        year = named_month_match.group(3)
        return f"{year}-{_MONTH_NUMBER_BY_NAME[month_name]}-{day}"

    return None


def _clean_company_candidate(value: str) -> str:
    candidate = " ".join(value.strip().split())
    candidate_lower = candidate.lower()

    cut_positions = [
        candidate_lower.find(stop_word)
        for stop_word in _COMPANY_STOP_WORDS
        if candidate_lower.find(stop_word) != -1
    ]
    if cut_positions:
        candidate = candidate[: min(cut_positions)].strip()

    return candidate.rstrip(" ,.;:")


def _extract_company(text: str) -> str:
    lowered_text = text.lower()

    phrase_patterns = (
        r"against ([A-Z][A-Za-z0-9&]*(?: [A-Z][A-Za-z0-9&]*){1,5})",
        r"audit of ([A-Z][A-Za-z0-9&]*(?: [A-Z][A-Za-z0-9&]*){1,5})",
        r"review of ([A-Z][A-Za-z0-9&]*(?: [A-Z][A-Za-z0-9&]*){1,5})",
        r"of ([A-Z][A-Za-z0-9&]*(?: [A-Z][A-Za-z0-9&]*){1,5}) opened",
    )

    for pattern in phrase_patterns:
        match = re.search(pattern, text)
        if match:
            company = _clean_company_candidate(match.group(1))
            if company:
                return company

    for match in _CAPITALIZED_COMPANY_PATTERN.finditer(text):
        candidate = _clean_company_candidate(match.group(1))
        if candidate.lower() in {
            "lawsuit filed",
            "filed on",
            "project delay",
            "compliance review",
            "mechanics lien",
        }:
            continue
        if len(candidate.split()) >= 2:
            return candidate

    if "lawsuit filed" in lowered_text and "against " not in lowered_text:
        return "unknown"

    return "unknown"


def extract_signal(
    result_item: dict[str, str],
    *,
    signal_type: str,
) -> dict[str, Any]:
    item = deepcopy(result_item)

    raw_text = _build_raw_text(item)
    company = _extract_company(raw_text)
    date_detected = _extract_date(raw_text) or ""

    return {
        "company": company,
        "signal_type": signal_type,
        "event_summary": item.get("title", ""),
        "source": item.get("url", ""),
        "date_detected": date_detected,
        "confidence_score": None,
        "priority": None,
        "raw_text": raw_text,
    }


def get_signal_rejection_reason(signal: dict[str, Any]) -> Optional[str]:
    company = str(signal.get("company", "")).lower()

    generic_terms = [
        "company",
        "real estate company",
        "construction company",
    ]
    if company in generic_terms:
        return "generic_company"

    if company == "unknown":
        return "generic_company"

    event_text = str(signal.get("event_summary", "")).lower()

    required_terms = [
        "lawsuit",
        "sued",
        "lien",
        "audit",
        "investigation",
        "filed",
    ]

    if not any(term in event_text for term in required_terms):
        return "missing_event_term"

    return None