from __future__ import annotations

import re


_VALID_SIGNAL_TYPES = {
    "litigation",
    "audit",
    "project_distress",
    "government",
}
_MONTH_NAMES = {
    "january": "01",
    "february": "02",
    "march": "03",
    "april": "04",
    "may": "05",
    "june": "06",
    "july": "07",
    "august": "08",
    "september": "09",
    "october": "10",
    "november": "11",
    "december": "12",
}
_GENERIC_ENTITY_EXCLUSIONS = {
    "against",
    "audit",
    "company",
    "compliance",
    "construction",
    "contract",
    "contractor",
    "contractors",
    "default",
    "debarred",
    "delay",
    "filed",
    "government",
    "investigation",
    "lawsuit",
    "lien",
    "mechanics",
    "named",
    "notice",
    "project",
    "review",
}
_TRAILING_ENTITY_EXCLUSIONS = {
    "audit",
    "complaint",
    "default",
    "dispute",
    "filing",
    "investigation",
    "notice",
    "project",
    "review",
}


def _collapse_whitespace(value: str) -> str:
    return " ".join(value.split())


def _extract_date_detected(text: str) -> str:
    iso_match = re.search(r"\b(20\d{2}-\d{2}-\d{2})\b", text)
    if iso_match is not None:
        return iso_match.group(1)
    month_match = re.search(
        r"\b("
        + "|".join(_MONTH_NAMES.keys())
        + r")\s+(\d{1,2}),\s*(20\d{2})\b",
        text.lower(),
    )
    if month_match is not None:
        month = _MONTH_NAMES[month_match.group(1)]
        day = month_match.group(2).zfill(2)
        year = month_match.group(3)
        return f"{year}-{month}-{day}"
    return "1970-01-01"


def _clean_company_candidate(candidate: str) -> str:
    cleaned_candidate = _collapse_whitespace(candidate.strip(" ,.;:-"))
    words = cleaned_candidate.split()
    while len(words) > 1 and words[-1].lower() in _TRAILING_ENTITY_EXCLUSIONS:
        words = words[:-1]
    if len(words) < 2 or len(words) > 4:
        return ""
    if words[0].lower() in _GENERIC_ENTITY_EXCLUSIONS:
        return ""
    if any(word.lower() in _MONTH_NAMES for word in words):
        return ""
    return " ".join(words)


def _extract_company(text: str) -> str:
    contextual_patterns = (
        r"against\s+([A-Z][A-Za-z0-9&.\-]*(?:\s+[A-Z][A-Za-z0-9&.\-]*){1,3})",
        r"filed against\s+([A-Z][A-Za-z0-9&.\-]*(?:\s+[A-Z][A-Za-z0-9&.\-]*){1,3})",
        r"([A-Z][A-Za-z0-9&.\-]*(?:\s+[A-Z][A-Za-z0-9&.\-]*){1,3})\s+named in",
        r"audit of\s+([A-Z][A-Za-z0-9&.\-]*(?:\s+[A-Z][A-Za-z0-9&.\-]*){1,3})",
        r"review of\s+([A-Z][A-Za-z0-9&.\-]*(?:\s+[A-Z][A-Za-z0-9&.\-]*){1,3})",
    )
    for pattern in contextual_patterns:
        match = re.search(pattern, text)
        if match is not None:
            candidate = _clean_company_candidate(match.group(1))
            if candidate != "":
                return candidate

    corporate_patterns = (
        r"\b([A-Z][A-Za-z0-9&.\-]*(?:\s+[A-Z][A-Za-z0-9&.\-]*)*\s+(?:Inc|LLC|Corp|Corporation|Co|Company|Ltd))\b",
        r"\b([A-Z][A-Za-z0-9&.\-]*(?:\s+[A-Z][A-Za-z0-9&.\-]*)*\s+Contractors?)\b",
        r"\b([A-Z][A-Za-z0-9&.\-]*(?:\s+[A-Z][A-Za-z0-9&.\-]*)*\s+Construction)\b",
        r"\b([A-Z][A-Za-z0-9&.\-]*(?:\s+[A-Z][A-Za-z0-9&.\-]*)*\s+Builders?)\b",
        r"\b([A-Z][A-Za-z0-9&.\-]*(?:\s+[A-Z][A-Za-z0-9&.\-]*)*\s+Services)\b",
        r"\b([A-Z][A-Za-z0-9&.\-]*(?:\s+[A-Z][A-Za-z0-9&.\-]*)*\s+Group)\b",
    )
    for pattern in corporate_patterns:
        match = re.search(pattern, text)
        if match is not None:
            candidate = _clean_company_candidate(match.group(1))
            if candidate != "":
                return candidate

    for match in re.finditer(r"\b([A-Z][A-Za-z0-9&.\-]*(?:\s+[A-Z][A-Za-z0-9&.\-]*){1,3})\b", text):
        candidate = _clean_company_candidate(match.group(1))
        if candidate != "":
            return candidate
    return "unknown"


def extract_signal(
    result_item: dict[str, str],
    *,
    signal_type: str,
) -> dict[str, object]:
    if signal_type not in _VALID_SIGNAL_TYPES:
        raise ValueError("invalid_signal_type")
    if not isinstance(result_item, dict):
        raise ValueError("invalid_result_item")
    required_keys = {"title", "snippet", "url"}
    if not required_keys.issubset(result_item.keys()):
        raise ValueError("invalid_result_item")
    if not all(isinstance(result_item[key], str) and result_item[key].strip() != "" for key in required_keys):
        raise ValueError("invalid_result_item")
    if "source_provider" in result_item and (
        not isinstance(result_item["source_provider"], str) or result_item["source_provider"].strip() == ""
    ):
        raise ValueError("invalid_result_item")

    title = _collapse_whitespace(result_item["title"])
    snippet = _collapse_whitespace(result_item["snippet"])
    url = result_item["url"].strip()
    raw_text = f"{title} {snippet}"

    return {
        "company": _extract_company(raw_text),
        "signal_type": signal_type,
        "event_summary": title,
        "source": url,
        "date_detected": _extract_date_detected(raw_text),
        "confidence_score": None,
        "priority": None,
        "raw_text": raw_text,
    }
