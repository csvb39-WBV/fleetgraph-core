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


def _extract_company(text: str) -> str:
    patterns = (
        r"\b([A-Z][A-Za-z0-9&.\-]*(?:\s+[A-Z][A-Za-z0-9&.\-]*)*\s+(?:Inc|LLC|Corp|Corporation|Co|Company|Ltd))\b",
        r"\b([A-Z][A-Za-z0-9&.\-]*(?:\s+[A-Z][A-Za-z0-9&.\-]*)*\s+Contractors?)\b",
        r"\b([A-Z][A-Za-z0-9&.\-]*(?:\s+[A-Z][A-Za-z0-9&.\-]*)*\s+Construction)\b",
        r"\b([A-Z][A-Za-z0-9&.\-]*(?:\s+[A-Z][A-Za-z0-9&.\-]*)*\s+Builders?)\b",
    )
    for pattern in patterns:
        match = re.search(pattern, text)
        if match is not None:
            return _collapse_whitespace(match.group(1))
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
    if set(result_item.keys()) != required_keys:
        raise ValueError("invalid_result_item")
    if not all(isinstance(result_item[key], str) and result_item[key].strip() != "" for key in required_keys):
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
