import re
from typing import Any, Dict, List, Optional, Tuple


EMAIL_DOMAIN_PATTERN = re.compile(r"[\w\.-]+@([\w\.-]+\.\w+)", re.IGNORECASE)
DOMAIN_PATTERN = re.compile(r"(https?://)?([\w\.-]+\.\w{2,})", re.IGNORECASE)
PHONE_PATTERN = re.compile(r"\+?\d[\d\-\s\(\)]{7,}")
LEGAL_ENTITY_PATTERN = re.compile(r"\b[A-Za-z0-9&.,\- ]{1,60}\b(?:LLC|Inc|Ltd|Corp)\b", re.IGNORECASE)
STREET_WORDS = (" st", " ave", " rd", " blvd", " street", " avenue", " road", " boulevard")


def _normalize_space(value: str) -> str:
    return " ".join(value.strip().split())


def _snippet(html: str, start: int, end: int) -> str:
    center = (start + end) // 2
    left = max(0, center - 80)
    right = min(len(html), left + 180)
    snippet = _normalize_space(html[left:right])
    return snippet[:200]


def _normalize_domain(domain: str) -> str:
    normalized = domain.strip().lower()
    if normalized.startswith("www."):
        normalized = normalized[4:]
    return normalized


def _append_signal(
    signals: List[Dict[str, Any]],
    signal_type: str,
    value: str,
    source_url: str,
    source_type: str,
    snippet: str,
) -> None:
    normalized_value = _normalize_space(value)
    if signal_type in ("shared_email_domain", "mentioned_domain"):
        normalized_value = _normalize_domain(normalized_value)
    if signal_type == "shared_phone":
        normalized_value = "".join(ch for ch in normalized_value if ch.isdigit())

    if not normalized_value:
        return

    signals.append(
        {
            "type": signal_type,
            "value": normalized_value,
            "source_url": source_url,
            "source_type": source_type,
            "snippet": _normalize_space(snippet)[:200],
        }
    )


def build_query_bundle(org_name: str, domain: str) -> List[str]:
    return [
        f"{org_name} {domain} contact",
        f"{org_name} about",
        f"{domain} contact",
        f"{domain} privacy",
        f"{org_name} partners",
        f"{domain} terms",
    ]


def classify_url(url: str) -> str:
    lowered = url.lower()
    if "/contact" in lowered:
        return "contact"
    if "/about" in lowered:
        return "about"
    if "/privacy" in lowered or "/terms" in lowered:
        return "legal"
    if "/partner" in lowered or "/partners" in lowered:
        return "partner"
    return "home"


def extract_evidence_from_html(
    html: str,
    url: str,
    current_domain: Optional[str] = None,
) -> List[Dict[str, Any]]:
    source_type = classify_url(url)
    signals: List[Dict[str, Any]] = []

    for match in EMAIL_DOMAIN_PATTERN.finditer(html):
        _append_signal(
            signals,
            "shared_email_domain",
            match.group(1),
            url,
            source_type,
            _snippet(html, match.start(), match.end()),
        )

    for match in DOMAIN_PATTERN.finditer(html):
        domain_value = _normalize_domain(match.group(2))
        if current_domain and domain_value == _normalize_domain(current_domain):
            continue
        _append_signal(
            signals,
            "mentioned_domain",
            domain_value,
            url,
            source_type,
            _snippet(html, match.start(), match.end()),
        )

    for match in PHONE_PATTERN.finditer(html):
        _append_signal(
            signals,
            "shared_phone",
            match.group(0),
            url,
            source_type,
            _snippet(html, match.start(), match.end()),
        )

    for line in html.splitlines():
        lowered = line.lower()
        if any(word in lowered for word in STREET_WORDS) and any(ch.isdigit() for ch in line):
            start = html.find(line)
            end = start + len(line)
            _append_signal(
                signals,
                "shared_address",
                line,
                url,
                source_type,
                _snippet(html, start, end),
            )

    for match in LEGAL_ENTITY_PATTERN.finditer(html):
        _append_signal(
            signals,
            "legal_entity_reference",
            match.group(0),
            url,
            source_type,
            _snippet(html, match.start(), match.end()),
        )

    if source_type == "partner":
        lowered = html.lower()
        if "partner" in lowered or "affiliate" in lowered or "network" in lowered:
            _append_signal(
                signals,
                "partner_reference",
                "partner_page_reference",
                url,
                source_type,
                _snippet(html, 0, min(len(html), 160)),
            )

    deduped: List[Dict[str, Any]] = []
    seen: set[Tuple[str, str, str]] = set()
    for signal in signals:
        key = (signal["type"], signal["value"], signal["source_url"])
        if key in seen:
            continue
        seen.add(key)
        deduped.append(signal)

    return deduped


def _get_pages(record: Dict[str, Any]) -> List[Tuple[str, str]]:
    pages: List[Tuple[str, str]] = []

    html_pages = record.get("html_pages")
    if isinstance(html_pages, list):
        for item in html_pages:
            if not isinstance(item, dict):
                continue
            url = item.get("url")
            html = item.get("html")
            if isinstance(url, str) and isinstance(html, str) and url and html:
                pages.append((url, html))

    source_url = record.get("source_url")
    html_value = record.get("html")
    if isinstance(source_url, str) and isinstance(html_value, str) and source_url and html_value:
        pages.append((source_url, html_value))

    text_value = record.get("text")
    if isinstance(source_url, str) and isinstance(text_value, str) and source_url and text_value:
        pages.append((source_url, text_value))

    urls = record.get("urls")
    texts = record.get("texts")
    if isinstance(urls, list) and isinstance(texts, list):
        for idx, url in enumerate(urls):
            if idx >= len(texts):
                break
            html = texts[idx]
            if isinstance(url, str) and isinstance(html, str) and url and html:
                pages.append((url, html))

    ordered_pages = list(dict.fromkeys(pages))
    return ordered_pages


def acquire_evidence(records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    result: List[Dict[str, Any]] = []

    for record in records:
        new_record = dict(record)

        org_name = str(new_record.get("organization_name", "")).strip()
        domain = str(new_record.get("domain", "")).strip()
        if org_name or domain:
            build_query_bundle(org_name, domain)

        pages = _get_pages(new_record)
        all_signals: List[Dict[str, Any]] = []
        for url, html in pages:
            all_signals.extend(extract_evidence_from_html(html, url, current_domain=domain or None))

        deduped: List[Dict[str, Any]] = []
        seen: set[Tuple[str, str, str]] = set()
        for signal in all_signals:
            key = (signal["type"], signal["value"], signal["source_url"])
            if key in seen:
                continue
            seen.add(key)
            deduped.append(signal)

        if deduped:
            new_record["evidence_signals"] = deduped

        result.append(new_record)

    return result
