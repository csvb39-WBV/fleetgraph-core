from __future__ import annotations


_REQUIRED_SIGNAL_KEYS = {
    "company",
    "signal_type",
    "event_summary",
    "source",
    "date_detected",
    "confidence_score",
    "priority",
    "raw_text",
}
_GENERIC_COMPANY_TERMS = {
    "unknown",
    "company",
    "contractor",
    "real estate company",
    "construction company",
    "service company",
}
_STRONG_LITIGATION_TERMS = (
    "lawsuit",
    "litigation",
    "claim",
    "sued",
)
_STRONG_ENTITY_TERMS = (
    "contractor",
    "subcontractor",
    "developer",
    "developer group",
    "construction management firm",
    "epc contractor",
    "specialty contractor",
    "electrical contractor",
    "mechanical contractor",
    "plumbing contractor",
    "roofing contractor",
    "glazing contractor",
    "civil contractor",
    "utility contractor",
    "infrastructure contractor",
    "public works contractor",
    "company",
    "corporation",
    "firm",
    "group",
    "holdings",
    "services",
    "partners",
    "associates",
)
_LEGAL_ENTITY_TERMS = (
    "law firm",
    "counsel",
    "outside counsel",
    "legal department",
    "llp",
    "pllc",
    "pc",
    "p.c.",
)
_AUDIT_ENTITY_TERMS = _STRONG_ENTITY_TERMS + _LEGAL_ENTITY_TERMS
_WEAK_PRESSURE_TERMS = (
    "dispute",
    "hearing",
    "review",
    "compliance",
    "legal",
)


def _is_non_empty_string(value: object) -> bool:
    return isinstance(value, str) and value.strip() != ""


def _validate_signal(signal: object) -> None:
    if not isinstance(signal, dict):
        raise ValueError("signal must be a dict")
    if set(signal.keys()) != _REQUIRED_SIGNAL_KEYS:
        raise ValueError("signal must contain the exact locked signal contract")

    for field_name in (
        "company",
        "signal_type",
        "event_summary",
        "source",
        "date_detected",
        "raw_text",
    ):
        if not _is_non_empty_string(signal[field_name]):
            raise ValueError(f"{field_name} must be a non-empty string")

    if signal["confidence_score"] is not None:
        raise ValueError("confidence_score must be None before scoring")
    if signal["priority"] is not None:
        raise ValueError("priority must be None before formatting")


def _contains_any(text: str, terms: tuple[str, ...]) -> bool:
    return any(term in text for term in terms)


def _normalized_company(company: str) -> str:
    return company.strip().lower()


def _is_generic_company(company: str) -> bool:
    return _normalized_company(company) in _GENERIC_COMPANY_TERMS


def _source_type_key(source: str) -> str:
    normalized_source = source.strip().lower()
    if "rss" in normalized_source or "news" in normalized_source:
        return "rss_news"
    if "duckduckgo" in normalized_source and "api" in normalized_source:
        return "duckduckgo_api"
    return "duckduckgo_html"


def _base_score_signal(signal: dict[str, object]) -> int:
    signal_type = str(signal["signal_type"]).strip().lower()
    event_summary = str(signal["event_summary"]).strip().lower()
    raw_text = str(signal["raw_text"]).strip().lower()
    company = str(signal["company"]).strip().lower()
    combined_text = " ".join((signal_type, event_summary, raw_text, company))

    if _contains_any(combined_text, ("default notice", "notice of default", "default")):
        return 5
    if "investigation" in combined_text:
        return 5
    if "subpoena" in combined_text:
        return 5
    if "mechanics lien" in combined_text and _contains_any(combined_text, _STRONG_ENTITY_TERMS):
        return 5
    if _contains_any(combined_text, _STRONG_LITIGATION_TERMS):
        return 5
    if "document production" in combined_text and _contains_any(combined_text, ("litigation", "counsel", "law firm")):
        return 5
    if "ediscovery" in combined_text and _contains_any(combined_text, ("law firm", "counsel", "litigation")):
        return 5
    if "forensic review" in combined_text and _contains_any(combined_text, ("litigation", "counsel", "investigation")):
        return 5

    if "audit" in combined_text and _contains_any(combined_text, _AUDIT_ENTITY_TERMS):
        return 4
    if "internal investigation" in combined_text and _contains_any(combined_text, ("company", "counsel", "legal")):
        return 4
    if _contains_any(combined_text, ("regulatory inquiry", "regulatory investigation")) and _contains_any(
        combined_text,
        ("company", "counsel", "law firm"),
    ):
        return 4
    if _contains_any(combined_text, ("project delay", "delay", "dispute")) and _contains_any(
        combined_text,
        ("contractor", "developer", "company", "group"),
    ):
        return 4

    if _contains_any(combined_text, ("legal", "counsel", "document production", "compliance", "dispute")):
        return 3
    if _contains_any(combined_text, _WEAK_PRESSURE_TERMS):
        return 2
    return 1


def _score_signal(signal: dict[str, object]) -> int:
    base_score = _base_score_signal(signal)
    if _source_type_key(str(signal["source"])) == "rss_news":
        return min(5, base_score + 1)
    return base_score


def score_signals(signals: list[dict[str, object]]) -> list[dict[str, object]]:
    if not isinstance(signals, list) or len(signals) == 0:
        raise ValueError("signals must be a non-empty list")

    scored_signals: list[dict[str, object]] = []
    for signal in signals:
        _validate_signal(signal)
        if _is_generic_company(str(signal["company"])):
            continue
        confidence_score = _score_signal(signal)
        scored_signals.append(
            {
                "company": signal["company"],
                "signal_type": signal["signal_type"],
                "event_summary": signal["event_summary"],
                "source": signal["source"],
                "date_detected": signal["date_detected"],
                "confidence_score": confidence_score,
                "priority": None,
                "raw_text": signal["raw_text"],
            }
        )

    return scored_signals
