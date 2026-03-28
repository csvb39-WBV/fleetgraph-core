from __future__ import annotations


def _query_suffix(*parts: str | None) -> str:
    return " ".join(part for part in parts if isinstance(part, str) and part.strip() != "")


def generate_company_query_pack(watchlist_entity: dict[str, object]) -> list[dict[str, object]]:
    company_name = str(watchlist_entity["company_name"])
    category = str(watchlist_entity["category"] or "").strip()
    segment = str(watchlist_entity["segment"] or "").strip()
    website_domain = str(watchlist_entity["website_domain"] or "").strip()
    quoted_company_name = f'"{company_name}"'
    category_suffix = _query_suffix(category, segment)
    domain_suffix = f"site:{website_domain}" if website_domain != "" else ""

    return [
        {
            "query_id": "watchlist_lawsuit_filed",
            "query": _query_suffix(quoted_company_name, "lawsuit filed", category_suffix),
            "signal_type": "litigation",
            "intent_type": "event_based",
            "max_results": 5,
            "query_kind": "event",
        },
        {
            "query_id": "watchlist_audit_investigation",
            "query": _query_suffix(quoted_company_name, "audit investigation", category_suffix),
            "signal_type": "audit",
            "intent_type": "event_based",
            "max_results": 5,
            "query_kind": "event",
        },
        {
            "query_id": "watchlist_dispute_delay_default",
            "query": _query_suffix(quoted_company_name, "dispute delay default", category_suffix),
            "signal_type": "project_distress",
            "intent_type": "event_based",
            "max_results": 5,
            "query_kind": "event",
        },
        {
            "query_id": "watchlist_subpoena_claim",
            "query": _query_suffix(quoted_company_name, "subpoena claim", category_suffix),
            "signal_type": "litigation",
            "intent_type": "event_based",
            "max_results": 4,
            "query_kind": "event",
        },
        {
            "query_id": "watchlist_category_dispute",
            "query": _query_suffix(quoted_company_name, category, "dispute", segment),
            "signal_type": "project_distress",
            "intent_type": "event_based",
            "max_results": 4,
            "query_kind": "event",
        },
        {
            "query_id": "watchlist_domain_investigation",
            "query": _query_suffix(quoted_company_name, domain_suffix, "investigation"),
            "signal_type": "audit",
            "intent_type": "event_based",
            "max_results": 4,
            "query_kind": "event",
        },
    ]
