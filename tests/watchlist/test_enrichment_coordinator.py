from __future__ import annotations

import pathlib
import sys


PROJECT_ROOT = pathlib.Path(__file__).resolve().parents[2]
SRC_ROOT = PROJECT_ROOT / "src"

if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))


from fleetgraph.watchlist.enrichment_coordinator import build_enrichment_record
from fleetgraph.watchlist.watchlist_loader import load_verified_subset


def test_company_enrichment_artifact_contains_contact_intelligence_record() -> None:
    company = load_verified_subset()[0]
    search_results = [
        {
            "title": "Turner Construction lawsuit filed over project delay (212) 555-0100",
            "snippet": "Public email jane.doe@turnerconstruction.com appears in the filing. Reach the team via 120 West 45th Street New York, NY 10036.",
            "url": "https://example.com/contact/turner-lawsuit",
            "source_provider": "rss_news",
        },
        {
            "title": "Turner Construction leadership update",
            "snippet": "General inquiries: info@turnerconstruction.com.",
            "url": "https://example.com/leadership/turner-team",
            "source_provider": "duckduckgo_html",
        },
    ]

    enrichment_record = build_enrichment_record(company, search_results, run_date="2026-03-28")

    assert enrichment_record["company_name"] == "Turner Construction"
    assert enrichment_record["website"] == "https://www.turnerconstruction.com/"
    assert enrichment_record["main_phone"] == "(212) 229-6000"
    assert enrichment_record["last_enriched_at"] == "2026-03-28"
    assert enrichment_record["published_emails"] == [
        {
            "email": "jane.doe@turnerconstruction.com",
            "source_url": "https://example.com/contact/turner-lawsuit",
            "confidence": "high",
            "type": "direct_email",
            "is_direct": True,
        }
    ]
    assert enrichment_record["general_emails"] == [
        {
            "email": "info@turnerconstruction.com",
            "source_url": "https://example.com/leadership/turner-team",
            "confidence": "high",
            "type": "general_email",
            "is_direct": False,
        }
    ]
    assert enrichment_record["direct_phones"] == [
        {
            "phone": "(212) 555-0100",
            "source_url": "https://example.com/contact/turner-lawsuit",
            "confidence": "medium",
            "type": "phone",
            "is_direct": True,
        }
    ]
    assert enrichment_record["contact_pages"] == [
        "https://example.com/contact/turner-lawsuit",
        "https://example.com/leadership/turner-team",
    ]
    assert enrichment_record["leadership_pages"] == [
        "https://example.com/leadership/turner-team",
    ]
    assert enrichment_record["address_lines"] == [
        "120 West 45th Street",
        "New York, NY 10036",
    ]
    assert enrichment_record["contact_sources"] == [
        "https://example.com/contact/turner-lawsuit",
        "https://example.com/leadership/turner-team",
    ]
    assert enrichment_record["email_pattern_guess"] is None
    assert enrichment_record["contact_confidence_level"] == "high"
    assert enrichment_record["reachability_score"] == 70


def test_non_public_direct_emails_are_never_fabricated() -> None:
    company = load_verified_subset()[0]
    search_results = [
        {
            "title": "Turner Construction investigation announced",
            "snippet": "Investigation announced with no public email listed.",
            "url": "https://example.com/about/turner-investigation",
            "source_provider": "rss_news",
        }
    ]

    enrichment_record = build_enrichment_record(company, search_results, run_date="2026-03-28")

    assert enrichment_record["published_emails"] == []
    assert enrichment_record["general_emails"] == []
    assert enrichment_record["direct_phones"] == []
    assert enrichment_record["email_pattern_guess"] == "first.last@turnerconstruction.com"
    assert enrichment_record["contact_confidence_level"] == "low"
    assert enrichment_record["reachability_score"] == 10


def test_contact_dedupe_and_empty_hit_case_are_deterministic() -> None:
    company = load_verified_subset()[0]
    duplicated_results = [
        {
            "title": "Turner Construction contact page (212) 555-0100",
            "snippet": "Email jane.doe@turnerconstruction.com for support.",
            "url": "https://example.com/contact/turner",
            "source_provider": "rss_news",
        },
        {
            "title": "Turner Construction contact page (212) 555-0100",
            "snippet": "Email jane.doe@turnerconstruction.com for support.",
            "url": "https://example.com/contact/turner",
            "source_provider": "duckduckgo_html",
        },
    ]

    duplicated = build_enrichment_record(company, duplicated_results, run_date="2026-03-28")
    first_empty = build_enrichment_record(company, [], run_date="2026-03-28")
    second_empty = build_enrichment_record(company, [], run_date="2026-03-28")

    assert len(duplicated["published_emails"]) == 1
    assert len(duplicated["direct_phones"]) == 1
    assert duplicated["contact_pages"] == ["https://example.com/contact/turner"]
    assert first_empty == second_empty
    assert first_empty["published_emails"] == []
    assert first_empty["general_emails"] == []
    assert first_empty["direct_phones"] == []
    assert first_empty["recent_signals"] == []
    assert first_empty["recent_projects"] == []
