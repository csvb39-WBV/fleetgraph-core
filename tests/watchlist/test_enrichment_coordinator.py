from __future__ import annotations

import pathlib
import sys


PROJECT_ROOT = pathlib.Path(__file__).resolve().parents[2]
SRC_ROOT = PROJECT_ROOT / "src"

if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))


from fleetgraph.watchlist.enrichment_coordinator import build_enrichment_record
from fleetgraph.watchlist.watchlist_loader import load_verified_subset


def test_company_enrichment_artifact_contains_structured_record() -> None:
    company = load_verified_subset()[0]
    search_results = [
        {
            "title": "Turner Construction lawsuit filed over project delay",
            "snippet": "Public email jane.doe@turnerconstruction.com appears in the filing.",
            "url": "https://example.com/turner-lawsuit",
            "source_provider": "rss_news",
        }
    ]

    enrichment_record = build_enrichment_record(company, search_results, run_date="2026-03-28")

    assert enrichment_record["company_name"] == "Turner Construction"
    assert enrichment_record["website"] == "https://www.turnerconstruction.com/"
    assert enrichment_record["main_phone"] == "(212) 229-6000"
    assert enrichment_record["last_enriched_at"] == "2026-03-28"
    assert enrichment_record["published_emails"] == [
        {
            "email": "jane.doe@turnerconstruction.com",
            "source_url": "https://example.com/turner-lawsuit",
            "confidence": "high",
        }
    ]
    assert enrichment_record["email_pattern_guess"] is None


def test_non_public_direct_emails_are_never_fabricated() -> None:
    company = load_verified_subset()[0]
    search_results = [
        {
            "title": "Turner Construction investigation announced",
            "snippet": "Investigation announced with no public email listed.",
            "url": "https://example.com/turner-investigation",
            "source_provider": "rss_news",
        }
    ]

    enrichment_record = build_enrichment_record(company, search_results, run_date="2026-03-28")

    assert enrichment_record["published_emails"] == []
    assert enrichment_record["email_pattern_guess"] == "first.last@turnerconstruction.com"


def test_empty_hit_case_returns_deterministic_empty_enrichment_state() -> None:
    company = load_verified_subset()[0]

    first = build_enrichment_record(company, [], run_date="2026-03-28")
    second = build_enrichment_record(company, [], run_date="2026-03-28")

    assert first == second
    assert first["published_emails"] == []
    assert first["recent_signals"] == []
    assert first["recent_projects"] == []
