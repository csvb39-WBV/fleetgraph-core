from __future__ import annotations

import json
import pathlib
import sys


PROJECT_ROOT = pathlib.Path(__file__).resolve().parents[2]
SRC_ROOT = PROJECT_ROOT / "src"

if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))


from fleetgraph.watchlist.artifact_writer import write_watchlist_artifact
from fleetgraph.watchlist.delta_engine import build_company_delta_summary
from fleetgraph.watchlist.intelligence_service import (
    get_changed_company_record,
    list_changed_companies,
    list_needs_review_companies,
    list_top_target_companies,
    read_watchlist_delta_summary,
    write_watchlist_delta_summary,
)
from fleetgraph.watchlist.priority_engine import score_watchlist_company
from fleetgraph.watchlist.read_service import get_watchlist_company_record
from fleetgraph.watchlist.watchlist_loader import load_verified_subset


def _runtime_config(tmp_path: pathlib.Path) -> dict[str, object]:
    return {
        "run_date": "2026-03-28",
        "output_directory": str(tmp_path / "outputs"),
        "cache_path": str(tmp_path / "cache" / "watchlist_cache.json"),
        "max_queries_per_run": 14,
        "max_results_per_query": 5,
        "connector_timeout_seconds": 5.0,
        "connector_max_retries": 1,
    }


def _artifact(company_name: str, *, signal_title: str, email: str | None = None, last_enriched_at: str = "2026-03-28") -> dict[str, object]:
    published_emails = []
    if email is not None:
        published_emails.append({"email": email, "source_url": "https://example.com/contact", "confidence": "high", "type": "direct_email", "is_direct": True})
    return {
        "company_name": company_name,
        "website": "https://www.example.com/",
        "main_phone": "(212) 555-0100",
        "hq_city": "New York",
        "hq_state": "NY",
        "priority_tier": "1",
        "category": "General Contractor",
        "segment": "Large National",
        "key_people": [{"name": "Jane Doe", "title": "CEO", "source_url": "https://example.com/leadership", "confidence": "high"}],
        "direct_phones": [{"phone": "(212) 555-0100", "source_url": "https://example.com/contact", "confidence": "medium", "type": "phone", "is_direct": True}],
        "general_emails": [],
        "published_emails": published_emails,
        "contact_pages": ["https://example.com/contact"],
        "leadership_pages": ["https://example.com/leadership"],
        "address_lines": ["120 West 45th Street", "New York, NY 10036"],
        "contact_sources": ["https://example.com/contact", "https://example.com/leadership"],
        "email_pattern_guess": None if email is not None else "first.last@example.com",
        "contact_confidence_level": "high" if email is not None else "medium",
        "reachability_score": 70 if email is not None else 30,
        "recent_signals": [{"event_summary": signal_title, "source_url": "https://example.com/signal", "source_provider": "rss_news", "confidence": "medium"}],
        "recent_projects": [],
        "source_links": ["https://example.com/signal"],
        "last_enriched_at": last_enriched_at,
        "confidence_level": "high" if email is not None else "medium",
    }


def test_delta_summary_round_trip_and_invalid_handling(tmp_path: pathlib.Path) -> None:
    runtime_config = _runtime_config(tmp_path)
    company = load_verified_subset()[0]
    current_company = get_watchlist_company_record(company["company_id"], runtime_config=runtime_config)["company"]
    delta_summary = build_company_delta_summary(None, current_company)
    scored = score_watchlist_company(current_company, delta_summary=delta_summary, reference_date="2026-03-28")
    delta_summary["priority_score"] = scored["priority_score"]
    delta_summary["priority_reason_codes"] = scored["priority_reason_codes"]

    delta_path = write_watchlist_delta_summary(delta_summary, runtime_config=runtime_config, company_id=company["company_id"])
    result = read_watchlist_delta_summary(company["company_id"], runtime_config=runtime_config)

    assert pathlib.Path(delta_path).exists() is True
    assert result["ok"] is True
    assert result["delta_summary"] == delta_summary

    pathlib.Path(delta_path).write_text("{bad", encoding="utf-8")
    invalid = read_watchlist_delta_summary(company["company_id"], runtime_config=runtime_config)
    assert invalid["ok"] is False
    assert invalid["error_code"] == "invalid_delta_summary"


def test_changed_top_targets_and_needs_review_surfaces_are_stable(tmp_path: pathlib.Path) -> None:
    runtime_config = _runtime_config(tmp_path)
    records = load_verified_subset()[:2]

    write_watchlist_artifact(
        _artifact(str(records[0]["company_name"]), signal_title="Investigation announced", email="jane.doe@example.com"),
        pathlib.Path(runtime_config["output_directory"]) / "watchlist",
        company_id=str(records[0]["company_id"]),
    )
    write_watchlist_artifact(
        _artifact(str(records[1]["company_name"]), signal_title="Routine notice posted", last_enriched_at="2026-03-27"),
        pathlib.Path(runtime_config["output_directory"]) / "watchlist",
        company_id=str(records[1]["company_id"]),
    )

    previous_company = get_watchlist_company_record(records[0]["company_id"], runtime_config=runtime_config)["company"]
    current_company = dict(previous_company)
    current_company["recent_signals"] = previous_company["recent_signals"] + [{"event_summary": "Lawsuit filed", "source_url": "https://example.com/lawsuit"}]
    current_company["published_emails"] = previous_company["published_emails"] + [{"email": "legal@example.com", "source_url": "https://example.com/legal", "confidence": "high", "type": "direct_email", "is_direct": True}]
    current_company["last_enriched_at"] = "2026-03-28"
    delta_summary = build_company_delta_summary(previous_company, current_company)
    scored = score_watchlist_company(current_company, delta_summary=delta_summary, reference_date="2026-03-28")
    delta_summary["priority_score"] = scored["priority_score"]
    delta_summary["priority_reason_codes"] = scored["priority_reason_codes"]
    write_watchlist_delta_summary(delta_summary, runtime_config=runtime_config, company_id=records[0]["company_id"])

    changed_first = list_changed_companies(runtime_config, dataset="verified_subset")
    changed_second = list_changed_companies(runtime_config, dataset="verified_subset")
    top_targets_first = list_top_target_companies(runtime_config, dataset="verified_subset", limit=3)
    top_targets_second = list_top_target_companies(runtime_config, dataset="verified_subset", limit=3)
    needs_review_first = list_needs_review_companies(runtime_config, dataset="verified_subset")
    needs_review_second = list_needs_review_companies(runtime_config, dataset="verified_subset")

    assert changed_first == changed_second
    assert top_targets_first == top_targets_second
    assert needs_review_first == needs_review_second
    assert changed_first["changed_companies"][0]["company_id"] == records[0]["company_id"]
    assert changed_first["changed_companies"][0]["new_signal_count"] == 1
    assert top_targets_first["top_targets"][0]["company_id"] == records[0]["company_id"]
    assert needs_review_first["needs_review"][0]["company_id"] == records[0]["company_id"]


def test_get_changed_company_record_handles_missing_delta_and_unknown_company(tmp_path: pathlib.Path) -> None:
    runtime_config = _runtime_config(tmp_path)
    company = load_verified_subset()[0]

    missing_delta = get_changed_company_record(company["company_id"], runtime_config=runtime_config)
    unknown = get_changed_company_record("missing-company", runtime_config=runtime_config)

    assert missing_delta["ok"] is True
    assert missing_delta["delta_summary"] is None
    assert unknown == {
        "ok": False,
        "company": None,
        "delta_summary": None,
        "error_code": "unknown_company_id",
    }
