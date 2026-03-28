from __future__ import annotations

import json
import pathlib
import sys


PROJECT_ROOT = pathlib.Path(__file__).resolve().parents[2]
SRC_ROOT = PROJECT_ROOT / "src"

if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))


from fleetgraph.watchlist.artifact_writer import write_watchlist_artifact
from fleetgraph.watchlist.read_service import (
    derive_enrichment_state,
    get_watchlist_company_record,
    list_watchlist_company_records,
    merge_seed_with_artifact,
    read_watchlist_artifact,
)
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


def _artifact(company_name: str) -> dict[str, object]:
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
        "published_emails": [{"email": "jane.doe@example.com", "source_url": "https://example.com/contact", "confidence": "high"}],
        "email_pattern_guess": None,
        "recent_signals": [{"event_summary": "Investigation announced", "source_url": "https://example.com/signal", "source_provider": "rss_news", "confidence": "medium"}],
        "recent_projects": [],
        "source_links": ["https://example.com/contact"],
        "last_enriched_at": "2026-03-28",
        "confidence_level": "high",
    }


def test_read_watchlist_artifact_handles_missing_and_invalid_artifacts(tmp_path: pathlib.Path) -> None:
    company = load_verified_subset()[0]
    runtime_config = _runtime_config(tmp_path)

    missing = read_watchlist_artifact(company["company_id"], runtime_config=runtime_config)
    assert missing == {
        "ok": False,
        "artifact": None,
        "artifact_path": str((pathlib.Path(runtime_config["output_directory"]).resolve() / "watchlist" / f"{company['company_id']}.json")),
        "error_code": "missing_artifact",
    }

    invalid_path = pathlib.Path(runtime_config["output_directory"]).resolve() / "watchlist"
    invalid_path.mkdir(parents=True, exist_ok=True)
    (invalid_path / f"{company['company_id']}.json").write_text("{bad", encoding="utf-8")
    invalid = read_watchlist_artifact(company["company_id"], runtime_config=runtime_config)
    assert invalid["ok"] is False
    assert invalid["error_code"] == "invalid_artifact"


def test_seed_artifact_merge_and_state_derivation_are_deterministic(tmp_path: pathlib.Path) -> None:
    company = load_verified_subset()[0]
    artifact = _artifact(str(company["company_name"]))

    first = merge_seed_with_artifact(company, artifact)
    second = merge_seed_with_artifact(company, artifact)

    assert first == second
    assert first["enrichment_state"] == "enriched"
    assert first["artifact_status"] == "ok"
    assert derive_enrichment_state(company, None) == "seed_only"
    partial_artifact = dict(artifact)
    partial_artifact["published_emails"] = []
    partial_artifact["recent_signals"] = []
    partial_artifact["last_enriched_at"] = "2026-03-28"
    assert derive_enrichment_state(company, partial_artifact) == "partial"


def test_list_watchlist_company_records_reads_real_artifacts_in_stable_order(tmp_path: pathlib.Path) -> None:
    runtime_config = _runtime_config(tmp_path)
    records = load_verified_subset()[:2]
    write_watchlist_artifact(_artifact(str(records[1]["company_name"])), pathlib.Path(runtime_config["output_directory"]) / "watchlist", company_id=str(records[1]["company_id"]))

    companies = list_watchlist_company_records(runtime_config=runtime_config, dataset="verified_subset")

    assert len(companies) == 27
    assert companies == list_watchlist_company_records(runtime_config=runtime_config, dataset="verified_subset")
    enriched_company = next(company for company in companies if company["company_id"] == records[1]["company_id"])
    assert enriched_company["enrichment_state"] == "enriched"
    assert enriched_company["last_enriched_at"] == "2026-03-28"
    assert enriched_company["artifact_status"] == "ok"


def test_get_watchlist_company_record_supports_seed_only_invalid_and_unknown_company(tmp_path: pathlib.Path) -> None:
    runtime_config = _runtime_config(tmp_path)
    company = load_verified_subset()[0]

    seed_only = get_watchlist_company_record(company["company_id"], runtime_config=runtime_config, dataset="verified_subset")
    assert seed_only["ok"] is True
    assert seed_only["company"]["enrichment_state"] == "seed_only"
    assert seed_only["company"]["last_enriched_at"] is None
    assert seed_only["company"]["artifact_status"] == "missing_artifact"

    invalid_path = pathlib.Path(runtime_config["output_directory"]).resolve() / "watchlist"
    invalid_path.mkdir(parents=True, exist_ok=True)
    (invalid_path / f"{company['company_id']}.json").write_text("{bad", encoding="utf-8")
    invalid = get_watchlist_company_record(company["company_id"], runtime_config=runtime_config, dataset="verified_subset")
    assert invalid["ok"] is True
    assert invalid["company"]["artifact_status"] == "invalid_artifact"

    unknown = get_watchlist_company_record("missing-company", runtime_config=runtime_config, dataset="verified_subset")
    assert unknown == {
        "ok": False,
        "company": None,
        "error_code": "unknown_company_id",
    }
