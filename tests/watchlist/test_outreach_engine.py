from __future__ import annotations

import pathlib
import sys


PROJECT_ROOT = pathlib.Path(__file__).resolve().parents[2]
SRC_ROOT = PROJECT_ROOT / "src"

if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))


import fleetgraph.watchlist.outreach_engine as outreach_engine
from fleetgraph.watchlist.outreach_engine import (
    build_outreach_record,
    determine_outreach_status,
    get_outreach_record,
    list_outreach_ready_companies,
    prepare_outreach_record,
    read_outreach_record,
)
from fleetgraph.watchlist.outreach_templates import build_email_body, build_subject_line
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


def _company(**overrides: object) -> dict[str, object]:
    company = {
        "company_id": "turner-construction--ny",
        "company_name": "Turner Construction",
        "published_emails": [{"email": "jane.doe@turnerconstruction.com", "source_url": "https://example.com/contact", "confidence": "high", "type": "direct_email", "is_direct": True}],
        "general_emails": [],
        "direct_phones": [{"phone": "(212) 555-0100", "source_url": "https://example.com/contact", "confidence": "medium", "type": "phone", "is_direct": True}],
        "contact_confidence_level": "high",
        "key_people": [{"name": "Jane Doe", "title": "CEO", "source_url": "https://example.com/leadership", "confidence": "high"}],
        "recent_signals": [{"event_summary": "lawsuit filed over payment dispute", "source_url": "https://example.com/signal"}],
        "source_links": ["https://example.com/signal"],
        "artifact_status": "ok",
    }
    company.update(overrides)
    return company


def test_outreach_qualification_is_deterministic_and_ready_with_contact_and_signal() -> None:
    company = _company()

    first = determine_outreach_status(company)
    second = determine_outreach_status(company)

    assert first == second
    assert first == {
        "outreach_status": "ready_to_draft",
        "qualification_reasons": ["qualified_for_outreach"],
        "ready_to_draft": True,
    }


def test_missing_contact_or_signal_blocks_readiness() -> None:
    no_contact = determine_outreach_status(_company(published_emails=[], direct_phones=[], recent_signals=[{"event_summary": "audit announced"}]))
    no_signal = determine_outreach_status(_company(recent_signals=[]))

    assert no_contact["outreach_status"] == "not_ready"
    assert "missing_contact_method" in no_contact["qualification_reasons"]
    assert no_signal["outreach_status"] == "not_ready"
    assert "missing_meaningful_signal" in no_signal["qualification_reasons"]


def test_subject_and_body_generation_are_deterministic() -> None:
    subject_first = build_subject_line(company_name="Turner Construction", signal_summary="lawsuit filed over payment dispute", target_role_guess="legal_risk")
    subject_second = build_subject_line(company_name="Turner Construction", signal_summary="lawsuit filed over payment dispute", target_role_guess="legal_risk")
    body_first = build_email_body(
        contact_name="Jane Doe",
        company_name="Turner Construction",
        signal_summary="lawsuit filed over payment dispute",
        why_now="Recent public signal: lawsuit filed over payment dispute.",
        why_this_company="We found a direct contact path and Turner Construction appears timely because the legal and risk workflow may be under pressure.",
    )
    body_second = build_email_body(
        contact_name="Jane Doe",
        company_name="Turner Construction",
        signal_summary="lawsuit filed over payment dispute",
        why_now="Recent public signal: lawsuit filed over payment dispute.",
        why_this_company="We found a direct contact path and Turner Construction appears timely because the legal and risk workflow may be under pressure.",
    )

    assert subject_first == subject_second == "Question about Turner Construction lawsuit filed over payment dispute"
    assert body_first == body_second
    assert "Hi Jane Doe," in body_first
    assert "FactLedger helps teams cut document chasing" in body_first


def test_outreach_record_persists_and_reloads_correctly(tmp_path: pathlib.Path, monkeypatch) -> None:
    runtime_config = _runtime_config(tmp_path)
    company = load_verified_subset()[0]
    company_record = _company(company_id=company["company_id"], company_name=company["company_name"])

    monkeypatch.setattr(outreach_engine, "get_watchlist_company_record", lambda *args, **kwargs: {"ok": True, "company": dict(company_record), "error_code": None})

    prepared = prepare_outreach_record(company["company_id"], runtime_config=runtime_config)
    reloaded = read_outreach_record(company["company_id"], runtime_config=runtime_config)
    fetched = get_outreach_record(company["company_id"], runtime_config=runtime_config)

    assert prepared["ok"] is True
    assert reloaded["ok"] is True
    assert reloaded["outreach_record"] == prepared["outreach_record"]
    assert fetched["ok"] is True
    assert fetched["outreach_record"] == prepared["outreach_record"]
    assert prepared["outreach_record"]["outreach_status"] == "ready_to_draft"


def test_outreach_ready_listing_and_unknown_company_are_deterministic(tmp_path: pathlib.Path, monkeypatch) -> None:
    runtime_config = _runtime_config(tmp_path)
    company_a = _company(company_id="a-company", company_name="Alpha Build")
    company_b = _company(company_id="b-company", company_name="Bravo Build", recent_signals=[])

    monkeypatch.setattr(outreach_engine, "list_watchlist_company_records", lambda *args, **kwargs: [dict(company_b), dict(company_a)])
    monkeypatch.setattr(outreach_engine, "get_watchlist_company_record", lambda company_id, **kwargs: {"ok": False, "company": None, "error_code": "unknown_company_id"})

    first = list_outreach_ready_companies(runtime_config)
    second = list_outreach_ready_companies(runtime_config)
    unknown = prepare_outreach_record("missing-company", runtime_config=runtime_config)

    assert first == second
    assert [record["company_id"] for record in first["outreach_ready"]] == ["a-company"]
    assert unknown == {
        "ok": False,
        "outreach_record": None,
        "record_path": None,
        "error_code": "unknown_company_id",
    }


def test_suppressed_and_drafted_states_are_preserved_without_send_path(tmp_path: pathlib.Path, monkeypatch) -> None:
    runtime_config = _runtime_config(tmp_path)
    company = load_verified_subset()[0]
    company_record = _company(company_id=company["company_id"], company_name=company["company_name"])

    monkeypatch.setattr(outreach_engine, "get_watchlist_company_record", lambda *args, **kwargs: {"ok": True, "company": dict(company_record), "error_code": None})

    drafted = prepare_outreach_record(company["company_id"], runtime_config=runtime_config, status_override="drafted")
    suppressed = prepare_outreach_record(company["company_id"], runtime_config=runtime_config, status_override="suppressed")
    built_suppressed = build_outreach_record(company_record, run_date="2026-03-28", existing_status="suppressed", suppressed=True)

    assert drafted["outreach_record"]["outreach_status"] == "drafted"
    assert suppressed["outreach_record"]["outreach_status"] == "suppressed"
    assert built_suppressed["qualification_reasons"] == ["company_suppressed"]
    assert hasattr(outreach_engine, "send_outreach") is False
