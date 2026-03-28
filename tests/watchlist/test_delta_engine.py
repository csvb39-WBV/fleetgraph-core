from __future__ import annotations

import pathlib
import sys


PROJECT_ROOT = pathlib.Path(__file__).resolve().parents[2]
SRC_ROOT = PROJECT_ROOT / "src"

if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))


from fleetgraph.watchlist.delta_engine import build_company_delta_summary


def _company(**overrides: object) -> dict[str, object]:
    company = {
        "company_id": "turner-construction--ny",
        "company_name": "Turner Construction",
        "enrichment_state": "partial",
        "recent_signals": [],
        "recent_projects": [],
        "published_emails": [],
        "key_people": [],
        "confidence_level": "medium",
        "last_enriched_at": "2026-03-27",
        "source_links": ["https://example.com/base"],
    }
    company.update(overrides)
    return company


def test_delta_detection_is_deterministic_for_fixed_inputs() -> None:
    previous = _company()
    current = _company(
        enrichment_state="enriched",
        recent_signals=[{"event_summary": "Investigation announced", "source_url": "https://example.com/signal"}],
        published_emails=[{"email": "jane.doe@example.com", "source_url": "https://example.com/email"}],
        key_people=[{"name": "Jane Doe", "title": "CEO", "source_url": "https://example.com/people"}],
        confidence_level="high",
        last_enriched_at="2026-03-28",
        source_links=["https://example.com/base", "https://example.com/signal"],
    )

    first = build_company_delta_summary(previous, current)
    second = build_company_delta_summary(previous, current)

    assert first == second
    assert first["change_detected"] is True
    assert first["change_types"] == [
        "enrichment_state_changed",
        "new_signals_added",
        "new_public_emails_added",
        "new_key_people_added",
        "confidence_level_changed",
        "last_enriched_at_changed",
        "source_link_count_changed",
    ]


def test_delta_detection_counts_new_signals_projects_emails_and_people() -> None:
    previous = _company(
        recent_signals=[{"event_summary": "Audit opened", "source_url": "https://example.com/audit"}],
        recent_projects=[{"project_summary": "Bridge repair", "source_url": "https://example.com/project-a"}],
    )
    current = _company(
        recent_signals=[
            {"event_summary": "Audit opened", "source_url": "https://example.com/audit"},
            {"event_summary": "Lawsuit filed", "source_url": "https://example.com/lawsuit"},
        ],
        recent_projects=[
            {"project_summary": "Bridge repair", "source_url": "https://example.com/project-a"},
            {"project_summary": "School build", "source_url": "https://example.com/project-b"},
        ],
        published_emails=[{"email": "legal@example.com", "source_url": "https://example.com/contact"}],
        key_people=[{"name": "Jane Doe", "title": "CEO", "source_url": "https://example.com/people"}],
        last_enriched_at="2026-03-28",
    )

    delta = build_company_delta_summary(previous, current)

    assert delta["new_signal_count"] == 1
    assert delta["new_project_count"] == 1
    assert delta["new_email_count"] == 1
    assert delta["new_key_people_count"] == 1


def test_delta_detection_handles_missing_previous_and_current_artifacts() -> None:
    current = _company(last_enriched_at=None)

    missing_previous = build_company_delta_summary(None, current)
    missing_current = build_company_delta_summary(current, None)

    assert missing_previous["change_types"] == [
        "missing_previous_artifact",
        "source_link_count_changed",
    ]
    assert missing_current["change_types"] == [
        "missing_current_artifact",
        "last_enriched_at_changed",
        "source_link_count_changed",
    ]
