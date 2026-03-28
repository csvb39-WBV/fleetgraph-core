from __future__ import annotations

import copy
import pathlib
import sys

import pytest


PROJECT_ROOT = pathlib.Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


from src.fleetgraph.signals.signal_scoring_engine import score_signals


def _signal(
    *,
    company: str = "Atlas Build Co",
    signal_type: str = "project_distress",
    event_summary: str = "Project dispute reported",
    source: str = "duckduckgo_html://search-result",
    date_detected: str = "2026-03-28",
    raw_text: str = "Project dispute reported for contractor Atlas Build Co.",
) -> dict[str, object]:
    return {
        "company": company,
        "signal_type": signal_type,
        "event_summary": event_summary,
        "source": source,
        "date_detected": date_detected,
        "confidence_score": None,
        "priority": None,
        "raw_text": raw_text,
    }


def test_scoring_correctness() -> None:
    signals = [
        _signal(signal_type="litigation", event_summary="Lawsuit filed", raw_text="Construction contractor lawsuit filed"),
        _signal(company="Beacon Masonry", event_summary="Mechanics lien recorded", raw_text="Mechanics lien filed against contractor Beacon Masonry"),
        _signal(company="Civic Audit Group", signal_type="audit", event_summary="Audit notice posted", raw_text="Audit notice posted for contractor Civic Audit Group"),
        _signal(company="Delta Works", event_summary="Project delay dispute", raw_text="Project delay dispute involving developer Delta Works"),
        _signal(company="Echo Systems", event_summary="Review opened", raw_text="Routine review opened"),
    ]

    result = score_signals(signals)

    assert [signal["confidence_score"] for signal in result] == [5, 5, 4, 4, 2]


def test_scoring_excludes_generic_companies() -> None:
    signals = [
        _signal(company="unknown", signal_type="litigation", event_summary="Lawsuit filed", raw_text="Unknown lawsuit filed"),
        _signal(company="company", signal_type="audit", event_summary="Audit notice posted", raw_text="Audit notice posted for company"),
        _signal(company="Atlas Services Group", signal_type="audit", event_summary="Audit notice posted", raw_text="Audit notice posted for Atlas Services Group"),
    ]

    result = score_signals(signals)

    assert result == [
        {
            "company": "Atlas Services Group",
            "signal_type": "audit",
            "event_summary": "Audit notice posted",
            "source": "duckduckgo_html://search-result",
            "date_detected": "2026-03-28",
            "confidence_score": 4,
            "priority": None,
            "raw_text": "Audit notice posted for Atlas Services Group",
        }
    ]


def test_rss_news_signals_are_boosted() -> None:
    signals = [
        _signal(
            company="Beacon Holdings",
            signal_type="audit",
            source="rss_news://industry-feed",
            event_summary="Audit notice posted",
            raw_text="Audit notice posted for Beacon Holdings.",
        )
    ]

    result = score_signals(signals)

    assert result[0]["confidence_score"] == 5


def test_urgent_legal_events_score_at_top_band() -> None:
    signals = [
        _signal(company="Smith & Jones LLP", signal_type="litigation", event_summary="Document production ordered", raw_text="Document production ordered for outside counsel Smith & Jones LLP."),
        _signal(company="Gray Counsel PLLC", signal_type="litigation", event_summary="Subpoena issued", raw_text="Subpoena issued to outside counsel Gray Counsel PLLC."),
        _signal(company="Atlas Services Group", signal_type="government", event_summary="Investigation opened", raw_text="Investigation opened for Atlas Services Group."),
        _signal(company="North Harbor Developers", signal_type="government", event_summary="Default notice filed", raw_text="Default notice filed against North Harbor Developers."),
    ]

    result = score_signals(signals)

    assert [signal["confidence_score"] for signal in result] == [5, 5, 5, 5]


def test_input_validation() -> None:
    with pytest.raises(ValueError):
        score_signals([])


def test_output_contract_validation() -> None:
    result = score_signals([_signal()])

    assert result == [
        {
            "company": "Atlas Build Co",
            "signal_type": "project_distress",
            "event_summary": "Project dispute reported",
            "source": "duckduckgo_html://search-result",
            "date_detected": "2026-03-28",
            "confidence_score": 4,
            "priority": None,
            "raw_text": "Project dispute reported for contractor Atlas Build Co.",
        }
    ]


def test_deterministic_repeatability() -> None:
    signals = [_signal()]

    first = score_signals(signals)
    second = score_signals(signals)

    assert first == second


def test_no_input_mutation() -> None:
    signals = [_signal()]
    snapshot = copy.deepcopy(signals)

    _ = score_signals(signals)

    assert signals == snapshot
