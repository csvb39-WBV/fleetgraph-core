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
    source: str = "city-record.example",
    date_detected: str = "2026-03-27",
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


def test_scoring_supports_legal_and_broader_entity_universe() -> None:
    signals = [
        _signal(
            company="Smith & Jones LLP",
            signal_type="litigation",
            event_summary="Document production order entered",
            raw_text="Document production order entered in litigation with outside counsel Smith & Jones LLP.",
        ),
        _signal(
            company="Atlas Services Group",
            signal_type="audit",
            event_summary="Audit notice posted",
            raw_text="Audit notice posted for Atlas Services Group.",
        ),
        _signal(
            company="Beacon Holdings",
            signal_type="government",
            event_summary="Internal investigation opened",
            raw_text="Internal investigation opened with legal department review at Beacon Holdings.",
        ),
        _signal(
            company="Gray Counsel PLLC",
            signal_type="litigation",
            event_summary="Subpoena issued",
            raw_text="Subpoena issued to outside counsel Gray Counsel PLLC for document collection.",
        ),
    ]

    result = score_signals(signals)

    assert [signal["confidence_score"] for signal in result] == [5, 4, 4, 5]


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
            "source": "city-record.example",
            "date_detected": "2026-03-27",
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
