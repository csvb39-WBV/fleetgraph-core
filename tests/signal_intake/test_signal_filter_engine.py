from __future__ import annotations

import copy
import pathlib
import sys

import pytest


PROJECT_ROOT = pathlib.Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


from src.fleetgraph.signals.signal_filter_engine import filter_signals


def _signal(confidence_score: int) -> dict[str, object]:
    return {
        "company": f"Company {confidence_score}",
        "signal_type": "audit",
        "event_summary": f"Audit event {confidence_score}",
        "source": "state-audit.example",
        "date_detected": "2026-03-27",
        "confidence_score": confidence_score,
        "priority": None,
        "raw_text": f"Audit event {confidence_score} recorded.",
    }


def test_filter_enforcement() -> None:
    result = filter_signals([_signal(2), _signal(3), _signal(4), _signal(5)])

    assert [signal["confidence_score"] for signal in result["retained_signals"]] == [3, 4, 5]
    assert [signal["confidence_score"] for signal in result["primary_signals"]] == [4, 5]


def test_input_validation() -> None:
    with pytest.raises(ValueError):
        filter_signals([])


def test_output_contract_validation() -> None:
    result = filter_signals([_signal(4)])

    assert result == {
        "retained_signals": [
            {
                "company": "Company 4",
                "signal_type": "audit",
                "event_summary": "Audit event 4",
                "source": "state-audit.example",
                "date_detected": "2026-03-27",
                "confidence_score": 4,
                "priority": None,
                "raw_text": "Audit event 4 recorded.",
            }
        ],
        "primary_signals": [
            {
                "company": "Company 4",
                "signal_type": "audit",
                "event_summary": "Audit event 4",
                "source": "state-audit.example",
                "date_detected": "2026-03-27",
                "confidence_score": 4,
                "priority": None,
                "raw_text": "Audit event 4 recorded.",
            }
        ],
    }


def test_deterministic_repeatability() -> None:
    signals = [_signal(4), _signal(5)]

    first = filter_signals(signals)
    second = filter_signals(signals)

    assert first == second


def test_no_input_mutation() -> None:
    signals = [_signal(4), _signal(5)]
    snapshot = copy.deepcopy(signals)

    _ = filter_signals(signals)

    assert signals == snapshot
