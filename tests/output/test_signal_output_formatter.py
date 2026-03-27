from __future__ import annotations

import copy
import pathlib
import sys

import pytest


PROJECT_ROOT = pathlib.Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


from src.fleetgraph.output.signal_output_formatter import format_signals


def _signal(confidence_score: int = 4) -> dict[str, object]:
    return {
        "company": "Atlas Build Co",
        "signal_type": "audit",
        "event_summary": "Audit notice posted",
        "source": "state-audit.example",
        "date_detected": "2026-03-27",
        "confidence_score": confidence_score,
        "priority": None,
        "raw_text": "Audit notice posted for Atlas Build Co.",
    }


def test_output_contract_validation() -> None:
    result = format_signals([_signal(4)])

    assert result == [
        {
            "company": "Atlas Build Co",
            "signal_type": "audit",
            "event_summary": "Audit notice posted",
            "source": "state-audit.example",
            "date_detected": "2026-03-27",
            "confidence_score": 4,
            "priority": "HIGH",
            "raw_text": "Audit notice posted for Atlas Build Co.",
            "recommended_action": "CALL NOW",
        }
    ]


def test_medium_priority_mapping() -> None:
    result = format_signals([_signal(3)])

    assert result[0]["priority"] == "MEDIUM"


def test_input_validation() -> None:
    with pytest.raises(ValueError):
        format_signals([])


def test_deterministic_repeatability() -> None:
    signals = [_signal(4)]

    first = format_signals(signals)
    second = format_signals(signals)

    assert first == second


def test_no_input_mutation() -> None:
    signals = [_signal(4)]
    snapshot = copy.deepcopy(signals)

    _ = format_signals(signals)

    assert signals == snapshot
