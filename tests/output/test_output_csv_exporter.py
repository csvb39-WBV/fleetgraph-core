from __future__ import annotations

import csv
import pathlib
import sys

import pytest


PROJECT_ROOT = pathlib.Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


from src.fleetgraph.output.csv_exporter import export_signals_to_csv


def _formatted_signal() -> dict[str, object]:
    return {
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


def test_csv_integrity(tmp_path: pathlib.Path) -> None:
    output_path = export_signals_to_csv([_formatted_signal()], tmp_path)

    with pathlib.Path(output_path).open("r", encoding="utf-8", newline="") as csv_file:
        rows = list(csv.DictReader(csv_file))

    assert pathlib.Path(output_path).name == "daily_signals.csv"
    assert rows == [
        {
            "company": "Atlas Build Co",
            "signal_type": "audit",
            "event_summary": "Audit notice posted",
            "source": "state-audit.example",
            "date_detected": "2026-03-27",
            "confidence_score": "4",
            "priority": "HIGH",
            "raw_text": "Audit notice posted for Atlas Build Co.",
            "recommended_action": "CALL NOW",
        }
    ]


def test_input_validation() -> None:
    with pytest.raises(ValueError):
        export_signals_to_csv([], pathlib.Path.cwd())
