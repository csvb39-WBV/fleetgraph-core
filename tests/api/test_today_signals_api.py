from __future__ import annotations

import csv
import json
import pathlib
import sys


PROJECT_ROOT = pathlib.Path(__file__).resolve().parents[2]
SRC_ROOT = PROJECT_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))


from fleetgraph.api.today_signals_api import build_today_signals_response


_SIGNAL_FIELDNAMES = [
    "company",
    "signal_type",
    "event_summary",
    "source",
    "date_detected",
    "confidence_score",
    "priority",
    "raw_text",
    "recommended_action",
]


def _write_outputs(output_directory: pathlib.Path) -> None:
    csv_path = output_directory / "daily_signals.csv"
    with csv_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=_SIGNAL_FIELDNAMES)
        writer.writeheader()
        writer.writerow(
            {
                "company": "Smith & Jones LLP",
                "signal_type": "litigation",
                "event_summary": "Document production ordered",
                "source": "court.example",
                "date_detected": "2026-03-27",
                "confidence_score": 5,
                "priority": "HIGH",
                "raw_text": "Document production ordered for outside counsel Smith & Jones LLP.",
                "recommended_action": "CALL NOW",
            }
        )
        writer.writerow(
            {
                "company": "Beacon Holdings",
                "signal_type": "audit",
                "event_summary": "Audit notice posted",
                "source": "audit.example",
                "date_detected": "2026-03-27",
                "confidence_score": 4,
                "priority": "HIGH",
                "raw_text": "Audit notice posted for Beacon Holdings.",
                "recommended_action": "CALL NOW",
            }
        )
    manifest = {
        "run_date": "2026-03-27",
        "query_count_executed": 8,
        "cache_hits": 3,
        "cache_misses": 5,
        "retained_signal_count": 2,
        "exported_signal_count": 2,
        "csv_path": str(csv_path),
        "status": "success",
        "error_code": None,
    }
    with (output_directory / "daily_signals_manifest.json").open("w", encoding="utf-8") as handle:
        json.dump(manifest, handle, indent=2, sort_keys=True)


def test_api_response_validation_with_broader_names(tmp_path: pathlib.Path) -> None:
    _write_outputs(tmp_path)

    result = build_today_signals_response(tmp_path)

    assert result["ok"] is True
    assert result["today_signals"] == {
        "top_signals": [
            {
                "company": "Smith & Jones LLP",
                "signal_type": "litigation",
                "event_summary": "Document production ordered",
                "source": "court.example",
                "date_detected": "2026-03-27",
                "confidence_score": 5,
                "priority": "HIGH",
                "raw_text": "Document production ordered for outside counsel Smith & Jones LLP.",
                "recommended_action": "CALL NOW",
            },
            {
                "company": "Beacon Holdings",
                "signal_type": "audit",
                "event_summary": "Audit notice posted",
                "source": "audit.example",
                "date_detected": "2026-03-27",
                "confidence_score": 4,
                "priority": "HIGH",
                "raw_text": "Audit notice posted for Beacon Holdings.",
                "recommended_action": "CALL NOW",
            },
        ],
        "retained_count": 2,
        "exported_count": 2,
        "run_date": "2026-03-27",
        "status": "success",
        "csv_path": str((tmp_path / "daily_signals.csv").resolve()),
        "summary": {
            "count_by_signal_type": {
                "audit": 1,
                "government": 0,
                "litigation": 1,
                "project_distress": 0,
            },
            "count_by_priority": {
                "HIGH": 2,
                "MEDIUM": 0,
            },
            "total_exported_count": 2,
            "top_companies": [
                "Beacon Holdings",
                "Smith & Jones LLP",
            ],
        },
    }
    assert result["error_code"] is None


def test_missing_output_handling(tmp_path: pathlib.Path) -> None:
    result = build_today_signals_response(tmp_path)

    assert result == {
        "ok": False,
        "today_signals": None,
        "error_code": "missing_manifest",
    }
