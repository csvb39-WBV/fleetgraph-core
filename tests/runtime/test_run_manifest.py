from __future__ import annotations

import pathlib
import sys

import pytest


PROJECT_ROOT = pathlib.Path(__file__).resolve().parents[2]
SRC_ROOT = PROJECT_ROOT / "src"

if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))


from fleetgraph.runtime.run_manifest import build_run_manifest


def _manifest(**overrides: object) -> dict[str, object]:
    manifest: dict[str, object] = {
        "run_date": "2026-03-27",
        "query_count_executed": 14,
        "cache_hits": 2,
        "cache_misses": 12,
        "source_success_count": 12,
        "suppressed_result_count": 3,
        "filtered_out_generic_company_count": 2,
        "fallback_triggered": False,
        "raw_results_count": 14,
        "extracted_signal_count": 10,
        "deduplicated_signal_count": 10,
        "retained_signal_count": 5,
        "exported_signal_count": 3,
        "csv_path": "C:/tmp/daily_signals.csv",
        "status": "success",
        "error_code": None,
    }
    manifest.update(overrides)
    return manifest


def test_manifest_correctness() -> None:
    result = build_run_manifest(_manifest())

    assert result == _manifest()


def test_manifest_invalid_rejection() -> None:
    with pytest.raises(ValueError, match="invalid_status"):
        build_run_manifest(_manifest(status="partial"))

    with pytest.raises(ValueError, match="invalid_query_count_executed"):
        build_run_manifest(_manifest(query_count_executed=True))

    with pytest.raises(ValueError, match="invalid_fallback_triggered"):
        build_run_manifest(_manifest(fallback_triggered="yes"))


def test_manifest_new_fields_present_and_correct() -> None:
    result = build_run_manifest(_manifest())

    assert result["source_success_count"] == 12
    assert result["suppressed_result_count"] == 3
    assert result["filtered_out_generic_company_count"] == 2
    assert result["fallback_triggered"] is False
    assert result["raw_results_count"] == 14
    assert result["extracted_signal_count"] == 10
    assert result["deduplicated_signal_count"] == 10


def test_manifest_deterministic_output() -> None:
    first = build_run_manifest(_manifest())
    second = build_run_manifest(_manifest())

    assert first == second
