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
        "query_count_executed": 7,
        "cache_hits": 2,
        "cache_misses": 5,
        "source_success_count": 5,
        "suppressed_result_count": 3,
        "raw_results_count": 7,
        "extracted_signal_count": 7,
        "deduplicated_signal_count": 7,
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


def test_manifest_new_fields_present_and_correct() -> None:
    result = build_run_manifest(_manifest())

    assert result["source_success_count"] == 5
    assert result["suppressed_result_count"] == 3
    assert result["raw_results_count"] == 7
    assert result["extracted_signal_count"] == 7
    assert result["deduplicated_signal_count"] == 7


def test_manifest_deterministic_output() -> None:
    first = build_run_manifest(_manifest())
    second = build_run_manifest(_manifest())

    assert first == second
