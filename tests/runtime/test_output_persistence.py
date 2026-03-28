from __future__ import annotations

import json
import pathlib
import sys


PROJECT_ROOT = pathlib.Path(__file__).resolve().parents[2]
SRC_ROOT = PROJECT_ROOT / "src"

if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))


from fleetgraph.runtime.output_persistence import resolve_output_paths, write_debug_report, write_manifest


def test_persistence_correctness(tmp_path: pathlib.Path) -> None:
    output_paths = resolve_output_paths(tmp_path)
    manifest_path = write_manifest({"status": "success", "run_date": "2026-03-27"}, tmp_path)
    debug_path = write_debug_report({"raw_results_count": 1, "queries": []}, tmp_path)

    assert output_paths["csv_filename"] == "daily_signals.csv"
    assert output_paths["manifest_filename"] == "daily_signals_manifest.json"
    assert output_paths["debug_filename"] == "daily_signals_debug.json"
    assert manifest_path == str((tmp_path / "daily_signals_manifest.json").resolve())
    assert debug_path == str((tmp_path / "daily_signals_debug.json").resolve())
    assert json.loads((tmp_path / "daily_signals_manifest.json").read_text(encoding="utf-8")) == {
        "run_date": "2026-03-27",
        "status": "success",
    }
    assert json.loads((tmp_path / "daily_signals_debug.json").read_text(encoding="utf-8")) == {
        "queries": [],
        "raw_results_count": 1,
    }
