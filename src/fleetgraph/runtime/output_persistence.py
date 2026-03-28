from __future__ import annotations

import json
from pathlib import Path


CSV_FILENAME = "daily_signals.csv"
MANIFEST_FILENAME = "daily_signals_manifest.json"
DEBUG_FILENAME = "daily_signals_debug.json"


def resolve_output_paths(output_directory: str | Path) -> dict[str, str]:
    output_directory_path = Path(output_directory).resolve()
    output_directory_path.mkdir(parents=True, exist_ok=True)
    return {
        "output_directory": str(output_directory_path),
        "csv_path": str(output_directory_path / CSV_FILENAME),
        "manifest_path": str(output_directory_path / MANIFEST_FILENAME),
        "debug_path": str(output_directory_path / DEBUG_FILENAME),
        "csv_filename": CSV_FILENAME,
        "manifest_filename": MANIFEST_FILENAME,
        "debug_filename": DEBUG_FILENAME,
    }


def write_manifest(manifest: dict[str, object], output_directory: str | Path) -> str:
    output_paths = resolve_output_paths(output_directory)
    manifest_path = Path(output_paths["manifest_path"])
    manifest_path.write_text(
        json.dumps(manifest, sort_keys=True, separators=(",", ":")),
        encoding="utf-8",
    )
    return str(manifest_path)


def write_debug_report(debug_report: dict[str, object], output_directory: str | Path) -> str:
    output_paths = resolve_output_paths(output_directory)
    debug_path = Path(output_paths["debug_path"])
    debug_path.write_text(
        json.dumps(debug_report, sort_keys=True, separators=(",", ":")),
        encoding="utf-8",
    )
    return str(debug_path)
