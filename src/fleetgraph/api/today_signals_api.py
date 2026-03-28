from __future__ import annotations

import csv
import json
from pathlib import Path

from fleetgraph.api.signal_summary_surface import build_signal_summary


_REQUIRED_MANIFEST_KEYS = {
    "run_date",
    "query_count_executed",
    "cache_hits",
    "cache_misses",
    "retained_signal_count",
    "exported_signal_count",
    "csv_path",
    "status",
    "error_code",
}
_REQUIRED_SIGNAL_KEYS = (
    "company",
    "signal_type",
    "event_summary",
    "source",
    "date_detected",
    "confidence_score",
    "priority",
    "raw_text",
    "recommended_action",
)
_ALLOWED_STATUS = {"success", "failed"}


def _invalid_response(error_code: str) -> dict[str, object]:
    return {
        "ok": False,
        "today_signals": None,
        "error_code": error_code,
    }


def _is_non_empty_string(value: object) -> bool:
    return isinstance(value, str) and value.strip() != ""


def _load_manifest(manifest_path: Path) -> dict[str, object] | None:
    if not manifest_path.exists():
        return None
    with manifest_path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def _validate_manifest(manifest: object) -> bool:
    if not isinstance(manifest, dict):
        return False
    if set(manifest.keys()) != _REQUIRED_MANIFEST_KEYS:
        return False
    if not _is_non_empty_string(manifest["run_date"]):
        return False
    if not _is_non_empty_string(manifest["csv_path"]):
        return False
    if manifest["status"] not in _ALLOWED_STATUS:
        return False
    if manifest["error_code"] is not None and not _is_non_empty_string(manifest["error_code"]):
        return False
    for field_name in (
        "query_count_executed",
        "cache_hits",
        "cache_misses",
        "retained_signal_count",
        "exported_signal_count",
    ):
        value = manifest[field_name]
        if not isinstance(value, int) or isinstance(value, bool) or value < 0:
            return False
    return True


def _load_signals(csv_path: Path) -> list[dict[str, object]] | None:
    if not csv_path.exists():
        return None
    with csv_path.open("r", encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))
    signals: list[dict[str, object]] = []
    for row in rows:
        if tuple(row.keys()) != _REQUIRED_SIGNAL_KEYS:
            return None
        if not _is_non_empty_string(row["company"]):
            return None
        confidence_score_text = row["confidence_score"]
        if not confidence_score_text.isdigit():
            return None
        signals.append(
            {
                "company": row["company"],
                "signal_type": row["signal_type"],
                "event_summary": row["event_summary"],
                "source": row["source"],
                "date_detected": row["date_detected"],
                "confidence_score": int(confidence_score_text),
                "priority": row["priority"],
                "raw_text": row["raw_text"],
                "recommended_action": row["recommended_action"],
            }
        )
    return signals


def build_today_signals_response(output_directory: str | Path) -> dict[str, object]:
    output_root = Path(output_directory).resolve()
    manifest_path = output_root / "daily_signals_manifest.json"
    manifest = _load_manifest(manifest_path)
    if manifest is None:
        return _invalid_response("missing_manifest")
    if not _validate_manifest(manifest):
        return _invalid_response("invalid_manifest")

    csv_path = Path(str(manifest["csv_path"]))
    if not csv_path.is_absolute():
        csv_path = (output_root / csv_path).resolve()

    signals = _load_signals(csv_path)
    if signals is None:
        return _invalid_response("missing_or_invalid_csv")

    if len(signals) != manifest["exported_signal_count"]:
        return _invalid_response("signal_count_mismatch")

    top_signals = signals[:5]
    summary = build_signal_summary(signals)

    return {
        "ok": True,
        "today_signals": {
            "top_signals": top_signals,
            "retained_count": manifest["retained_signal_count"],
            "exported_count": manifest["exported_signal_count"],
            "run_date": manifest["run_date"],
            "status": manifest["status"],
            "csv_path": str(csv_path),
            "summary": summary,
        },
        "error_code": None,
    }
