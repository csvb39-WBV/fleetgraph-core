from __future__ import annotations

import csv
from pathlib import Path


_CSV_HEADER = (
    "company",
    "signal",
    "priority",
    "event_id",
    "type",
)


def _get_filename(payload: dict[str, object]) -> str:
    request_id = payload.get("request_id")
    if isinstance(request_id, str) and request_id != "":
        return f"output_{request_id}.csv"
    return "output_export.csv"


def _get_results(payload: dict[str, object]) -> list[object]:
    results = payload.get("results")
    if not isinstance(results, list):
        raise ValueError("payload must contain results.")
    return results


def _get_row_value(item: object, field_name: str) -> object:
    if not isinstance(item, dict):
        return ""
    return item.get(field_name, "")


def export_csv_output(payload: dict[str, object], output_dir: str) -> dict[str, object]:
    if not isinstance(payload, dict):
        raise ValueError("payload must contain results.")

    results = _get_results(payload)

    directory = Path(output_dir)
    directory.mkdir(parents=True, exist_ok=True)

    filename = _get_filename(payload)
    output_path = directory / filename

    with output_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(_CSV_HEADER)
        for item in results:
            writer.writerow(
                [
                    _get_row_value(item, "company_name"),
                    _get_row_value(item, "signal_type"),
                    _get_row_value(item, "priority_score"),
                    _get_row_value(item, "source_event_id"),
                    _get_row_value(item, "event_type"),
                ]
            )

    return {
        "ok": True,
        "path": str(output_path),
        "filename": filename,
    }
