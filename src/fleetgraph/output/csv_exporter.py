from __future__ import annotations

import csv
from pathlib import Path


_REQUIRED_FORMATTED_SIGNAL_KEYS = (
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


def _is_non_empty_string(value: object) -> bool:
    return isinstance(value, str) and value.strip() != ""


def _validate_formatted_signal(signal: object) -> None:
    if not isinstance(signal, dict):
        raise ValueError("formatted signal must be a dict")
    if tuple(signal.keys()) != _REQUIRED_FORMATTED_SIGNAL_KEYS:
        raise ValueError("formatted signal must contain the exact output structure")

    for field_name in (
        "company",
        "signal_type",
        "event_summary",
        "source",
        "date_detected",
        "priority",
        "raw_text",
        "recommended_action",
    ):
        if not _is_non_empty_string(signal[field_name]):
            raise ValueError(f"{field_name} must be a non-empty string")

    confidence_score = signal["confidence_score"]
    if not isinstance(confidence_score, int) or isinstance(confidence_score, bool):
        raise ValueError("confidence_score must be an int")


def export_signals_to_csv(
    formatted_signals: list[dict[str, object]],
    output_directory: str | Path,
    output_filename: str = "daily_signals.csv",
) -> str:
    if not isinstance(formatted_signals, list) or len(formatted_signals) == 0:
        raise ValueError("formatted_signals must be a non-empty list")
    if not _is_non_empty_string(output_filename):
        raise ValueError("output_filename must be a non-empty string")

    for formatted_signal in formatted_signals:
        _validate_formatted_signal(formatted_signal)

    output_path = Path(output_directory).resolve() / output_filename
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with output_path.open("w", encoding="utf-8", newline="") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=list(_REQUIRED_FORMATTED_SIGNAL_KEYS))
        writer.writeheader()
        for formatted_signal in formatted_signals:
            writer.writerow(formatted_signal)

    return str(output_path)
