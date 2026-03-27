from __future__ import annotations

from pathlib import Path

from src.fleetgraph.output.csv_exporter import export_signals_to_csv
from src.fleetgraph.output.signal_output_formatter import format_signals
from src.fleetgraph.signals.signal_filter_engine import filter_signals
from src.fleetgraph.signals.signal_scoring_engine import score_signals


_REQUIRED_SIGNAL_KEYS = {
    "company",
    "signal_type",
    "event_summary",
    "source",
    "date_detected",
    "confidence_score",
    "priority",
    "raw_text",
}


def _is_non_empty_string(value: object) -> bool:
    return isinstance(value, str) and value.strip() != ""


def _validate_unscored_signal(signal: object) -> None:
    if not isinstance(signal, dict):
        raise ValueError("signal must be a dict")
    if set(signal.keys()) != _REQUIRED_SIGNAL_KEYS:
        raise ValueError("signal must contain the exact locked signal contract")

    for field_name in (
        "company",
        "signal_type",
        "event_summary",
        "source",
        "date_detected",
        "raw_text",
    ):
        if not _is_non_empty_string(signal[field_name]):
            raise ValueError(f"{field_name} must be a non-empty string")

    if signal["confidence_score"] is not None:
        raise ValueError("acquisition signals must have confidence_score=None")
    if signal["priority"] is not None:
        raise ValueError("acquisition signals must have priority=None")


def run_signal_pipeline(
    acquisition_request: dict,
    acquisition_runner,
    output_directory: str | Path,
    output_filename: str = "daily_signals.csv",
) -> dict[str, object]:
    if not isinstance(acquisition_request, dict):
        raise ValueError("acquisition_request must be a dict")
    if not callable(acquisition_runner):
        raise ValueError("acquisition_runner must be callable")
    if not _is_non_empty_string(output_filename):
        raise ValueError("output_filename must be a non-empty string")

    acquisition_result = acquisition_runner(acquisition_request)
    if not isinstance(acquisition_result, list) or len(acquisition_result) == 0:
        raise ValueError("acquisition_runner must return a non-empty list")
    for signal in acquisition_result:
        _validate_unscored_signal(signal)

    scored_signals = score_signals(acquisition_result)
    filtered_signals = filter_signals(scored_signals)
    primary_signals = filtered_signals["primary_signals"]
    formatted_signals = format_signals(primary_signals)
    csv_path = export_signals_to_csv(formatted_signals, output_directory, output_filename)

    return {
        "primary_signals": formatted_signals,
        "retained_signal_count": len(filtered_signals["retained_signals"]),
        "exported_signal_count": len(formatted_signals),
        "csv_path": csv_path,
    }
