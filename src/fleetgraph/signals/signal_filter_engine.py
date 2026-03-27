from __future__ import annotations


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


def _validate_signal(signal: object) -> None:
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

    confidence_score = signal["confidence_score"]
    if not isinstance(confidence_score, int) or isinstance(confidence_score, bool):
        raise ValueError("confidence_score must be an int")
    if confidence_score < 1 or confidence_score > 5:
        raise ValueError("confidence_score must be between 1 and 5")
    if signal["priority"] is not None:
        raise ValueError("priority must be None before formatting")


def filter_signals(signals: list[dict[str, object]]) -> dict[str, list[dict[str, object]]]:
    if not isinstance(signals, list) or len(signals) == 0:
        raise ValueError("signals must be a non-empty list")

    retained_signals: list[dict[str, object]] = []
    primary_signals: list[dict[str, object]] = []
    for signal in signals:
        _validate_signal(signal)
        if signal["confidence_score"] >= 3:
            retained_signals.append(
                {
                    "company": signal["company"],
                    "signal_type": signal["signal_type"],
                    "event_summary": signal["event_summary"],
                    "source": signal["source"],
                    "date_detected": signal["date_detected"],
                    "confidence_score": signal["confidence_score"],
                    "priority": None,
                    "raw_text": signal["raw_text"],
                }
            )
        if signal["confidence_score"] >= 4:
            primary_signals.append(
                {
                    "company": signal["company"],
                    "signal_type": signal["signal_type"],
                    "event_summary": signal["event_summary"],
                    "source": signal["source"],
                    "date_detected": signal["date_detected"],
                    "confidence_score": signal["confidence_score"],
                    "priority": None,
                    "raw_text": signal["raw_text"],
                }
            )

    return {
        "retained_signals": retained_signals,
        "primary_signals": primary_signals,
    }
