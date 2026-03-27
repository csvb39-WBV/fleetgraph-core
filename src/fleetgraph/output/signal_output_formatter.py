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
    if confidence_score < 3 or confidence_score > 5:
        raise ValueError("confidence_score must be between 3 and 5 for formatting")
    if signal["priority"] is not None:
        raise ValueError("priority must be None before formatting")


def _priority_for_score(confidence_score: int) -> str:
    if confidence_score >= 4:
        return "HIGH"
    return "MEDIUM"


def format_signals(signals: list[dict[str, object]]) -> list[dict[str, object]]:
    if not isinstance(signals, list) or len(signals) == 0:
        raise ValueError("signals must be a non-empty list")

    formatted_signals: list[dict[str, object]] = []
    for signal in signals:
        _validate_signal(signal)
        formatted_signal = {
            "company": signal["company"],
            "signal_type": signal["signal_type"],
            "event_summary": signal["event_summary"],
            "source": signal["source"],
            "date_detected": signal["date_detected"],
            "confidence_score": signal["confidence_score"],
            "priority": _priority_for_score(signal["confidence_score"]),
            "raw_text": signal["raw_text"],
            "recommended_action": "CALL NOW",
        }
        if any(not _is_non_empty_string(formatted_signal[field_name]) for field_name in (
            "company",
            "signal_type",
            "event_summary",
            "source",
            "date_detected",
            "priority",
            "raw_text",
            "recommended_action",
        )):
            raise ValueError("formatted signal contains an empty field")
        formatted_signals.append(formatted_signal)

    return formatted_signals
