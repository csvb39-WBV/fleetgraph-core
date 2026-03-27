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

    if signal["confidence_score"] is not None:
        raise ValueError("confidence_score must be None before scoring")
    if signal["priority"] is not None:
        raise ValueError("priority must be None before formatting")


def _contains_any(text: str, terms: tuple[str, ...]) -> bool:
    return any(term in text for term in terms)


def _score_signal(signal: dict[str, object]) -> int:
    signal_type = str(signal["signal_type"]).strip().lower()
    event_summary = str(signal["event_summary"]).strip().lower()
    raw_text = str(signal["raw_text"]).strip().lower()
    combined_text = " ".join((signal_type, event_summary, raw_text))

    if "mechanics lien" in combined_text:
        return 5
    if _contains_any(combined_text, ("lawsuit", "litigation")) and _contains_any(
        combined_text,
        ("contractor", "subcontractor", "builder", "construction"),
    ):
        return 5
    if "audit" in combined_text:
        return 4
    if "delay" in combined_text and _contains_any(
        combined_text,
        ("project", "construction", "contractor", "dispute"),
    ):
        return 4
    if "dispute" in combined_text:
        return 3
    if _contains_any(combined_text, ("investigation", "review", "hearing")):
        return 2
    return 1


def score_signals(signals: list[dict[str, object]]) -> list[dict[str, object]]:
    if not isinstance(signals, list) or len(signals) == 0:
        raise ValueError("signals must be a non-empty list")

    scored_signals: list[dict[str, object]] = []
    for signal in signals:
        _validate_signal(signal)
        confidence_score = _score_signal(signal)
        scored_signals.append(
            {
                "company": signal["company"],
                "signal_type": signal["signal_type"],
                "event_summary": signal["event_summary"],
                "source": signal["source"],
                "date_detected": signal["date_detected"],
                "confidence_score": confidence_score,
                "priority": None,
                "raw_text": signal["raw_text"],
            }
        )

    return scored_signals
