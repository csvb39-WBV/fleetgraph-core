from __future__ import annotations


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
_ALLOWED_SIGNAL_TYPES = ("audit", "government", "litigation", "project_distress")
_ALLOWED_PRIORITIES = ("HIGH", "MEDIUM")
_ALLOWED_SOURCE_TYPES = ("rss_news", "duckduckgo_html", "duckduckgo_api")


def _is_non_empty_string(value: object) -> bool:
    return isinstance(value, str) and value.strip() != ""


def _source_type_key(source: str) -> str:
    normalized_source = source.strip().lower()
    if "rss" in normalized_source or "news" in normalized_source:
        return "rss_news"
    if "duckduckgo" in normalized_source and "api" in normalized_source:
        return "duckduckgo_api"
    return "duckduckgo_html"


def _validate_signal(signal: object) -> None:
    if not isinstance(signal, dict):
        raise ValueError("signal must be a dict")
    if tuple(signal.keys()) != _REQUIRED_SIGNAL_KEYS:
        raise ValueError("signal must contain the exact formatted signal contract")

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

    if signal["signal_type"] not in _ALLOWED_SIGNAL_TYPES:
        raise ValueError("signal_type must be a supported signal type")
    if signal["priority"] not in _ALLOWED_PRIORITIES:
        raise ValueError("priority must be HIGH or MEDIUM")
    if _source_type_key(str(signal["source"])) not in _ALLOWED_SOURCE_TYPES:
        raise ValueError("source must map to a supported source type")

    confidence_score = signal["confidence_score"]
    if not isinstance(confidence_score, int) or isinstance(confidence_score, bool):
        raise ValueError("confidence_score must be an int")
    if confidence_score < 3 or confidence_score > 5:
        raise ValueError("confidence_score must be between 3 and 5")


def build_signal_summary(signals: list[dict[str, object]]) -> dict[str, object]:
    if not isinstance(signals, list):
        raise ValueError("signals must be a list")
    if len(signals) == 0:
        raise ValueError("signals must be a non-empty list")

    validated_signals: list[dict[str, object]] = []
    for signal in signals:
        _validate_signal(signal)
        validated_signals.append({key: signal[key] for key in _REQUIRED_SIGNAL_KEYS})

    signal_type_counts = {signal_type: 0 for signal_type in _ALLOWED_SIGNAL_TYPES}
    priority_counts = {priority: 0 for priority in _ALLOWED_PRIORITIES}
    source_counts = {source_type: 0 for source_type in _ALLOWED_SOURCE_TYPES}
    for signal in validated_signals:
        signal_type_counts[str(signal["signal_type"])] += 1
        priority_counts[str(signal["priority"])] += 1
        source_counts[_source_type_key(str(signal["source"]))] += 1

    top_companies = sorted({str(signal["company"]).strip() for signal in validated_signals})

    return {
        "count_by_signal_type": signal_type_counts,
        "count_by_priority": priority_counts,
        "count_by_source": source_counts,
        "total_exported_count": len(validated_signals),
        "top_companies": top_companies,
    }
