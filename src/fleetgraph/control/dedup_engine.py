
from __future__ import annotations

import re


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


def _normalize_text(value: str) -> str:
    collapsed = re.sub(r"[^a-z0-9]+", " ", value.lower())
    return " ".join(collapsed.split())


def _normalize_company(company: str) -> str:
    normalized = _normalize_text(company)
    normalized = re.sub(r"\b(inc|llc|corp|corporation|co|company|ltd|pllc)\b", "", normalized)
    return " ".join(normalized.split())


def _is_valid_signal(signal: object) -> bool:
    if not isinstance(signal, dict):
        return False
    if set(signal.keys()) != _REQUIRED_SIGNAL_KEYS:
        return False
    for key in ("company", "signal_type", "event_summary", "source", "date_detected", "raw_text"):
        if not isinstance(signal[key], str) or signal[key].strip() == "":
            return False
    return signal["confidence_score"] is None and signal["priority"] is None


def deduplicate_signals(signals: list[dict[str, object]]) -> list[dict[str, object]]:
    if not isinstance(signals, list):
        raise ValueError("invalid_signals")
    if not all(_is_valid_signal(signal) for signal in signals):
        raise ValueError("invalid_signal")

    seen_keys: set[tuple[str, str, str]] = set()
    deduplicated_signals: list[dict[str, object]] = []
    for signal in signals:
        dedup_key = (
            _normalize_company(signal["company"]),
            _normalize_text(signal["event_summary"]),
            _normalize_text(signal["source"]),
        )
        if dedup_key in seen_keys:
            continue
        seen_keys.add(dedup_key)
        deduplicated_signals.append(
            {
                "company": signal["company"],
                "signal_type": signal["signal_type"],
                "event_summary": signal["event_summary"],
                "source": signal["source"],
                "date_detected": signal["date_detected"],
                "confidence_score": None,
                "priority": None,
                "raw_text": signal["raw_text"],
            }
        )
    return deduplicated_signals