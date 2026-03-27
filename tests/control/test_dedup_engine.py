from __future__ import annotations

from fleetgraph.control.dedup_engine import deduplicate_signals


def _signal(**overrides: object) -> dict[str, object]:
    signal: dict[str, object] = {
        "company": "Acme Construction LLC",
        "signal_type": "litigation",
        "event_summary": "Acme Construction LLC faces mechanics lien filing",
        "source": "https://example.com/acme-lien",
        "date_detected": "2026-03-26",
        "confidence_score": None,
        "priority": None,
        "raw_text": "Acme Construction LLC faces mechanics lien filing 2026-03-26",
    }
    signal.update(overrides)
    return signal


def test_deduplication_correctness() -> None:
    signals = [
        _signal(),
        _signal(company="Acme Construction", event_summary="Acme Construction LLC faces mechanics lien filing"),
        _signal(company="Beacon Builders", event_summary="Beacon Builders audit findings released", source="https://example.com/beacon-audit"),
    ]

    assert deduplicate_signals(signals) == [
        _signal(),
        _signal(company="Beacon Builders", event_summary="Beacon Builders audit findings released", source="https://example.com/beacon-audit"),
    ]


def test_deduplication_preserves_deterministic_ordering() -> None:
    signals = [
        _signal(company="Zeta Contractors", event_summary="Zeta Contractors dispute emerges", source="https://example.com/zeta"),
        _signal(company="Acme Construction LLC", source="https://example.com/acme"),
        _signal(company="Acme Construction", source="https://example.com/acme"),
    ]

    result = deduplicate_signals(signals)

    assert [signal["company"] for signal in result] == [
        "Zeta Contractors",
        "Acme Construction LLC",
    ]
