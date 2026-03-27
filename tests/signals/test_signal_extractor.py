from __future__ import annotations

import copy

from fleetgraph.signals.signal_extractor import extract_signal


def _result_item(**overrides: str) -> dict[str, str]:
    result_item = {
        "title": "Acme Construction LLC sued in mechanics lien filing",
        "snippet": "Filed on 2026-03-26 after project payment dispute.",
        "url": "https://example.com/acme-lien",
    }
    result_item.update(overrides)
    return result_item


def test_signal_extraction_contract_validation() -> None:
    result = extract_signal(_result_item(), signal_type="litigation")

    assert result == {
        "company": "Acme Construction LLC",
        "signal_type": "litigation",
        "event_summary": "Acme Construction LLC sued in mechanics lien filing",
        "source": "https://example.com/acme-lien",
        "date_detected": "2026-03-26",
        "confidence_score": None,
        "priority": None,
        "raw_text": (
            "Acme Construction LLC sued in mechanics lien filing "
            "Filed on 2026-03-26 after project payment dispute."
        ),
    }


def test_signal_extraction_unresolved_company_fallback() -> None:
    result = extract_signal(
        _result_item(
            title="Mechanics lien filing posted",
            snippet="Filed on 2026-03-26 after payment dispute.",
        ),
        signal_type="litigation",
    )

    assert result["company"] == "unknown"


def test_signal_extraction_no_mutation() -> None:
    result_item = _result_item()
    snapshot = copy.deepcopy(result_item)

    _ = extract_signal(result_item, signal_type="litigation")

    assert result_item == snapshot
