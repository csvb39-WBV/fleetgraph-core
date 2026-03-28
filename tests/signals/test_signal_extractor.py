from __future__ import annotations

import copy

from fleetgraph.signals.signal_extractor import extract_signal, get_signal_rejection_reason


def _result_item(**overrides: str) -> dict[str, str]:
    result_item = {
        "title": "Acme Construction LLC sued in mechanics lien filing",
        "snippet": "Filed on 2026-03-26 after project payment dispute.",
        "url": "https://example.com/acme-lien",
        "source_provider": "duckduckgo_api",
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


def test_signal_extraction_detects_company_from_lawsuit_phrase() -> None:
    result = extract_signal(
        _result_item(
            title="Mechanics lien filed against Atlas Build Group",
            snippet="Complaint filed against Atlas Build Group on 2026-03-26.",
        ),
        signal_type="litigation",
    )

    assert result["company"] == "Atlas Build Group"


def test_signal_extraction_detects_company_from_audit_phrase() -> None:
    result = extract_signal(
        _result_item(
            title="Compliance review of Beacon Masonry Services opened",
            snippet="Audit of Beacon Masonry Services began on 2026-03-26.",
        ),
        signal_type="audit",
    )

    assert result["company"] == "Beacon Masonry Services"


def test_signal_extraction_detects_company_from_capitalized_sequence() -> None:
    result = extract_signal(
        _result_item(
            title="Atlas Build Group project delay reported",
            snippet="Atlas Build Group disclosed a delay on 2026-03-26.",
        ),
        signal_type="project_distress",
    )

    assert result["company"] == "Atlas Build Group"


def test_generic_company_signals_removed() -> None:
    signal = {
        "company": "company",
        "signal_type": "litigation",
        "event_summary": "lawsuit filed against company",
        "source": "https://example.com/company",
        "date_detected": "2026-03-26",
        "confidence_score": None,
        "priority": None,
        "raw_text": "lawsuit filed against company",
    }

    assert get_signal_rejection_reason(signal) == "generic_company"


def test_multi_word_generic_company_signals_removed() -> None:
    signal = {
        "company": "Real Estate Company",
        "signal_type": "litigation",
        "event_summary": "lawsuit filed against real estate company",
        "source": "https://example.com/real-estate-company",
        "date_detected": "2026-03-26",
        "confidence_score": None,
        "priority": None,
        "raw_text": "lawsuit filed against real estate company",
    }

    assert get_signal_rejection_reason(signal) == "generic_company"


def test_unknown_company_signals_removed() -> None:
    result = extract_signal(
        _result_item(
            title="Lawsuit filed after project delay",
            snippet="Filed on 2026-03-26 after payment dispute.",
        ),
        signal_type="litigation",
    )

    assert result["company"] == "unknown"
    assert get_signal_rejection_reason(result) == "generic_company"


def test_event_validation_rejects_non_event_titles() -> None:
    signal = {
        "company": "Atlas Build Group",
        "signal_type": "audit",
        "event_summary": "Atlas Build Group quarterly revenue update",
        "source": "https://example.com/revenue",
        "date_detected": "2026-03-26",
        "confidence_score": None,
        "priority": None,
        "raw_text": "Atlas Build Group quarterly revenue update",
    }

    assert get_signal_rejection_reason(signal) == "missing_event_term"


def test_signal_extraction_no_mutation() -> None:
    result_item = _result_item()
    snapshot = copy.deepcopy(result_item)

    _ = extract_signal(result_item, signal_type="litigation")

    assert result_item == snapshot
