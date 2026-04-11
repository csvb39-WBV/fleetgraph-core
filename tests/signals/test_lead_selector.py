from __future__ import annotations

from datetime import date

from fleetgraph.signals.lead_selector import select_phased_leads
from fleetgraph.signals.signal_normalizer import normalize_signal_batch, normalize_signal_record


REFERENCE_DATE = date(2026, 4, 11)


def _raw_signal(
    *,
    company: str,
    signal_type: str,
    event_summary: str,
    date_detected: str,
    source: str = "https://example.com/source",
) -> dict[str, object]:
    return {
        "company": company,
        "signal_type": signal_type,
        "event_summary": event_summary,
        "source": source,
        "date_detected": date_detected,
        "confidence_score": None,
        "priority": None,
        "raw_text": event_summary,
    }


def _normalized_signal(
    *,
    company_id: str,
    company_name: str,
    signal_type: str,
    signal_detail: str,
    event_date: date,
    source_url: str | None = "https://example.com/source",
) -> dict[str, object]:
    return {
        "company_id": company_id,
        "company_name": company_name,
        "signal_type": signal_type,
        "signal_detail": signal_detail,
        "event_date": event_date,
        "source_url": source_url,
    }


def test_signal_normalization_accepts_raw_and_canonical_inputs_and_skips_unusable_records() -> None:
    canonical_signal = _normalized_signal(
        company_id="atlas-build-group",
        company_name="Atlas Build Group",
        signal_type="litigation_risk",
        signal_detail="Lawsuit filed against Atlas Build Group",
        event_date=date(2026, 4, 5),
        source_url="https://example.com/atlas-lawsuit",
    )
    raw_signal = _raw_signal(
        company="Beacon Masonry Services",
        signal_type="payment_risk",
        event_summary="Mechanics lien filed against Beacon Masonry Services",
        date_detected="2026-04-04",
        source="https://example.com/beacon-lien",
    )
    unusable_signal = {
        "primary_entity": "company_node_1",
        "signal_type": "payment_risk",
        "source_event_id": "event-1",
    }

    normalized_batch = normalize_signal_batch([canonical_signal, raw_signal, unusable_signal])

    assert normalize_signal_record(canonical_signal) == canonical_signal
    assert normalized_batch == [
        canonical_signal,
        {
            "company_id": "beacon-masonry-services--signal--834660e7",
            "company_name": "Beacon Masonry Services",
            "signal_type": "payment_risk",
            "signal_detail": "Mechanics lien filed against Beacon Masonry Services",
            "event_date": date(2026, 4, 4),
            "source_url": "https://example.com/beacon-lien",
        },
    ]


def test_phased_selection_expands_in_strict_tier_order() -> None:
    signals = [
        _normalized_signal(
            company_id="company-t3c",
            company_name="Company T3C",
            signal_type="payment_risk",
            signal_detail="Bond claim filed by Company T3C",
            event_date=date(2023, 5, 15),
        ),
        _normalized_signal(
            company_id="company-t3b",
            company_name="Company T3B",
            signal_type="enforcement_risk",
            signal_detail="Regulatory enforcement action against Company T3B",
            event_date=date(2025, 1, 1),
        ),
        _normalized_signal(
            company_id="company-t2",
            company_name="Company T2",
            signal_type="payment_risk",
            signal_detail="Mechanics lien filed by Company T2",
            event_date=date(2026, 3, 15),
        ),
        _normalized_signal(
            company_id="company-t1",
            company_name="Company T1",
            signal_type="litigation_risk",
            signal_detail="Lawsuit filed against Company T1",
            event_date=date(2026, 4, 8),
        ),
        _normalized_signal(
            company_id="company-t3a",
            company_name="Company T3A",
            signal_type="audit_risk",
            signal_detail="Audit review of Company T3A",
            event_date=date(2025, 8, 1),
        ),
    ]

    result = select_phased_leads(
        signals,
        reference_date=REFERENCE_DATE,
        max_batch_size=4,
    )

    assert [lead["company_id"] for lead in result["selected_leads"]] == [
        "company-t1",
        "company-t2",
        "company-t3a",
        "company-t3b",
    ]
    assert [lead["selected_bucket"] for lead in result["selected_leads"]] == [
        "T1",
        "T2",
        "T3A",
        "T3B",
    ]
    assert result["icp_fallback"] == {
        "eligible": False,
        "next_phase": "ICP_FALLBACK",
        "reason": "batch_full",
    }


def test_selector_dedupes_company_by_deterministic_precedence() -> None:
    signals = [
        _normalized_signal(
            company_id="atlas-build-group",
            company_name="Atlas Build Group",
            signal_type="payment_risk",
            signal_detail="Mechanics lien filed against Atlas Build Group",
            event_date=date(2026, 4, 5),
        ),
        _normalized_signal(
            company_id="atlas-build-group",
            company_name="Atlas Build Group",
            signal_type="litigation_risk",
            signal_detail="Lawsuit filed against Atlas Build Group",
            event_date=date(2026, 4, 6),
        ),
        _normalized_signal(
            company_id="beacon-masonry-services",
            company_name="Beacon Masonry Services",
            signal_type="payment_risk",
            signal_detail="Mechanics lien filed against Beacon Masonry Services",
            event_date=date(2026, 4, 4),
        ),
    ]

    result = select_phased_leads(
        signals,
        reference_date=REFERENCE_DATE,
    )

    assert [lead["company_id"] for lead in result["selected_leads"]] == [
        "atlas-build-group",
        "beacon-masonry-services",
    ]
    assert result["selected_leads"][0]["signal_type"] == "payment_risk"
    assert result["selected_leads"][0]["signal_detail"] == "Mechanics lien filed against Atlas Build Group"


def test_selector_excludes_companies_in_cooldown_window() -> None:
    signals = [
        _normalized_signal(
            company_id="atlas-build-group",
            company_name="Atlas Build Group",
            signal_type="litigation_risk",
            signal_detail="Lawsuit filed against Atlas Build Group",
            event_date=date(2026, 4, 8),
        ),
        _normalized_signal(
            company_id="beacon-masonry-services",
            company_name="Beacon Masonry Services",
            signal_type="payment_risk",
            signal_detail="Mechanics lien filed against Beacon Masonry Services",
            event_date=date(2026, 4, 7),
        ),
    ]

    result = select_phased_leads(
        signals,
        reference_date=REFERENCE_DATE,
        cooldown_days=45,
        prior_contact_records=[
            {
                "company_id": "atlas-build-group",
                "contacted_at": date(2026, 3, 20),
            }
        ],
    )

    assert [lead["company_id"] for lead in result["selected_leads"]] == [
        "beacon-masonry-services",
    ]
    assert result["skipped_company_ids_by_reason"]["cooldown"] == [
        "atlas-build-group",
    ]


def test_older_tier_gating_blocks_weak_signals_in_t3b_and_t3c() -> None:
    signals = [
        _normalized_signal(
            company_id="weak-t3b-company",
            company_name="Weak T3B Company",
            signal_type="audit_risk",
            signal_detail="Audit review of Weak T3B Company",
            event_date=date(2025, 2, 1),
        ),
        _normalized_signal(
            company_id="strong-t3b-company",
            company_name="Strong T3B Company",
            signal_type="enforcement_risk",
            signal_detail="Regulatory enforcement action against Strong T3B Company",
            event_date=date(2025, 2, 1),
        ),
        _normalized_signal(
            company_id="weak-t3c-company",
            company_name="Weak T3C Company",
            signal_type="project_risk",
            signal_detail="Project delay reported for Weak T3C Company",
            event_date=date(2023, 6, 1),
        ),
        _normalized_signal(
            company_id="strong-t3c-company",
            company_name="Strong T3C Company",
            signal_type="payment_risk",
            signal_detail="Bond claim filed by Strong T3C Company",
            event_date=date(2023, 6, 1),
        ),
    ]

    result = select_phased_leads(
        signals,
        reference_date=REFERENCE_DATE,
        max_batch_size=10,
    )

    assert [lead["company_id"] for lead in result["selected_leads"]] == [
        "strong-t3b-company",
        "strong-t3c-company",
    ]
    assert result["skipped_company_ids_by_reason"]["historical_gate"] == [
        "weak-t3b-company",
        "weak-t3c-company",
    ]
    assert result["icp_fallback"]["eligible"] is True


def test_repeated_identical_runs_produce_identical_output() -> None:
    signals = [
        _normalized_signal(
            company_id="atlas-build-group",
            company_name="Atlas Build Group",
            signal_type="payment_risk",
            signal_detail="Mechanics lien filed against Atlas Build Group",
            event_date=date(2026, 4, 4),
        ),
        _normalized_signal(
            company_id="beacon-masonry-services",
            company_name="Beacon Masonry Services",
            signal_type="litigation_risk",
            signal_detail="Lawsuit filed against Beacon Masonry Services",
            event_date=date(2026, 4, 4),
        ),
    ]

    first = select_phased_leads(
        signals,
        reference_date=REFERENCE_DATE,
        max_batch_size=10,
    )
    second = select_phased_leads(
        signals,
        reference_date=REFERENCE_DATE,
        max_batch_size=10,
    )

    assert first == second
