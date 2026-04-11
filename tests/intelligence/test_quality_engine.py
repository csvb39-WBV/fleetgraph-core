from __future__ import annotations

from copy import deepcopy
from datetime import date

from fleetgraph.prospects.prospect_assembly import normalize_enrichment_contacts
from fleetgraph.quality.contact_scoring import score_contact_quality
from fleetgraph.quality.deduplication import deduplicate_contacts
from fleetgraph.quality.email_validation import validate_contact_email
from fleetgraph.quality.filter import (
    build_high_quality_prospects,
    filter_enrichment_contacts,
    select_best_contacts,
)
from fleetgraph.quality.role_confidence import score_role_confidence


REFERENCE_LEADS = [
    {
        "company_id": "atlas-build-group",
        "company_name": "Atlas Build Group",
        "selected_bucket": "T1",
        "signal_type": "litigation_risk",
        "signal_detail": "Lawsuit filed against Atlas Build Group",
        "event_date": date(2026, 4, 9),
        "source_url": "https://example.com/atlas-lawsuit",
    },
    {
        "company_id": "beacon-masonry-services",
        "company_name": "Beacon Masonry Services",
        "selected_bucket": "T2",
        "signal_type": "payment_risk",
        "signal_detail": "Mechanics lien filed against Beacon Masonry Services",
        "event_date": date(2026, 3, 20),
        "source_url": "https://example.com/beacon-lien",
    },
]


def _contact(**overrides: object) -> dict[str, object]:
    record = {
        "company_id": "atlas-build-group",
        "name": "Alex Owner",
        "title": "Owner",
        "email": "alex.owner@example.com",
        "priority_rank": 1,
    }
    record.update(overrides)
    return record


def test_enhanced_email_validation_filters_invalid_role_based_and_disposable_emails() -> None:
    assert validate_contact_email("Alex.Owner@Example.com") == {
        "email": "alex.owner@example.com",
        "is_valid": True,
        "reason": "valid_email",
    }
    assert validate_contact_email("info@example.com")["reason"] == "role_based_email"
    assert validate_contact_email("contact@mailinator.com")["reason"] == "disposable_domain"
    assert validate_contact_email("bad-email")["reason"] == "invalid_format"


def test_contact_quality_scoring_ranks_high_quality_contacts_higher() -> None:
    high_quality = score_contact_quality(_contact())
    low_quality = score_contact_quality(_contact(title="Coordinator", email="test@mailinator.com"))

    assert high_quality["quality_score"] > low_quality["quality_score"]
    assert high_quality["confidence_level"] == "HIGH"
    assert low_quality["confidence_level"] == "LOW"


def test_role_confidence_classifies_expected_roles() -> None:
    assert score_role_confidence("CEO") == {"role_confidence": "HIGH", "role_score": 1.0}
    assert score_role_confidence("Controller") == {"role_confidence": "MEDIUM", "role_score": 0.7}
    assert score_role_confidence("Coordinator") == {"role_confidence": "LOW", "role_score": 0.35}


def test_deduplication_keeps_highest_quality_contact_deterministically() -> None:
    contacts = [
        {**_contact(email="alex@example.com", quality_score=0.91, role_score=1.0)},
        {**_contact(name="Alex Owner Duplicate", email="alex@example.com", quality_score=0.75, role_score=0.7)},
        {**_contact(name="Alex Owner", title="Owner", email="alex.alt@example.com", quality_score=0.89, role_score=1.0)},
    ]

    deduplicated = deduplicate_contacts(contacts)

    assert [contact["email"] for contact in deduplicated] == ["alex@example.com"]


def test_enrichment_quality_filter_rejects_low_quality_low_role_and_wrong_company_contacts() -> None:
    result = filter_enrichment_contacts(
        [
            _contact(email="alex.owner@example.com"),
            _contact(name="Info Team", title="Office Manager", email="info@example.com"),
            _contact(name="Mismatch", company_id="other-company", email="mismatch@example.com"),
            _contact(name="Low Role", title="Coordinator", email="coordinator@example.com"),
        ],
        company_id="atlas-build-group",
    )

    assert [contact["email"] for contact in result["filtered_contacts"]] == ["alex.owner@example.com"]
    assert result["filtered_contacts"][0]["quality_score"] > 0.0
    assert result["filtered_contacts"][0]["role_confidence"] == "HIGH"
    assert [contact["rejection_reason"] for contact in result["rejected_contacts"]] == [
        "inconsistent_company_mapping",
        "low_role_confidence",
        "role_based_email",
    ]


def test_wrapper_builds_high_quality_prospects_without_modifying_build2() -> None:
    result = build_high_quality_prospects(
        [REFERENCE_LEADS[0]],
        [
            _contact(name="Alex Owner", title="Owner", email="alex@example.com", priority_rank=1),
            _contact(name="Bella Finance", title="CFO", email="bella@example.com", priority_rank=2),
            _contact(name="Chris Director", title="Director", email="chris@example.com", priority_rank=4),
            _contact(name="Dana Ops", title="Operations Coordinator", email="dana@example.com", priority_rank=6),
        ],
        max_contacts_per_company=2,
    )

    assert [contact["email"] for contact in result["filtered_contacts"]] == [
        "alex@example.com",
        "bella@example.com",
    ]
    assert all("quality_score" in contact for contact in result["filtered_contacts"])
    assert all("role_confidence" in contact for contact in result["filtered_contacts"])
    assert [contact["email"] for contact in result["prospects"][0]["contacts"]] == [
        "alex@example.com",
        "bella@example.com",
    ]
    assert all("quality_score" not in contact for contact in result["prospects"][0]["contacts"])


def test_normalized_enrichment_contacts_stay_deterministic() -> None:
    contacts = [
        _contact(name="  Bella Finance  ", title="  CFO  ", email="  BELLA@EXAMPLE.COM  "),
        _contact(),
    ]

    first = normalize_enrichment_contacts(contacts)
    second = normalize_enrichment_contacts(contacts)

    assert first == second


def test_quality_wrapper_is_deterministic_and_non_mutating() -> None:
    leads = deepcopy(REFERENCE_LEADS)
    enrichment_records = [
        _contact(),
        _contact(name="Bella Finance", title="CFO", email="bella@example.com", priority_rank=2),
    ]
    baseline_leads = deepcopy(leads)
    baseline_enrichment = deepcopy(enrichment_records)

    first = build_high_quality_prospects(leads, enrichment_records)
    second = build_high_quality_prospects(leads, enrichment_records)

    assert first == second
    assert leads == baseline_leads
    assert enrichment_records == baseline_enrichment


def test_select_best_contacts_respects_cap() -> None:
    filtered_contacts = [
        filter_enrichment_contacts([_contact(name="Alex Owner", email="alex@example.com")], company_id="atlas-build-group")["filtered_contacts"][0],
        filter_enrichment_contacts([_contact(name="Bella Finance", title="CFO", email="bella@example.com", priority_rank=2)], company_id="atlas-build-group")["filtered_contacts"][0],
        filter_enrichment_contacts([_contact(name="Chris Director", title="Director", email="chris@example.com", priority_rank=4)], company_id="atlas-build-group")["filtered_contacts"][0],
    ]

    selected = select_best_contacts(filtered_contacts, max_contacts_per_company=2)

    assert [contact["email"] for contact in selected] == [
        "alex@example.com",
        "bella@example.com",
    ]
