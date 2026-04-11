from __future__ import annotations

from copy import deepcopy
from datetime import date

from fleetgraph.prospects.prospect_assembly import (
    assemble_prospects,
    normalize_enrichment_contacts,
)


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


def _enrichment_contact(**overrides: object) -> dict[str, object]:
    record = {
        "company_id": "atlas-build-group",
        "name": "Alex Owner",
        "title": "Owner",
        "email": "alex.owner@example.com",
    }
    record.update(overrides)
    return record


def test_contact_normalization_and_invalid_contact_filtering() -> None:
    contacts = [
        _enrichment_contact(),
        _enrichment_contact(name="  Bella Finance  ", title="  CFO  ", email="  BELLA@EXAMPLE.COM  "),
        _enrichment_contact(name="No Email", email=""),
        _enrichment_contact(name="Bad Email", email="not-an-email"),
        _enrichment_contact(company_id="", name="Missing Company"),
    ]

    result = normalize_enrichment_contacts(contacts)

    assert result == [
        {
            "company_id": "atlas-build-group",
            "name": "Alex Owner",
            "title": "Owner",
            "email": "alex.owner@example.com",
            "priority_rank": 1,
        },
        {
            "company_id": "atlas-build-group",
            "name": "Bella Finance",
            "title": "CFO",
            "email": "bella@example.com",
            "priority_rank": 2,
        },
    ]


def test_contact_ranking_order_follows_business_priority() -> None:
    prospects = assemble_prospects(
        [REFERENCE_LEADS[0]],
        [
            _enrichment_contact(name="Dana Ops", title="VP Operations", email="dana@example.com"),
            _enrichment_contact(name="Chris Finance", title="Controller", email="chris@example.com"),
            _enrichment_contact(name="Alex Owner", title="Owner", email="alex@example.com"),
            _enrichment_contact(name="Evan Director", title="Director of Business Development", email="evan@example.com"),
        ],
        max_contacts_per_company=4,
    )

    assert [contact["email"] for contact in prospects[0]["contacts"]] == [
        "alex@example.com",
        "chris@example.com",
        "dana@example.com",
        "evan@example.com",
    ]
    assert [contact["priority_rank"] for contact in prospects[0]["contacts"]] == [1, 2, 3, 4]


def test_duplicate_contact_suppression_is_deterministic_by_email() -> None:
    prospects = assemble_prospects(
        [REFERENCE_LEADS[0]],
        [
            _enrichment_contact(name="Alex Owner", title="Owner", email="alex@example.com"),
            _enrichment_contact(name="Alex Owner Duplicate", title="President", email="alex@example.com"),
            _enrichment_contact(name="Bella Finance", title="CFO", email="bella@example.com"),
        ],
    )

    assert prospects[0]["contacts"] == [
        {
            "name": "Alex Owner",
            "title": "Owner",
            "email": "alex@example.com",
            "priority_rank": 1,
        },
        {
            "name": "Bella Finance",
            "title": "CFO",
            "email": "bella@example.com",
            "priority_rank": 2,
        },
    ]


def test_max_contacts_cap_keeps_top_three_contacts() -> None:
    prospects = assemble_prospects(
        [REFERENCE_LEADS[0]],
        [
            _enrichment_contact(name="Alex Owner", title="Owner", email="alex@example.com"),
            _enrichment_contact(name="Bella Finance", title="CFO", email="bella@example.com"),
            _enrichment_contact(name="Chris Ops", title="Project Executive", email="chris@example.com"),
            _enrichment_contact(name="Dana Growth", title="Director of Business Development", email="dana@example.com"),
        ],
    )

    assert [contact["email"] for contact in prospects[0]["contacts"]] == [
        "alex@example.com",
        "bella@example.com",
        "chris@example.com",
    ]


def test_prospect_exclusion_when_no_valid_contacts_remain() -> None:
    prospects = assemble_prospects(
        REFERENCE_LEADS,
        [
            _enrichment_contact(company_id="atlas-build-group", name="Bad", title="Owner", email="bad-email"),
            _enrichment_contact(company_id="beacon-masonry-services", name=" ", title="President", email="beacon@example.com"),
        ],
    )

    assert prospects == []


def test_lead_fields_are_preserved_exactly_in_prospect_output() -> None:
    prospect = assemble_prospects(
        [REFERENCE_LEADS[0]],
        [_enrichment_contact()],
    )[0]

    assert prospect["company_id"] == REFERENCE_LEADS[0]["company_id"]
    assert prospect["company_name"] == REFERENCE_LEADS[0]["company_name"]
    assert prospect["selected_bucket"] == REFERENCE_LEADS[0]["selected_bucket"]
    assert prospect["signal_type"] == REFERENCE_LEADS[0]["signal_type"]
    assert prospect["signal_detail"] == REFERENCE_LEADS[0]["signal_detail"]
    assert prospect["event_date"] == REFERENCE_LEADS[0]["event_date"]
    assert prospect["source_url"] == REFERENCE_LEADS[0]["source_url"]


def test_prospect_id_is_deterministic_and_repeated_runs_match() -> None:
    enrichment_records = [
        _enrichment_contact(name="Alex Owner", title="Owner", email="alex@example.com"),
        _enrichment_contact(name="Bella Finance", title="CFO", email="bella@example.com"),
    ]
    first = assemble_prospects(REFERENCE_LEADS, enrichment_records)
    second = assemble_prospects(REFERENCE_LEADS, enrichment_records)

    assert first == second
    assert first[0]["prospect_id"] == second[0]["prospect_id"]


def test_assembly_is_non_mutating() -> None:
    leads = deepcopy(REFERENCE_LEADS)
    enrichment_records = [_enrichment_contact()]
    snapshot_leads = deepcopy(leads)
    snapshot_enrichment = deepcopy(enrichment_records)

    _ = assemble_prospects(leads, enrichment_records)

    assert leads == snapshot_leads
    assert enrichment_records == snapshot_enrichment
