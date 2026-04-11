from __future__ import annotations

from copy import deepcopy
from datetime import date

from fleetgraph.messaging.message_engine import (
    generate_message_drafts,
    resolve_signal_family,
)


BASE_CONTACTS = [
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


def _prospect(
    *,
    prospect_id: str = "prospect:atlas:001",
    company_id: str = "atlas-build-group",
    company_name: str = "Atlas Build Group",
    selected_bucket: str = "T1",
    signal_type: str = "litigation_risk",
    signal_detail: str = "Lawsuit filed against Atlas Build Group",
    contacts: list[dict[str, object]] | None = None,
    project_name: str | None = None,
) -> dict[str, object]:
    record: dict[str, object] = {
        "prospect_id": prospect_id,
        "company_id": company_id,
        "company_name": company_name,
        "selected_bucket": selected_bucket,
        "signal_type": signal_type,
        "signal_detail": signal_detail,
        "event_date": date(2026, 4, 9),
        "source_url": "https://example.com/atlas-lawsuit",
        "contacts": deepcopy(BASE_CONTACTS if contacts is None else contacts),
    }
    if project_name is not None:
        record["project_name"] = project_name
    return record


def test_signal_family_mapping_is_deterministic() -> None:
    assert resolve_signal_family(signal_type="litigation_risk", signal_detail="Lawsuit filed in court") == "litigation"
    assert resolve_signal_family(signal_type="payment_risk", signal_detail="Mechanics lien filed") == "payment"
    assert resolve_signal_family(signal_type="safety_event", signal_detail="OSHA enforcement review") == "enforcement"
    assert resolve_signal_family(signal_type="audit_risk", signal_detail="Documentation review") == "generic"


def test_tier_framing_changes_across_tiers() -> None:
    buckets = ["T1", "T2", "T3A", "T3B", "T3C"]
    drafts = [generate_message_drafts([_prospect(selected_bucket=bucket, prospect_id=f"prospect:{bucket}")])[0] for bucket in buckets]

    assert "immediate pressure" in drafts[0]["body"]
    assert "recent issue" in drafts[1]["body"]
    assert "prior issue" in drafts[2]["body"]
    assert "broader pattern" in drafts[3]["body"]
    assert "structural risk" in drafts[4]["body"]


def test_placeholder_resolution_and_field_preservation() -> None:
    draft = generate_message_drafts([_prospect()])[0]

    assert draft["prospect_id"] == "prospect:atlas:001"
    assert draft["company_id"] == "atlas-build-group"
    assert draft["company_name"] == "Atlas Build Group"
    assert draft["selected_bucket"] == "T1"
    assert draft["signal_type"] == "litigation_risk"
    assert draft["signal_detail"] == "Lawsuit filed against Atlas Build Group"
    assert draft["contact"] == BASE_CONTACTS[0]
    assert "{{" not in draft["subject"]
    assert "}}" not in draft["subject"]
    assert "{{" not in draft["body"]
    assert "}}" not in draft["body"]


def test_optional_field_omission_stays_natural() -> None:
    without_project = generate_message_drafts([_prospect(project_name=None)])[0]
    with_project = generate_message_drafts([_prospect(project_name="Harbor Expansion")])[0]

    assert "If Harbor Expansion is still a useful internal reference point" in with_project["body"]
    assert "still a useful internal reference point" not in without_project["body"]


def test_subjects_vary_by_family_without_fabricated_urgency() -> None:
    litigation = generate_message_drafts([_prospect(signal_type="litigation_risk", signal_detail="Court docket filed")])[0]
    payment = generate_message_drafts([_prospect(signal_type="payment_risk", signal_detail="Mechanics lien filed")])[0]
    enforcement = generate_message_drafts([_prospect(signal_type="enforcement_risk", signal_detail="OSHA review opened")])[0]
    generic = generate_message_drafts([_prospect(signal_type="audit_risk", signal_detail="Documentation review")])[0]

    assert litigation["subject"] == "Quick question for Atlas Build Group"
    assert payment["subject"] == "Question about project documentation at Atlas Build Group"
    assert enforcement["subject"] == "Question regarding documentation workflows at Atlas Build Group"
    assert generic["subject"] == "Quick question for Atlas Build Group"
    assert all("urgent" not in draft["subject"].lower() for draft in (litigation, payment, enforcement, generic))


def test_multi_contact_order_follows_prospect_contact_order() -> None:
    drafts = generate_message_drafts([
        _prospect(
            contacts=[
                {
                    "name": "Bella Finance",
                    "title": "CFO",
                    "email": "bella@example.com",
                    "priority_rank": 2,
                },
                {
                    "name": "Alex Owner",
                    "title": "Owner",
                    "email": "alex@example.com",
                    "priority_rank": 1,
                },
            ]
        )
    ])

    assert [draft["contact"]["email"] for draft in drafts] == [
        "bella@example.com",
        "alex@example.com",
    ]


def test_cta_is_self_serve_aligned_and_not_meeting_oriented() -> None:
    draft = generate_message_drafts([_prospect()])[0]
    body = draft["body"].lower()

    assert "self-serve workflow" in body
    assert "evaluate it directly" in body
    assert "conversation" not in body
    assert "meeting" not in body
    assert "demo" not in body
    assert "call" not in body


def test_repeated_identical_generation_is_deterministic() -> None:
    prospects = [
        _prospect(prospect_id="prospect:atlas:001"),
        _prospect(
            prospect_id="prospect:beacon:001",
            company_id="beacon-masonry-services",
            company_name="Beacon Masonry Services",
            selected_bucket="T2",
            signal_type="payment_risk",
            signal_detail="Mechanics lien filed against Beacon Masonry Services",
        ),
    ]

    first = generate_message_drafts(prospects)
    second = generate_message_drafts(prospects)

    assert first == second


def test_generation_is_non_mutating() -> None:
    prospects = [_prospect()]
    snapshot = deepcopy(prospects)

    _ = generate_message_drafts(prospects)

    assert prospects == snapshot
