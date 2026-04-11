from __future__ import annotations

from copy import deepcopy
from datetime import date

from fleetgraph.messaging.message_optimization import (
    generate_optimized_message_drafts,
    get_template_variant_registry,
    select_message_variant,
)


def _prospect(
    *,
    prospect_id: str = "prospect:atlas:001",
    company_id: str = "atlas-build-group",
    company_name: str = "Atlas Build Group",
    selected_bucket: str = "T1",
    signal_type: str = "litigation_risk",
    signal_detail: str = "Lawsuit filed against Atlas Build Group",
    contacts: list[dict[str, object]] | None = None,
) -> dict[str, object]:
    return {
        "prospect_id": prospect_id,
        "company_id": company_id,
        "company_name": company_name,
        "selected_bucket": selected_bucket,
        "signal_type": signal_type,
        "signal_detail": signal_detail,
        "event_date": date(2026, 4, 9),
        "source_url": "https://example.com/source",
        "contacts": contacts or [
            {
                "name": "Alex Owner",
                "title": "Owner",
                "email": "alex@example.com",
                "priority_rank": 1,
            }
        ],
    }


def test_variant_registry_covers_representative_family_bucket_combinations() -> None:
    registry = get_template_variant_registry()

    assert set(registry["litigation"]["T1"].keys()) == {"v1", "v2"}
    assert set(registry["payment"]["T2"].keys()) == {"v1", "v2"}
    assert set(registry["enforcement"]["T3A"].keys()) == {"v1", "v2"}
    assert set(registry["generic"]["T3C"].keys()) == {"v1", "v2"}


def test_deterministic_variant_selection_is_stable() -> None:
    first = select_message_variant(
        signal_family="litigation",
        selected_bucket="T1",
        prospect_id="prospect:atlas:001",
        contact_email="alex@example.com",
    )
    second = select_message_variant(
        signal_family="litigation",
        selected_bucket="T1",
        prospect_id="prospect:atlas:001",
        contact_email="alex@example.com",
    )

    assert first == second
    assert first["template_variant_id"] == "v1"
    assert first["selection_mode"] == "default"


def test_variant_override_uses_requested_variant() -> None:
    selected = select_message_variant(
        signal_family="payment",
        selected_bucket="T2",
        prospect_id="prospect:atlas:001",
        contact_email="alex@example.com",
        variant_overrides={"prospect:atlas:001|alex@example.com": "v2"},
    )

    assert selected["template_variant_id"] == "v2"
    assert selected["selection_mode"] == "override"


def test_field_preservation_and_optimization_metadata_are_present() -> None:
    optimized_draft = generate_optimized_message_drafts([_prospect()])[0]

    assert optimized_draft["prospect_id"] == "prospect:atlas:001"
    assert optimized_draft["company_id"] == "atlas-build-group"
    assert optimized_draft["company_name"] == "Atlas Build Group"
    assert optimized_draft["selected_bucket"] == "T1"
    assert optimized_draft["signal_type"] == "litigation_risk"
    assert optimized_draft["signal_detail"] == "Lawsuit filed against Atlas Build Group"
    assert optimized_draft["contact"]["email"] == "alex@example.com"
    assert optimized_draft["template_family"] == "litigation"
    assert optimized_draft["template_variant_id"] == "v1"
    assert optimized_draft["message_optimization_id"].startswith("message-opt:")
    assert optimized_draft["optimization_metadata"] == {
        "variant_group": "litigation_T1_baseline",
        "selection_mode": "default",
        "copy_style": "balanced",
    }


def test_no_placeholder_regression_and_self_serve_cta_preservation() -> None:
    default_draft = generate_optimized_message_drafts([_prospect()])[0]
    override_draft = generate_optimized_message_drafts(
        [_prospect(signal_type="payment_risk", signal_detail="Mechanics lien filed against Atlas Build Group")],
        variant_overrides={"prospect:atlas:001|alex@example.com": "v2"},
    )[0]

    for draft in (default_draft, override_draft):
        assert "{{" not in draft["subject"]
        assert "}}" not in draft["subject"]
        assert "{{" not in draft["body"]
        assert "}}" not in draft["body"]
        lowered_body = draft["body"].lower()
        assert "self-serve" in lowered_body
        assert "workflow" in lowered_body
        assert "meeting" not in lowered_body
        assert "demo" not in lowered_body
        assert "call" not in lowered_body


def test_deterministic_generation_is_stable() -> None:
    prospects = [
        _prospect(),
        _prospect(
            prospect_id="prospect:beacon:001",
            company_id="beacon-masonry-services",
            company_name="Beacon Masonry Services",
            selected_bucket="T2",
            signal_type="payment_risk",
            signal_detail="Mechanics lien filed against Beacon Masonry Services",
            contacts=[
                {
                    "name": "Bella Finance",
                    "title": "CFO",
                    "email": "bella@example.com",
                    "priority_rank": 2,
                }
            ],
        ),
    ]

    first = generate_optimized_message_drafts(prospects, campaign_key="campaign-alpha")
    second = generate_optimized_message_drafts(prospects, campaign_key="campaign-alpha")

    assert first == second


def test_non_mutation_and_ordering_are_preserved() -> None:
    prospects = [
        _prospect(
            prospect_id="prospect:one",
            company_id="company-one",
            company_name="Company One",
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
            ],
        ),
        _prospect(
            prospect_id="prospect:two",
            company_id="company-two",
            company_name="Company Two",
            selected_bucket="T3A",
            signal_type="audit_risk",
            signal_detail="Documentation review for Company Two",
            contacts=[
                {
                    "name": "Chris Ops",
                    "title": "Project Executive",
                    "email": "chris@example.com",
                    "priority_rank": 3,
                }
            ],
        ),
    ]
    baseline = deepcopy(prospects)

    drafts = generate_optimized_message_drafts(prospects)

    assert [draft["contact"]["email"] for draft in drafts] == [
        "bella@example.com",
        "alex@example.com",
        "chris@example.com",
    ]
    assert prospects == baseline
