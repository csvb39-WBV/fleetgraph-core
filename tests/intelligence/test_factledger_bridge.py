from __future__ import annotations

from copy import deepcopy

from fleetgraph.conversion.factledger_bridge import (
    build_conversion_entries,
    build_landing_reference,
    build_recommended_handoff,
    resolve_landing_path,
)


def _plan(
    *,
    draft_id: str,
    prospect_id: str,
    company_id: str,
    company_name: str,
    contact_email: str,
    contact_name: str,
    sequence_step: int,
    send_window: str,
) -> dict[str, object]:
    return {
        "draft_id": draft_id,
        "prospect_id": prospect_id,
        "company_id": company_id,
        "company_name": company_name,
        "contact_email": contact_email,
        "contact_name": contact_name,
        "sequence_step": sequence_step,
        "send_window": send_window,
    }


def _draft(
    *,
    prospect_id: str,
    company_id: str,
    company_name: str,
    contact_email: str,
    contact_name: str,
    selected_bucket: str,
    signal_type: str,
    signal_detail: str,
) -> dict[str, object]:
    return {
        "prospect_id": prospect_id,
        "company_id": company_id,
        "company_name": company_name,
        "contact": {
            "name": contact_name,
            "email": contact_email,
            "title": "Owner",
            "priority_rank": 1,
        },
        "selected_bucket": selected_bucket,
        "signal_type": signal_type,
        "signal_detail": signal_detail,
    }


def test_landing_path_mapping_is_correct() -> None:
    assert resolve_landing_path("litigation") == "/litigation-case"
    assert resolve_landing_path("payment") == "/payment-dispute"
    assert resolve_landing_path("enforcement") == "/compliance-review"
    assert resolve_landing_path("generic") == "/general-review"
    assert resolve_landing_path("LITIGATION") == "/litigation-case"


def test_factledger_flow_mapping_is_correct() -> None:
    assert build_recommended_handoff("litigation", "T1") == {
        "intake_type": "claims_evidence_reconstruction",
        "recommended_flow": "self_serve_claims_record_review",
        "source_context": "litigation_signal|T1",
    }
    assert build_recommended_handoff("payment", "T2")["recommended_flow"] == "self_serve_payment_record_review"
    assert build_recommended_handoff("enforcement", "T3A")["recommended_flow"] == "self_serve_compliance_record_review"
    assert build_recommended_handoff("generic", "T3B")["recommended_flow"] == "self_serve_general_record_review"


def test_attribution_and_conversion_ids_are_deterministic() -> None:
    execution_plan = [
        _plan(
            draft_id="draft-1",
            prospect_id="prospect:1",
            company_id="company-1",
            company_name="Company One",
            contact_email="alex@example.com",
            contact_name="Alex Owner",
            sequence_step=1,
            send_window="TUESDAY_0915",
        )
    ]
    message_drafts = [
        _draft(
            prospect_id="prospect:1",
            company_id="company-1",
            company_name="Company One",
            contact_email="alex@example.com",
            contact_name="Alex Owner",
            selected_bucket="T1",
            signal_type="litigation_risk",
            signal_detail="Lawsuit filed against Company One",
        )
    ]

    first = build_conversion_entries(execution_plan, message_drafts)
    second = build_conversion_entries(execution_plan, message_drafts)

    assert first == second
    assert first[0]["campaign_id"] == second[0]["campaign_id"]
    assert first[0]["conversion_entry_id"] == second[0]["conversion_entry_id"]


def test_field_preservation_and_contract_shape() -> None:
    entry = build_conversion_entries(
        [
            _plan(
                draft_id="draft-1",
                prospect_id="prospect:1",
                company_id="company-1",
                company_name="Company One",
                contact_email="alex@example.com",
                contact_name="Alex Owner",
                sequence_step=2,
                send_window="FRIDAY_0915",
            )
        ],
        [
            _draft(
                prospect_id="prospect:1",
                company_id="company-1",
                company_name="Company One",
                contact_email="alex@example.com",
                contact_name="Alex Owner",
                selected_bucket="T2",
                signal_type="payment_risk",
                signal_detail="Mechanics lien filed against Company One",
            )
        ],
    )[0]

    assert entry["prospect_id"] == "prospect:1"
    assert entry["company_id"] == "company-1"
    assert entry["company_name"] == "Company One"
    assert entry["contact_email"] == "alex@example.com"
    assert entry["contact_name"] == "Alex Owner"
    assert entry["selected_bucket"] == "T2"
    assert entry["signal_family"] == "payment"
    assert entry["entry_mode"] == "SELF_SERVE"
    assert entry["attribution_seed"] == {
        "draft_id": "draft-1",
        "sequence_step": 2,
        "send_window": "FRIDAY_0915",
    }


def test_optional_path_handling_supports_relative_paths() -> None:
    assert build_landing_reference("/litigation-case") == "/litigation-case"
    assert build_landing_reference("/litigation-case", base_path="/factledger") == "/factledger/litigation-case"


def test_non_mutation_and_ordering_are_preserved() -> None:
    execution_plan = [
        _plan(
            draft_id="draft-b",
            prospect_id="prospect:b",
            company_id="company-b",
            company_name="Company B",
            contact_email="b@example.com",
            contact_name="B Contact",
            sequence_step=1,
            send_window="TUESDAY_0915",
        ),
        _plan(
            draft_id="draft-a",
            prospect_id="prospect:a",
            company_id="company-a",
            company_name="Company A",
            contact_email="a@example.com",
            contact_name="A Contact",
            sequence_step=2,
            send_window="FRIDAY_0915",
        ),
    ]
    message_drafts = [
        _draft(
            prospect_id="prospect:b",
            company_id="company-b",
            company_name="Company B",
            contact_email="b@example.com",
            contact_name="B Contact",
            selected_bucket="T1",
            signal_type="enforcement_risk",
            signal_detail="OSHA enforcement review for Company B",
        ),
        _draft(
            prospect_id="prospect:a",
            company_id="company-a",
            company_name="Company A",
            contact_email="a@example.com",
            contact_name="A Contact",
            selected_bucket="T3A",
            signal_type="audit_risk",
            signal_detail="Documentation review for Company A",
        ),
    ]
    baseline_plan = deepcopy(execution_plan)
    baseline_drafts = deepcopy(message_drafts)

    entries = build_conversion_entries(execution_plan, message_drafts)

    assert [entry["prospect_id"] for entry in entries] == ["prospect:b", "prospect:a"]
    assert execution_plan == baseline_plan
    assert message_drafts == baseline_drafts


def test_blocked_state_records_are_not_routed() -> None:
    entries = build_conversion_entries(
        [
            _plan(
                draft_id="draft-1",
                prospect_id="prospect:1",
                company_id="company-1",
                company_name="Company One",
                contact_email="alex@example.com",
                contact_name="Alex Owner",
                sequence_step=1,
                send_window="TUESDAY_0915",
            )
        ],
        [
            _draft(
                prospect_id="prospect:1",
                company_id="company-1",
                company_name="Company One",
                contact_email="alex@example.com",
                contact_name="Alex Owner",
                selected_bucket="T1",
                signal_type="litigation_risk",
                signal_detail="Lawsuit filed against Company One",
            )
        ],
        state_records=[
            {
                "draft_id": "draft-1",
                "status": "SUPPRESSED",
            }
        ],
    )

    assert entries == []
