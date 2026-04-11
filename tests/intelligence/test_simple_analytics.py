from __future__ import annotations

from copy import deepcopy

from fleetgraph.analytics.simple_analytics import build_simple_analytics_report, normalize_analytics_inputs


OPTIMIZED_MESSAGE_DRAFTS = [
    {
        "prospect_id": "prospect:atlas:001",
        "company_id": "atlas-build-group",
        "company_name": "Atlas Build Group",
        "contact": {
            "name": "Alex Owner",
            "title": "Owner",
            "email": "alex@example.com",
            "priority_rank": 1,
        },
        "selected_bucket": "T1",
        "signal_type": "litigation_risk",
        "signal_detail": "Lawsuit filed against Atlas Build Group",
        "subject": "Quick question for Atlas Build Group",
        "body": "Hi Alex Owner,\n\nTest body.",
        "template_family": "litigation",
        "template_variant_id": "v1",
        "message_optimization_id": "message-opt:1111",
        "optimization_metadata": {
            "variant_group": "litigation_T1_baseline",
            "selection_mode": "default",
            "copy_style": "balanced",
        },
    },
    {
        "prospect_id": "prospect:beacon:001",
        "company_id": "beacon-masonry-services",
        "company_name": "Beacon Masonry Services",
        "contact": {
            "name": "Bella Finance",
            "title": "CFO",
            "email": "bella@example.com",
            "priority_rank": 2,
        },
        "selected_bucket": "T2",
        "signal_type": "payment_risk",
        "signal_detail": "Mechanics lien filed against Beacon Masonry Services",
        "subject": "Question after a recent payment issue at Beacon Masonry Services",
        "body": "Hi Bella Finance,\n\nTest body.",
        "template_family": "payment",
        "template_variant_id": "v2",
        "message_optimization_id": "message-opt:2222",
        "optimization_metadata": {
            "variant_group": "payment_T2_concise",
            "selection_mode": "override",
            "copy_style": "concise",
        },
    },
    {
        "prospect_id": "prospect:cobalt:001",
        "company_id": "cobalt-concrete",
        "company_name": "Cobalt Concrete",
        "contact": {
            "name": "Casey Finance",
            "title": "CFO",
            "email": "casey@example.com",
            "priority_rank": 2,
        },
        "selected_bucket": "T2",
        "signal_type": "payment_risk",
        "signal_detail": "Mechanics lien filed against Cobalt Concrete",
        "subject": "Question after a recent payment issue at Cobalt Concrete",
        "body": "Hi Casey Finance,\n\nTest body.",
        "template_family": "payment",
        "template_variant_id": "v2",
        "message_optimization_id": "message-opt:3333",
        "optimization_metadata": {
            "variant_group": "payment_T2_concise",
            "selection_mode": "override",
            "copy_style": "concise",
        },
    },
]

EXECUTION_PLAN_RECORDS = [
    {
        "draft_id": "draft-1",
        "prospect_id": "prospect:atlas:001",
        "company_id": "atlas-build-group",
        "company_name": "Atlas Build Group",
        "contact_email": "alex@example.com",
        "contact_name": "Alex Owner",
        "sequence_step": 1,
        "send_window": "TUESDAY_0915",
        "scheduled_send_at": "2026-04-14T09:15:00",
        "subject": "Quick question for Atlas Build Group",
        "body": "Hi Alex Owner,\n\nTest body.",
    },
    {
        "draft_id": "draft-2",
        "prospect_id": "prospect:beacon:001",
        "company_id": "beacon-masonry-services",
        "company_name": "Beacon Masonry Services",
        "contact_email": "bella@example.com",
        "contact_name": "Bella Finance",
        "sequence_step": 1,
        "send_window": "TUESDAY_0915",
        "scheduled_send_at": "2026-04-14T09:15:00",
        "subject": "Question after a recent payment issue at Beacon Masonry Services",
        "body": "Hi Bella Finance,\n\nTest body.",
    },
    {
        "draft_id": "draft-3",
        "prospect_id": "prospect:cobalt:001",
        "company_id": "cobalt-concrete",
        "company_name": "Cobalt Concrete",
        "contact_email": "casey@example.com",
        "contact_name": "Casey Finance",
        "sequence_step": 1,
        "send_window": "FRIDAY_0915",
        "scheduled_send_at": "2026-04-17T09:15:00",
        "subject": "Question after a recent payment issue at Cobalt Concrete",
        "body": "Hi Casey Finance,\n\nTest body.",
    },
]

STATE_RECORDS = [
    {
        "draft_id": "draft-1",
        "prospect_id": "prospect:atlas:001",
        "company_id": "atlas-build-group",
        "contact_email": "alex@example.com",
        "sequence_step": 1,
        "status": "REPLIED",
        "last_event_at": "2026-04-14T10:00:00",
        "next_scheduled_at": None,
    },
    {
        "draft_id": "draft-2",
        "prospect_id": "prospect:beacon:001",
        "company_id": "beacon-masonry-services",
        "contact_email": "bella@example.com",
        "sequence_step": 1,
        "status": "SENT",
        "last_event_at": "2026-04-14T09:20:00",
        "next_scheduled_at": None,
    },
    {
        "draft_id": "draft-3",
        "prospect_id": "prospect:cobalt:001",
        "company_id": "cobalt-concrete",
        "contact_email": "casey@example.com",
        "sequence_step": 1,
        "status": "SUPPRESSED",
        "last_event_at": "2026-04-16T09:20:00",
        "next_scheduled_at": None,
    },
]

CONVERSION_SIGNAL_RECORDS = [
    {
        "prospect_id": "prospect:atlas:001",
        "conversion_flag": True,
        "reason": "reply_detected",
    },
    {
        "prospect_id": "prospect:beacon:001",
        "conversion_flag": False,
        "reason": "no_conversion_signal",
    },
    {
        "prospect_id": "prospect:cobalt:001",
        "conversion_flag": False,
        "reason": "no_conversion_signal",
    },
]

CONVERSION_ENTRIES = [
    {
        "conversion_entry_id": "conversion:1111",
        "prospect_id": "prospect:atlas:001",
        "company_id": "atlas-build-group",
        "company_name": "Atlas Build Group",
        "contact_email": "alex@example.com",
        "contact_name": "Alex Owner",
        "signal_family": "litigation",
        "selected_bucket": "T1",
        "campaign_id": "campaign:test-alpha",
        "landing_path": "/litigation-case",
        "entry_mode": "SELF_SERVE",
        "attribution_seed": {
            "draft_id": "draft-1",
            "sequence_step": 1,
            "send_window": "TUESDAY_0915",
        },
        "factledger_handoff": {
            "intake_type": "claims_evidence_reconstruction",
            "recommended_flow": "self_serve_claims_record_review",
            "source_context": "litigation_signal|T1",
        },
    },
    {
        "conversion_entry_id": "conversion:2222",
        "prospect_id": "prospect:beacon:001",
        "company_id": "beacon-masonry-services",
        "company_name": "Beacon Masonry Services",
        "contact_email": "bella@example.com",
        "contact_name": "Bella Finance",
        "signal_family": "payment",
        "selected_bucket": "T2",
        "campaign_id": "campaign:test-alpha",
        "landing_path": "/payment-dispute",
        "entry_mode": "SELF_SERVE",
        "attribution_seed": {
            "draft_id": "draft-2",
            "sequence_step": 1,
            "send_window": "TUESDAY_0915",
        },
        "factledger_handoff": {
            "intake_type": "payment_support_review",
            "recommended_flow": "self_serve_payment_record_review",
            "source_context": "payment_signal|T2",
        },
    },
    {
        "conversion_entry_id": "conversion:3333",
        "prospect_id": "prospect:cobalt:001",
        "company_id": "cobalt-concrete",
        "company_name": "Cobalt Concrete",
        "contact_email": "casey@example.com",
        "contact_name": "Casey Finance",
        "signal_family": "payment",
        "selected_bucket": "T2",
        "campaign_id": "campaign:test-alpha",
        "landing_path": "/payment-dispute",
        "entry_mode": "SELF_SERVE",
        "attribution_seed": {
            "draft_id": "draft-3",
            "sequence_step": 1,
            "send_window": "FRIDAY_0915",
        },
        "factledger_handoff": {
            "intake_type": "payment_support_review",
            "recommended_flow": "self_serve_payment_record_review",
            "source_context": "payment_signal|T2",
        },
    },
]


def test_campaign_summary_rolls_up_counts_and_rates_correctly() -> None:
    report = build_simple_analytics_report(
        optimized_message_drafts=OPTIMIZED_MESSAGE_DRAFTS,
        execution_plan_records=EXECUTION_PLAN_RECORDS,
        state_records=STATE_RECORDS,
        conversion_signal_records=CONVERSION_SIGNAL_RECORDS,
        conversion_entries=CONVERSION_ENTRIES,
    )

    assert report["campaign_summary"] == {
        "campaign_id": "campaign:test-alpha",
        "total_planned": 3,
        "total_sent": 2,
        "total_replied": 1,
        "total_bounced": 0,
        "total_unsubscribed": 0,
        "total_suppressed": 1,
        "total_converted": 1,
        "reply_rate": 0.5,
        "conversion_rate": 0.5,
    }


def test_variant_summary_groups_and_rates_correctly() -> None:
    report = build_simple_analytics_report(
        optimized_message_drafts=OPTIMIZED_MESSAGE_DRAFTS,
        execution_plan_records=EXECUTION_PLAN_RECORDS,
        state_records=STATE_RECORDS,
        conversion_signal_records=CONVERSION_SIGNAL_RECORDS,
        conversion_entries=CONVERSION_ENTRIES,
    )

    assert report["variant_summary"] == [
        {
            "template_variant_id": "v1",
            "template_family": "litigation",
            "selected_bucket": "T1",
            "planned": 1,
            "sent": 1,
            "replied": 1,
            "converted": 1,
            "reply_rate": 1.0,
            "conversion_rate": 1.0,
        },
        {
            "template_variant_id": "v2",
            "template_family": "payment",
            "selected_bucket": "T2",
            "planned": 2,
            "sent": 1,
            "replied": 0,
            "converted": 0,
            "reply_rate": 0.0,
            "conversion_rate": 0.0,
        },
    ]


def test_signal_summary_groups_by_family_and_bucket() -> None:
    report = build_simple_analytics_report(
        optimized_message_drafts=OPTIMIZED_MESSAGE_DRAFTS,
        execution_plan_records=EXECUTION_PLAN_RECORDS,
        state_records=STATE_RECORDS,
        conversion_signal_records=CONVERSION_SIGNAL_RECORDS,
        conversion_entries=CONVERSION_ENTRIES,
    )

    assert report["signal_summary"] == [
        {
            "template_family": "litigation",
            "selected_bucket": "T1",
            "planned": 1,
            "sent": 1,
            "replied": 1,
            "bounced": 0,
            "converted": 1,
        },
        {
            "template_family": "payment",
            "selected_bucket": "T2",
            "planned": 2,
            "sent": 1,
            "replied": 0,
            "bounced": 0,
            "converted": 0,
        },
    ]


def test_role_summary_is_deterministic_and_counts_correctly() -> None:
    report = build_simple_analytics_report(
        optimized_message_drafts=OPTIMIZED_MESSAGE_DRAFTS,
        execution_plan_records=EXECUTION_PLAN_RECORDS,
        state_records=STATE_RECORDS,
        conversion_signal_records=CONVERSION_SIGNAL_RECORDS,
        conversion_entries=CONVERSION_ENTRIES,
    )

    assert report["role_summary"] == [
        {
            "role_confidence": "HIGH",
            "contact_title_group": "executive",
            "planned": 1,
            "sent": 1,
            "replied": 1,
            "converted": 1,
        },
        {
            "role_confidence": "MEDIUM",
            "contact_title_group": "finance",
            "planned": 2,
            "sent": 1,
            "replied": 0,
            "converted": 0,
        },
    ]


def test_zero_safe_rate_handling_returns_zero_rates() -> None:
    suppressed_only_states = [
        {
            "draft_id": "draft-1",
            "prospect_id": "prospect:atlas:001",
            "company_id": "atlas-build-group",
            "contact_email": "alex@example.com",
            "sequence_step": 1,
            "status": "SUPPRESSED",
            "last_event_at": "2026-04-14T09:20:00",
            "next_scheduled_at": None,
        }
    ]
    report = build_simple_analytics_report(
        optimized_message_drafts=OPTIMIZED_MESSAGE_DRAFTS[:1],
        execution_plan_records=EXECUTION_PLAN_RECORDS[:1],
        state_records=suppressed_only_states,
        conversion_signal_records=CONVERSION_SIGNAL_RECORDS[:1],
        conversion_entries=CONVERSION_ENTRIES[:1],
    )

    assert report["campaign_summary"]["total_sent"] == 0
    assert report["campaign_summary"]["reply_rate"] == 0.0
    assert report["campaign_summary"]["conversion_rate"] == 0.0
    assert report["variant_summary"][0]["reply_rate"] == 0.0
    assert report["variant_summary"][0]["conversion_rate"] == 0.0


def test_normalization_and_report_generation_are_deterministic_and_non_mutating() -> None:
    optimized_drafts = deepcopy(OPTIMIZED_MESSAGE_DRAFTS)
    execution_plan_records = deepcopy(EXECUTION_PLAN_RECORDS)
    state_records = deepcopy(STATE_RECORDS)
    conversion_signal_records = deepcopy(CONVERSION_SIGNAL_RECORDS)
    conversion_entries = deepcopy(CONVERSION_ENTRIES)

    baseline_optimized = deepcopy(optimized_drafts)
    baseline_execution = deepcopy(execution_plan_records)
    baseline_states = deepcopy(state_records)
    baseline_signals = deepcopy(conversion_signal_records)
    baseline_entries = deepcopy(conversion_entries)

    first_inputs = normalize_analytics_inputs(
        optimized_message_drafts=optimized_drafts,
        execution_plan_records=execution_plan_records,
        state_records=state_records,
        conversion_signal_records=conversion_signal_records,
        conversion_entries=conversion_entries,
    )
    second_inputs = normalize_analytics_inputs(
        optimized_message_drafts=optimized_drafts,
        execution_plan_records=execution_plan_records,
        state_records=state_records,
        conversion_signal_records=conversion_signal_records,
        conversion_entries=conversion_entries,
    )
    first_report = build_simple_analytics_report(
        optimized_message_drafts=optimized_drafts,
        execution_plan_records=execution_plan_records,
        state_records=state_records,
        conversion_signal_records=conversion_signal_records,
        conversion_entries=conversion_entries,
    )
    second_report = build_simple_analytics_report(
        optimized_message_drafts=optimized_drafts,
        execution_plan_records=execution_plan_records,
        state_records=state_records,
        conversion_signal_records=conversion_signal_records,
        conversion_entries=conversion_entries,
    )

    assert first_inputs == second_inputs
    assert first_report == second_report
    assert optimized_drafts == baseline_optimized
    assert execution_plan_records == baseline_execution
    assert state_records == baseline_states
    assert conversion_signal_records == baseline_signals
    assert conversion_entries == baseline_entries


def test_summary_outputs_remain_in_stable_deterministic_order() -> None:
    report = build_simple_analytics_report(
        optimized_message_drafts=list(reversed(OPTIMIZED_MESSAGE_DRAFTS)),
        execution_plan_records=list(reversed(EXECUTION_PLAN_RECORDS)),
        state_records=list(reversed(STATE_RECORDS)),
        conversion_signal_records=list(reversed(CONVERSION_SIGNAL_RECORDS)),
        conversion_entries=list(reversed(CONVERSION_ENTRIES)),
    )

    assert [row["template_variant_id"] for row in report["variant_summary"]] == ["v1", "v2"]
    assert [row["template_family"] for row in report["signal_summary"]] == ["litigation", "payment"]
    assert [row["role_confidence"] for row in report["role_summary"]] == ["HIGH", "MEDIUM"]
