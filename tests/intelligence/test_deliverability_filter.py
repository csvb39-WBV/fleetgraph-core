from __future__ import annotations

from copy import deepcopy

from fleetgraph.deliverability.domain_policy import evaluate_domain_policy
from fleetgraph.deliverability.filter import (
    evaluate_send_safety,
    filter_execution_plan_for_deliverability,
)
from fleetgraph.deliverability.rate_control import (
    evaluate_bounce_protection,
    resolve_stage_max_per_window,
    resolve_warmup_stage,
)
from fleetgraph.deliverability.suppression import (
    is_suppressed,
    is_valid_contact_email,
)
from fleetgraph.state.state_store import build_state_store


def _policy(
    *,
    domain: str = "fleetgraph.co",
    max_daily_sends: int = 100,
    max_per_window: int = 50,
    warmup_stage: int = 4,
    reputation_score: float = 0.98,
) -> dict[str, object]:
    return {
        "domain": domain,
        "max_daily_sends": max_daily_sends,
        "max_per_window": max_per_window,
        "warmup_stage": warmup_stage,
        "reputation_score": reputation_score,
    }


def _metric(*, domain: str = "fleetgraph.co", sent: int = 100, bounced: int = 2) -> dict[str, object]:
    return {
        "domain": domain,
        "sent": sent,
        "bounced": bounced,
    }


def _suppression(email: str, *, reason: str = "BOUNCED") -> dict[str, object]:
    return {
        "email": email,
        "reason": reason,
    }


def _plan(
    *,
    draft_id: str,
    company_id: str = "atlas-build-group",
    contact_email: str = "alex@example.com",
    scheduled_send_at: str = "2026-04-14T09:15:00",
    send_window: str = "TUESDAY_0915",
) -> dict[str, object]:
    return {
        "draft_id": draft_id,
        "prospect_id": f"prospect:{draft_id}",
        "company_id": company_id,
        "company_name": "Atlas Build Group",
        "contact_email": contact_email,
        "contact_name": "Alex Owner",
        "sequence_step": 1,
        "send_window": send_window,
        "scheduled_send_at": scheduled_send_at,
        "subject": "Quick question for Atlas Build Group",
        "body": "Hi Alex Owner,\n\nTest body.",
    }


def test_domain_limits_enforce_window_and_daily_caps() -> None:
    policy_records = [_policy(max_daily_sends=2, max_per_window=3, warmup_stage=1)]

    window_evaluation = evaluate_domain_policy(
        "fleetgraph.co",
        policy_records,
        scheduled_send_at="2026-04-14T09:15:00",
        send_window="TUESDAY_0915",
        send_counts_by_day={"fleetgraph.co|2026-04-14": 1},
        send_counts_by_window={"fleetgraph.co|TUESDAY_0915|2026-04-14T09:15:00": 10},
    )
    daily_evaluation = evaluate_domain_policy(
        "fleetgraph.co",
        policy_records,
        scheduled_send_at="2026-04-14T09:15:00",
        send_window="TUESDAY_0915",
        send_counts_by_day={"fleetgraph.co|2026-04-14": 2},
        send_counts_by_window={},
    )

    assert window_evaluation["allow_send"] is False
    assert window_evaluation["reason"] == "domain_window_cap_reached"
    assert window_evaluation["effective_max_per_window"] == 3
    assert daily_evaluation["allow_send"] is False
    assert daily_evaluation["reason"] == "domain_daily_cap_reached"


def test_warmup_ramp_resolves_expected_stage_caps() -> None:
    assert resolve_stage_max_per_window(1) == 10
    assert resolve_stage_max_per_window(2) == 20
    assert resolve_stage_max_per_window(3) == 30
    assert resolve_stage_max_per_window(4) == 50
    assert resolve_warmup_stage(6) == {"warmup_stage": 4, "max_per_window": 50}


def test_bounce_protection_blocks_only_over_threshold() -> None:
    healthy = evaluate_bounce_protection("fleetgraph.co", [_metric(sent=100, bounced=5)])
    degraded = evaluate_bounce_protection("fleetgraph.co", [_metric(sent=100, bounced=6)])

    assert healthy["allow_send"] is True
    assert healthy["status"] == "HEALTHY"
    assert degraded["allow_send"] is False
    assert degraded["status"] == "DEGRADED"


def test_suppression_lookup_is_case_insensitive() -> None:
    suppression_records = [_suppression("ALEX@EXAMPLE.COM")]

    assert is_suppressed("alex@example.com", suppression_records) is True
    assert is_suppressed("bella@example.com", suppression_records) is False


def test_email_validation_rejects_invalid_and_disposable_addresses() -> None:
    assert is_valid_contact_email("alex@example.com") is True
    assert is_valid_contact_email("") is False
    assert is_valid_contact_email("not-an-email") is False
    assert is_valid_contact_email("alex@mailinator.com") is False


def test_evaluate_send_safety_combines_state_suppression_bounce_and_validation() -> None:
    plan_record = _plan(draft_id="draft-1")
    state_records = build_state_store([plan_record])

    safe_context = {
        "sender_domain": "fleetgraph.co",
        "state_records": state_records,
        "domain_policies": [_policy(warmup_stage=4)],
        "suppression_list": [],
        "domain_metrics": [_metric(sent=100, bounced=2)],
        "send_counts_by_day": {},
        "send_counts_by_window": {},
    }
    unsafe_context = {
        "sender_domain": "fleetgraph.co",
        "state_records": state_records,
        "domain_policies": [_policy(warmup_stage=4)],
        "suppression_list": [_suppression("alex@example.com")],
        "domain_metrics": [_metric(sent=100, bounced=10)],
        "send_counts_by_day": {},
        "send_counts_by_window": {},
    }

    assert evaluate_send_safety(plan_record, safe_context) is True
    assert evaluate_send_safety(plan_record, unsafe_context) is False


def test_filter_removes_unsafe_sends_and_preserves_ordering() -> None:
    execution_plan = [
        _plan(draft_id="draft-1", contact_email="alex@example.com"),
        _plan(draft_id="draft-2", contact_email="skip@example.com"),
        _plan(draft_id="draft-3", contact_email="bad-address"),
        _plan(draft_id="draft-4", contact_email="bella@example.com", scheduled_send_at="2026-04-17T09:15:00", send_window="FRIDAY_0915"),
    ]
    state_records = build_state_store(execution_plan)
    state_records[1]["status"] = "SUPPRESSED"

    filtered_plan = filter_execution_plan_for_deliverability(
        execution_plan,
        sender_domain="fleetgraph.co",
        state_records=state_records,
        domain_policies=[_policy(max_daily_sends=10, max_per_window=10, warmup_stage=4)],
        suppression_list=[],
        domain_metrics=[_metric(sent=100, bounced=1)],
    )

    assert [row["draft_id"] for row in filtered_plan] == ["draft-1", "draft-4"]


def test_filter_enforces_per_window_capacity_deterministically() -> None:
    execution_plan = [
        _plan(draft_id="draft-1", contact_email="one@example.com"),
        _plan(draft_id="draft-2", contact_email="two@example.com"),
        _plan(draft_id="draft-3", contact_email="three@example.com"),
    ]

    filtered_plan = filter_execution_plan_for_deliverability(
        execution_plan,
        sender_domain="fleetgraph.co",
        state_records=build_state_store(execution_plan),
        domain_policies=[_policy(max_daily_sends=10, max_per_window=50, warmup_stage=2)],
        suppression_list=[],
        domain_metrics=[_metric(sent=100, bounced=0)],
        send_counts_by_window={"fleetgraph.co|TUESDAY_0915|2026-04-14T09:15:00": 19},
    )

    assert [row["draft_id"] for row in filtered_plan] == ["draft-1"]


def test_deliverability_filter_is_deterministic_and_non_mutating() -> None:
    execution_plan = [
        _plan(draft_id="draft-1", contact_email="alex@example.com"),
        _plan(draft_id="draft-2", contact_email="bella@example.com", scheduled_send_at="2026-04-17T09:15:00", send_window="FRIDAY_0915"),
    ]
    baseline_plan = deepcopy(execution_plan)
    state_records = build_state_store(execution_plan)
    baseline_state = deepcopy(state_records)

    first = filter_execution_plan_for_deliverability(
        execution_plan,
        sender_domain="fleetgraph.co",
        state_records=state_records,
        domain_policies=[_policy(max_daily_sends=10, max_per_window=10, warmup_stage=4)],
        suppression_list=[],
        domain_metrics=[_metric(sent=100, bounced=1)],
    )
    second = filter_execution_plan_for_deliverability(
        execution_plan,
        sender_domain="fleetgraph.co",
        state_records=state_records,
        domain_policies=[_policy(max_daily_sends=10, max_per_window=10, warmup_stage=4)],
        suppression_list=[],
        domain_metrics=[_metric(sent=100, bounced=1)],
    )

    assert first == second
    assert execution_plan == baseline_plan
    assert state_records == baseline_state
