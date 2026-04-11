from __future__ import annotations

from copy import deepcopy
from datetime import datetime

import pytest

from fleetgraph.state.cooldown import is_in_cooldown
from fleetgraph.state.response_processor import process_response_events
from fleetgraph.state.state_engine import (
    apply_state_updates,
    detect_conversion_signals,
    filter_execution_plan,
)
from fleetgraph.state.state_store import build_state_store


def _plan_record(
    *,
    draft_id: str,
    prospect_id: str = "prospect:atlas:001",
    company_id: str = "atlas-build-group",
    company_name: str = "Atlas Build Group",
    contact_email: str = "alex@example.com",
    contact_name: str = "Alex Owner",
    sequence_step: int = 1,
    scheduled_send_at: str = "2026-04-14T09:15:00",
) -> dict[str, object]:
    return {
        "draft_id": draft_id,
        "prospect_id": prospect_id,
        "company_id": company_id,
        "company_name": company_name,
        "contact_email": contact_email,
        "contact_name": contact_name,
        "sequence_step": sequence_step,
        "send_window": "TUESDAY_0915" if "04-14" in scheduled_send_at or "04-21" in scheduled_send_at else "FRIDAY_0915",
        "scheduled_send_at": scheduled_send_at,
        "subject": "Quick question for Atlas Build Group",
        "body": "Hi Alex Owner,\n\nTest body.",
    }


def _sequence_plan() -> list[dict[str, object]]:
    return [
        _plan_record(draft_id="draft-1", sequence_step=1, scheduled_send_at="2026-04-14T09:15:00"),
        _plan_record(draft_id="draft-2", sequence_step=2, scheduled_send_at="2026-04-17T09:15:00"),
        _plan_record(draft_id="draft-3", sequence_step=3, scheduled_send_at="2026-04-21T09:15:00"),
        _plan_record(draft_id="draft-4", sequence_step=4, scheduled_send_at="2026-04-24T09:15:00"),
    ]


def test_state_store_builds_pending_records_without_duplicates() -> None:
    state_records = build_state_store(_sequence_plan())

    assert [(row["draft_id"], row["status"], row["sequence_step"]) for row in state_records] == [
        ("draft-1", "PENDING", 1),
        ("draft-2", "PENDING", 2),
        ("draft-3", "PENDING", 3),
        ("draft-4", "PENDING", 4),
    ]


def test_valid_and_invalid_state_transitions() -> None:
    state_records = build_state_store([_plan_record(draft_id="draft-1")])
    sent_state = apply_state_updates(
        state_records,
        [
            {
                "draft_id": "draft-1",
                "status": "SENT",
                "last_event_at": datetime(2026, 4, 14, 9, 20, 0),
                "next_scheduled_at": None,
            }
        ],
    )

    assert sent_state[0]["status"] == "SENT"

    with pytest.raises(ValueError, match="invalid transition: PENDING -> REPLIED"):
        apply_state_updates(
            state_records,
            [
                {
                    "draft_id": "draft-1",
                    "status": "REPLIED",
                    "last_event_at": datetime(2026, 4, 14, 9, 21, 0),
                    "next_scheduled_at": None,
                }
            ],
        )


def test_reply_event_suppresses_future_steps() -> None:
    state_records = build_state_store(_sequence_plan())
    state_records = apply_state_updates(
        state_records,
        [
            {
                "draft_id": "draft-1",
                "status": "SENT",
                "last_event_at": datetime(2026, 4, 14, 9, 20, 0),
                "next_scheduled_at": None,
            },
            {
                "draft_id": "draft-2",
                "status": "SENT",
                "last_event_at": datetime(2026, 4, 17, 9, 20, 0),
                "next_scheduled_at": None,
            },
        ],
    )
    response_updates = process_response_events(
        [
            {
                "draft_id": "draft-2",
                "event_type": "REPLIED",
                "timestamp": datetime(2026, 4, 17, 10, 0, 0),
            }
        ]
    )

    updated_state = apply_state_updates(state_records, response_updates)

    assert [(row["draft_id"], row["status"]) for row in updated_state] == [
        ("draft-1", "SENT"),
        ("draft-2", "REPLIED"),
        ("draft-3", "SUPPRESSED"),
        ("draft-4", "SUPPRESSED"),
    ]


def test_bounce_event_suppresses_future_steps() -> None:
    state_records = build_state_store(_sequence_plan())
    state_records = apply_state_updates(
        state_records,
        [
            {
                "draft_id": "draft-1",
                "status": "SENT",
                "last_event_at": datetime(2026, 4, 14, 9, 20, 0),
                "next_scheduled_at": None,
            }
        ],
    )
    response_updates = process_response_events(
        [
            {
                "draft_id": "draft-1",
                "event_type": "BOUNCED",
                "timestamp": datetime(2026, 4, 14, 9, 25, 0),
            }
        ]
    )

    updated_state = apply_state_updates(state_records, response_updates)

    assert [(row["draft_id"], row["status"]) for row in updated_state] == [
        ("draft-1", "BOUNCED"),
        ("draft-2", "SUPPRESSED"),
        ("draft-3", "SUPPRESSED"),
        ("draft-4", "SUPPRESSED"),
    ]


def test_runtime_cooldown_blocks_within_45_days_and_allows_afterward() -> None:
    state_records = build_state_store(_sequence_plan())
    state_records = apply_state_updates(
        state_records,
        [
            {
                "draft_id": "draft-1",
                "status": "SENT",
                "last_event_at": datetime(2026, 4, 14, 9, 20, 0),
                "next_scheduled_at": None,
            },
            {
                "draft_id": "draft-2",
                "status": "SENT",
                "last_event_at": datetime(2026, 4, 17, 9, 20, 0),
                "next_scheduled_at": None,
            },
        ],
    )
    state_records = apply_state_updates(
        state_records,
        process_response_events(
            [
                {
                    "draft_id": "draft-2",
                    "event_type": "REPLIED",
                    "timestamp": datetime(2026, 4, 17, 10, 0, 0),
                }
            ]
        ),
    )

    assert is_in_cooldown("atlas-build-group", datetime(2026, 5, 20, 9, 0, 0), state_records) is True
    assert is_in_cooldown("atlas-build-group", datetime(2026, 6, 2, 10, 1, 0), state_records) is False


def test_execution_filter_keeps_only_valid_pending_sends() -> None:
    execution_plan = _sequence_plan()
    state_records = build_state_store(execution_plan)
    state_records = apply_state_updates(
        state_records,
        [
            {
                "draft_id": "draft-1",
                "status": "SENT",
                "last_event_at": datetime(2026, 4, 14, 9, 20, 0),
                "next_scheduled_at": None,
            },
            {
                "draft_id": "draft-2",
                "status": "SUPPRESSED",
                "last_event_at": datetime(2026, 4, 15, 9, 20, 0),
                "next_scheduled_at": None,
            },
        ],
    )

    filtered_plan = filter_execution_plan(execution_plan, state_records)

    assert [row["draft_id"] for row in filtered_plan] == ["draft-3", "draft-4"]


def test_conversion_signal_detection_flags_reply_and_engagement_patterns() -> None:
    conversion_flags = detect_conversion_signals(
        [
            {
                "prospect_id": "prospect:atlas:001",
                "event_type": "OPENED",
                "timestamp": datetime(2026, 4, 14, 9, 30, 0),
            },
            {
                "prospect_id": "prospect:atlas:001",
                "event_type": "OPENED",
                "timestamp": datetime(2026, 4, 14, 9, 35, 0),
            },
            {
                "prospect_id": "prospect:beacon:001",
                "event_type": "REPLIED",
                "timestamp": datetime(2026, 4, 15, 10, 0, 0),
            },
            {
                "prospect_id": "prospect:cobalt:001",
                "event_type": "ENGAGED",
                "timestamp": datetime(2026, 4, 16, 10, 0, 0),
            },
            {
                "prospect_id": "prospect:cobalt:001",
                "event_type": "ENGAGED",
                "timestamp": datetime(2026, 4, 16, 11, 0, 0),
            },
            {
                "prospect_id": "prospect:delta:001",
                "event_type": "OPENED",
                "timestamp": datetime(2026, 4, 16, 12, 0, 0),
            },
        ]
    )

    assert conversion_flags == [
        {"prospect_id": "prospect:atlas:001", "conversion_flag": True, "reason": "multiple_email_opens"},
        {"prospect_id": "prospect:beacon:001", "conversion_flag": True, "reason": "reply_detected"},
        {"prospect_id": "prospect:cobalt:001", "conversion_flag": True, "reason": "repeated_engagement_signals"},
        {"prospect_id": "prospect:delta:001", "conversion_flag": False, "reason": "no_conversion_signal"},
    ]


def test_state_updates_are_deterministic_and_non_mutating() -> None:
    execution_plan = _sequence_plan()
    state_records = build_state_store(execution_plan)
    baseline_plan = deepcopy(execution_plan)
    baseline_state = deepcopy(state_records)
    response_updates = process_response_events(
        [
            {
                "draft_id": "draft-1",
                "event_type": "UNSUBSCRIBED",
                "timestamp": datetime(2026, 4, 14, 10, 0, 0),
            }
        ]
    )

    sent_state = apply_state_updates(
        state_records,
        [
            {
                "draft_id": "draft-1",
                "status": "SENT",
                "last_event_at": datetime(2026, 4, 14, 9, 20, 0),
                "next_scheduled_at": None,
            }
        ],
    )
    first = apply_state_updates(sent_state, response_updates)
    second = apply_state_updates(sent_state, response_updates)

    assert first == second
    assert execution_plan == baseline_plan
    assert state_records == baseline_state
