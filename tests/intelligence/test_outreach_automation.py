from __future__ import annotations

from datetime import datetime

import pytest

from fleetgraph.outreach.outreach_automation import (
    build_outreach_execution_plan,
    build_sender_payloads,
    resolve_next_send_window,
)


def _draft(
    *,
    prospect_id: str = "prospect:atlas:001",
    company_id: str = "atlas-build-group",
    company_name: str = "Atlas Build Group",
    email: str = "alex@example.com",
    name: str = "Alex Owner",
    priority_rank: int = 1,
    subject: str = "Quick question for Atlas Build Group",
    body: str = "Hi Alex Owner,\n\nTest body.",
) -> dict[str, object]:
    return {
        "prospect_id": prospect_id,
        "company_id": company_id,
        "company_name": company_name,
        "contact": {
            "name": name,
            "title": "Owner",
            "email": email,
            "priority_rank": priority_rank,
        },
        "selected_bucket": "T1",
        "signal_type": "litigation_risk",
        "signal_detail": "Lawsuit filed against Atlas Build Group",
        "subject": subject,
        "body": body,
    }


@pytest.mark.parametrize(
    ("reference_datetime", "expected_window", "expected_at"),
    [
        (datetime(2026, 4, 13, 20, 0, 0), "TUESDAY_0915", "2026-04-14T09:15:00"),
        (datetime(2026, 4, 14, 8, 0, 0), "TUESDAY_0915", "2026-04-14T09:15:00"),
        (datetime(2026, 4, 14, 9, 16, 0), "FRIDAY_0915", "2026-04-17T09:15:00"),
        (datetime(2026, 4, 16, 20, 0, 0), "FRIDAY_0915", "2026-04-17T09:15:00"),
        (datetime(2026, 4, 17, 9, 16, 0), "TUESDAY_0915", "2026-04-21T09:15:00"),
        (datetime(2026, 4, 18, 12, 0, 0), "TUESDAY_0915", "2026-04-21T09:15:00"),
        (datetime(2026, 4, 19, 12, 0, 0), "TUESDAY_0915", "2026-04-21T09:15:00"),
    ],
)
def test_next_valid_send_window_resolution(
    reference_datetime: datetime,
    expected_window: str,
    expected_at: str,
) -> None:
    result = resolve_next_send_window(reference_datetime)

    assert result["send_window"] == expected_window
    assert result["scheduled_send_at"] == expected_at


def test_sequence_planning_creates_ordered_tuesday_friday_steps_only() -> None:
    result = build_outreach_execution_plan(
        [_draft()],
        reference_datetime=datetime(2026, 4, 13, 20, 0, 0),
        max_sequence_length=4,
    )

    assert [(row["sequence_step"], row["send_window"], row["scheduled_send_at"]) for row in result["planned_sends"]] == [
        (1, "TUESDAY_0915", "2026-04-14T09:15:00"),
        (2, "FRIDAY_0915", "2026-04-17T09:15:00"),
        (3, "TUESDAY_0915", "2026-04-21T09:15:00"),
        (4, "FRIDAY_0915", "2026-04-24T09:15:00"),
    ]
    assert all(datetime.fromisoformat(row["scheduled_send_at"]).weekday() in (1, 4) for row in result["planned_sends"])


def test_batch_cap_enforcement_and_overflow_are_deterministic() -> None:
    drafts = [
        _draft(
            prospect_id=f"prospect:{index:03d}",
            company_id=f"company-{index:03d}",
            company_name=f"Company {index:03d}",
            email=f"contact{index:03d}@example.com",
            name=f"Contact {index:03d}",
        )
        for index in range(1, 4)
    ]

    result = build_outreach_execution_plan(
        drafts,
        reference_datetime=datetime(2026, 4, 13, 20, 0, 0),
        max_sequence_length=2,
        max_emails_per_send_window=2,
    )

    assert [(row["prospect_id"], row["sequence_step"], row["send_window"]) for row in result["planned_sends"]] == [
        ("prospect:001", 1, "TUESDAY_0915"),
        ("prospect:002", 1, "TUESDAY_0915"),
        ("prospect:001", 2, "FRIDAY_0915"),
        ("prospect:002", 2, "FRIDAY_0915"),
    ]
    assert [(row["prospect_id"], row["sequence_step"], row["send_window"], row["overflow_reason"]) for row in result["overflow"]] == [
        ("prospect:003", 1, "TUESDAY_0915", "send_window_cap_reached"),
        ("prospect:003", 2, "FRIDAY_0915", "send_window_cap_reached"),
    ]


def test_deterministic_ids_and_repeated_runs_match() -> None:
    drafts = [
        _draft(),
        _draft(
            prospect_id="prospect:beacon:001",
            company_id="beacon-masonry-services",
            company_name="Beacon Masonry Services",
            email="bella@example.com",
            name="Bella Finance",
            priority_rank=2,
            subject="Question after a recent payment issue at Beacon Masonry Services",
            body="Hi Bella Finance,\n\nBeacon body.",
        ),
    ]

    first = build_outreach_execution_plan(
        drafts,
        reference_datetime=datetime(2026, 4, 13, 20, 0, 0),
        max_sequence_length=2,
    )
    second = build_outreach_execution_plan(
        drafts,
        reference_datetime=datetime(2026, 4, 13, 20, 0, 0),
        max_sequence_length=2,
    )

    assert first == second
    assert [row["draft_id"] for row in first["planned_sends"]] == [row["draft_id"] for row in second["planned_sends"]]


def test_multi_draft_ordering_is_stable() -> None:
    drafts = [
        _draft(prospect_id="prospect:b", company_id="company-b", company_name="Company B", email="b@example.com", name="B Contact"),
        _draft(prospect_id="prospect:a", company_id="company-a", company_name="Company A", email="a@example.com", name="A Contact"),
    ]

    result = build_outreach_execution_plan(
        drafts,
        reference_datetime=datetime(2026, 4, 13, 20, 0, 0),
        max_sequence_length=1,
    )

    assert [(row["prospect_id"], row["contact_email"], row["sequence_step"]) for row in result["planned_sends"]] == [
        ("prospect:b", "b@example.com", 1),
        ("prospect:a", "a@example.com", 1),
    ]


def test_sender_adapter_payload_generation_preserves_fields() -> None:
    plan = build_outreach_execution_plan(
        [_draft()],
        reference_datetime=datetime(2026, 4, 13, 20, 0, 0),
        max_sequence_length=1,
    )

    payloads = build_sender_payloads(plan["planned_sends"])

    assert payloads == [
        {
            "draft_id": plan["planned_sends"][0]["draft_id"],
            "recipient": {
                "email": "alex@example.com",
                "name": "Alex Owner",
            },
            "message": {
                "subject": "Quick question for Atlas Build Group",
                "body": "Hi Alex Owner,\n\nTest body.",
            },
            "metadata": {
                "prospect_id": "prospect:atlas:001",
                "company_id": "atlas-build-group",
                "company_name": "Atlas Build Group",
                "sequence_step": 1,
                "send_window": "TUESDAY_0915",
                "scheduled_send_at": "2026-04-14T09:15:00",
            },
        }
    ]


def test_jitter_is_deterministic_and_stays_in_window() -> None:
    first = build_outreach_execution_plan(
        [_draft()],
        reference_datetime=datetime(2026, 4, 13, 20, 0, 0),
        max_sequence_length=1,
        max_jitter_minutes=10,
    )
    second = build_outreach_execution_plan(
        [_draft()],
        reference_datetime=datetime(2026, 4, 13, 20, 0, 0),
        max_sequence_length=1,
        max_jitter_minutes=10,
    )

    assert first == second
    scheduled = datetime.fromisoformat(first["planned_sends"][0]["scheduled_send_at"])
    assert scheduled.weekday() == 1
    assert scheduled.hour == 9
    assert 15 <= scheduled.minute <= 25
