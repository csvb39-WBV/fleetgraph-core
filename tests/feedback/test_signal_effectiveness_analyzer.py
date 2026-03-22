from __future__ import annotations

import pathlib
import sys

import pytest


PROJECT_ROOT = pathlib.Path(__file__).resolve().parents[2]
SRC_ROOT = PROJECT_ROOT / "src"

if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))


from fleetgraph_core.feedback.signal_effectiveness_analyzer import (
    analyze_signal_effectiveness,
)


def _valid_payload() -> dict[str, object]:
    return {
        "company_id": "cmp-001",
        "tracked_outcomes": [
            {
                "company_id": "cmp-001",
                "icp": "OEM",
                "priority": "HIGH",
                "opportunity_score": 90.0,
                "reason": "r1",
                "timing": "IMMEDIATE",
                "stage": "READY",
                "pipeline_status": "OPEN",
                "outcome_status": "WON",
                "notes": "won",
            },
            {
                "company_id": "cmp-001",
                "icp": "DLR",
                "priority": "MEDIUM",
                "opportunity_score": 70.0,
                "reason": "r2",
                "timing": "7_DAYS",
                "stage": "READY",
                "pipeline_status": "OPEN",
                "outcome_status": "NO_RESPONSE",
                "notes": "no response",
            },
            {
                "company_id": "cmp-001",
                "icp": "OEM",
                "priority": "LOW",
                "opportunity_score": 40.0,
                "reason": "r3",
                "timing": "30_DAYS",
                "stage": "READY",
                "pipeline_status": "OPEN",
                "outcome_status": "LOST",
                "notes": "lost",
            },
            {
                "company_id": "cmp-001",
                "icp": "UPFITTER",
                "priority": "LOW",
                "opportunity_score": 30.0,
                "reason": "r4",
                "timing": "30_DAYS",
                "stage": "READY",
                "pipeline_status": "OPEN",
                "outcome_status": "PENDING",
                "notes": "pending",
            },
        ],
    }


def test_analyze_signal_effectiveness_returns_expected_summary_and_per_icp_stats() -> None:
    result = analyze_signal_effectiveness(_valid_payload())

    assert result == {
        "company_id": "cmp-001",
        "signal_effectiveness": {
            "total_records": 4,
            "total_effectiveness_score": -0.5,
            "icp_effectiveness": {
                "DLR": {
                    "record_count": 1,
                    "won_count": 0,
                    "lost_count": 0,
                    "no_response_count": 1,
                    "pending_count": 0,
                    "effectiveness_score": -0.5,
                },
                "OEM": {
                    "record_count": 2,
                    "won_count": 1,
                    "lost_count": 1,
                    "no_response_count": 0,
                    "pending_count": 0,
                    "effectiveness_score": 0.0,
                },
                "UPFITTER": {
                    "record_count": 1,
                    "won_count": 0,
                    "lost_count": 0,
                    "no_response_count": 0,
                    "pending_count": 1,
                    "effectiveness_score": 0.0,
                },
            },
        },
    }


def test_analyze_signal_effectiveness_icp_keys_are_alphabetically_ordered() -> None:
    result = analyze_signal_effectiveness(_valid_payload())

    assert list(result["signal_effectiveness"]["icp_effectiveness"].keys()) == [
        "DLR",
        "OEM",
        "UPFITTER",
    ]


def test_analyze_signal_effectiveness_is_deterministic_and_order_independent() -> None:
    payload_a = _valid_payload()
    payload_b = {
        "company_id": payload_a["company_id"],
        "tracked_outcomes": list(reversed(payload_a["tracked_outcomes"])),
    }

    first = analyze_signal_effectiveness(payload_a)
    second = analyze_signal_effectiveness(payload_b)
    third = analyze_signal_effectiveness(payload_a)

    assert first == second
    assert first == third


def test_analyze_signal_effectiveness_rejects_invalid_top_level_shapes() -> None:
    with pytest.raises(ValueError, match="input must be a mapping"):
        analyze_signal_effectiveness(["bad"])

    with pytest.raises(ValueError, match="company_id must be a non-empty string"):
        analyze_signal_effectiveness({"tracked_outcomes": []})

    with pytest.raises(ValueError, match="tracked_outcomes must be a list"):
        analyze_signal_effectiveness({"company_id": "cmp-001", "tracked_outcomes": {}})


def test_analyze_signal_effectiveness_rejects_invalid_tracked_outcome_shape_values() -> None:
    with pytest.raises(
        ValueError,
        match=r"tracked_outcomes\[0\] missing required field: notes",
    ):
        analyze_signal_effectiveness(
            {
                "company_id": "cmp-001",
                "tracked_outcomes": [
                    {
                        "company_id": "cmp-001",
                        "icp": "DLR",
                        "priority": "HIGH",
                        "opportunity_score": 90.0,
                        "reason": "r",
                        "timing": "IMMEDIATE",
                        "stage": "READY",
                        "pipeline_status": "OPEN",
                        "outcome_status": "WON",
                    }
                ],
            }
        )

    with pytest.raises(
        ValueError,
        match=r"tracked_outcomes\[0\]\.company_id must match top-level company_id",
    ):
        payload = _valid_payload()
        payload["tracked_outcomes"][0]["company_id"] = "cmp-other"
        analyze_signal_effectiveness(payload)

    with pytest.raises(
        ValueError,
        match=r"tracked_outcomes\[0\]\.priority must be one of: HIGH, MEDIUM, LOW",
    ):
        payload = _valid_payload()
        payload["tracked_outcomes"][0]["priority"] = "URGENT"
        analyze_signal_effectiveness(payload)

    with pytest.raises(
        ValueError,
        match=r"tracked_outcomes\[0\]\.timing must be one of: IMMEDIATE, 7_DAYS, 30_DAYS",
    ):
        payload = _valid_payload()
        payload["tracked_outcomes"][0]["timing"] = "14_DAYS"
        analyze_signal_effectiveness(payload)

    with pytest.raises(
        ValueError,
        match=r"tracked_outcomes\[0\]\.stage must be READY",
    ):
        payload = _valid_payload()
        payload["tracked_outcomes"][0]["stage"] = "PENDING"
        analyze_signal_effectiveness(payload)

    with pytest.raises(
        ValueError,
        match=r"tracked_outcomes\[0\]\.pipeline_status must be OPEN",
    ):
        payload = _valid_payload()
        payload["tracked_outcomes"][0]["pipeline_status"] = "CLOSED"
        analyze_signal_effectiveness(payload)

    with pytest.raises(
        ValueError,
        match=(
            r"tracked_outcomes\[0\]\.outcome_status must be one of: "
            r"LOST, NO_RESPONSE, PENDING, WON"
        ),
    ):
        payload = _valid_payload()
        payload["tracked_outcomes"][0]["outcome_status"] = "MAYBE"
        analyze_signal_effectiveness(payload)


def test_analyze_signal_effectiveness_stable_failure_behavior() -> None:
    payload = {
        "company_id": "cmp-err",
        "tracked_outcomes": {},
    }

    messages: list[str] = []
    for _ in range(3):
        with pytest.raises(ValueError) as caught:
            analyze_signal_effectiveness(payload)
        messages.append(str(caught.value))

    assert messages == [
        "tracked_outcomes must be a list",
        "tracked_outcomes must be a list",
        "tracked_outcomes must be a list",
    ]