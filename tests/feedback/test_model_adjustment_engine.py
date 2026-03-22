from __future__ import annotations

import pathlib
import sys

import pytest


PROJECT_ROOT = pathlib.Path(__file__).resolve().parents[2]
SRC_ROOT = PROJECT_ROOT / "src"

if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))


from fleetgraph_core.feedback.model_adjustment_engine import build_model_adjustments


def _valid_payload() -> dict[str, object]:
    return {
        "company_id": "cmp-001",
        "tracked_outcomes": [
            {
                "company_id": "cmp-001",
                "icp": "DLR",
                "priority": "HIGH",
                "opportunity_score": 92.0,
                "reason": "Expansion",
                "timing": "IMMEDIATE",
                "stage": "READY",
                "pipeline_status": "OPEN",
                "outcome_status": "WON",
                "notes": "won quickly",
            },
            {
                "company_id": "cmp-001",
                "icp": "DLR",
                "priority": "MEDIUM",
                "opportunity_score": 70.0,
                "reason": "Operations",
                "timing": "7_DAYS",
                "stage": "READY",
                "pipeline_status": "OPEN",
                "outcome_status": "NO_RESPONSE",
                "notes": "follow up",
            },
            {
                "company_id": "cmp-001",
                "icp": "OEM",
                "priority": "LOW",
                "opportunity_score": 40.0,
                "reason": "General",
                "timing": "30_DAYS",
                "stage": "READY",
                "pipeline_status": "OPEN",
                "outcome_status": "LOST",
                "notes": "not a fit",
            },
            {
                "company_id": "cmp-001",
                "icp": "UPFITTER",
                "priority": "MEDIUM",
                "opportunity_score": 65.0,
                "reason": "Permits",
                "timing": "7_DAYS",
                "stage": "READY",
                "pipeline_status": "OPEN",
                "outcome_status": "PENDING",
                "notes": "waiting",
            },
        ],
    }


def test_build_model_adjustments_maps_outcomes_and_aggregates_per_icp_and_total() -> None:
    result = build_model_adjustments(_valid_payload())

    assert result == {
        "company_id": "cmp-001",
        "model_adjustments": {
            "icp_adjustments": {
                "DLR": 0.5,
                "OEM": -1.0,
                "UPFITTER": 0.0,
            },
            "total_adjustments": -0.5,
        },
    }


def test_build_model_adjustments_is_order_independent_and_deterministic() -> None:
    payload_a = _valid_payload()
    payload_b = {
        "company_id": payload_a["company_id"],
        "tracked_outcomes": list(reversed(payload_a["tracked_outcomes"])),
    }

    first = build_model_adjustments(payload_a)
    second = build_model_adjustments(payload_b)
    third = build_model_adjustments(payload_a)

    assert first == second
    assert first == third


def test_build_model_adjustments_rejects_invalid_top_level_shapes() -> None:
    with pytest.raises(ValueError, match="input must be a mapping"):
        build_model_adjustments(["bad"])

    with pytest.raises(ValueError, match="company_id must be a non-empty string"):
        build_model_adjustments({"tracked_outcomes": []})

    with pytest.raises(ValueError, match="tracked_outcomes must be a list"):
        build_model_adjustments({"company_id": "cmp-001", "tracked_outcomes": {}})


def test_build_model_adjustments_rejects_invalid_tracked_outcome_structures() -> None:
    with pytest.raises(
        ValueError,
        match=r"tracked_outcomes\[0\] missing required field: notes",
    ):
        build_model_adjustments(
            {
                "company_id": "cmp-001",
                "tracked_outcomes": [
                    {
                        "company_id": "cmp-001",
                        "icp": "DLR",
                        "priority": "HIGH",
                        "opportunity_score": 90.0,
                        "reason": "Expansion",
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
        build_model_adjustments(payload)

    with pytest.raises(
        ValueError,
        match=r"tracked_outcomes\[0\]\.priority must be one of: HIGH, MEDIUM, LOW",
    ):
        payload = _valid_payload()
        payload["tracked_outcomes"][0]["priority"] = "URGENT"
        build_model_adjustments(payload)

    with pytest.raises(
        ValueError,
        match=r"tracked_outcomes\[0\]\.timing must be one of: IMMEDIATE, 7_DAYS, 30_DAYS",
    ):
        payload = _valid_payload()
        payload["tracked_outcomes"][0]["timing"] = "14_DAYS"
        build_model_adjustments(payload)

    with pytest.raises(
        ValueError,
        match=r"tracked_outcomes\[0\]\.stage must be READY",
    ):
        payload = _valid_payload()
        payload["tracked_outcomes"][0]["stage"] = "PENDING"
        build_model_adjustments(payload)

    with pytest.raises(
        ValueError,
        match=r"tracked_outcomes\[0\]\.pipeline_status must be OPEN",
    ):
        payload = _valid_payload()
        payload["tracked_outcomes"][0]["pipeline_status"] = "CLOSED"
        build_model_adjustments(payload)

    with pytest.raises(
        ValueError,
        match=(
            r"tracked_outcomes\[0\]\.outcome_status must be one of: "
            r"LOST, NO_RESPONSE, PENDING, WON"
        ),
    ):
        payload = _valid_payload()
        payload["tracked_outcomes"][0]["outcome_status"] = "MAYBE"
        build_model_adjustments(payload)


def test_build_model_adjustments_rejects_non_string_notes() -> None:
    payload = _valid_payload()
    payload["tracked_outcomes"][0]["notes"] = 123

    with pytest.raises(ValueError, match=r"tracked_outcomes\[0\]\.notes must be a string"):
        build_model_adjustments(payload)


def test_build_model_adjustments_stable_failure_behavior() -> None:
    bad_payload = {
        "company_id": "cmp-err",
        "tracked_outcomes": {},
    }

    messages: list[str] = []
    for _ in range(3):
        with pytest.raises(ValueError) as caught:
            build_model_adjustments(bad_payload)
        messages.append(str(caught.value))

    assert messages == [
        "tracked_outcomes must be a list",
        "tracked_outcomes must be a list",
        "tracked_outcomes must be a list",
    ]