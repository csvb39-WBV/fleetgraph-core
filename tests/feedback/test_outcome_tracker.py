from __future__ import annotations

import pathlib
import sys

import pytest


PROJECT_ROOT = pathlib.Path(__file__).resolve().parents[2]
SRC_ROOT = PROJECT_ROOT / "src"

if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))


from fleetgraph_core.feedback.outcome_tracker import track_outcomes


def _valid_payload() -> dict[str, object]:
    return {
        "company_id": "cmp-001",
        "pipeline_records": [
            {
                "company_id": "cmp-001",
                "icp": "DLR",
                "priority": "HIGH",
                "opportunity_score": 90.0,
                "reason": "Expansion",
                "timing": "IMMEDIATE",
                "stage": "READY",
                "pipeline_status": "OPEN",
            },
            {
                "company_id": "cmp-001",
                "icp": "OEM",
                "priority": "MEDIUM",
                "opportunity_score": 70.0,
                "reason": "Procurement",
                "timing": "7_DAYS",
                "stage": "READY",
                "pipeline_status": "OPEN",
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
            },
        ],
        "outcomes": [
            {
                "icp": "OEM",
                "outcome_status": "WON",
                "notes": "positive response",
            }
        ],
    }


def test_track_outcomes_applies_first_match_and_defaults_in_input_order() -> None:
    result = track_outcomes(_valid_payload())

    assert result["company_id"] == "cmp-001"
    tracked = result["tracked_outcomes"]
    assert [item["icp"] for item in tracked] == ["DLR", "OEM", "OEM"]

    assert tracked[0]["outcome_status"] == "PENDING"
    assert tracked[0]["notes"] == ""

    # First OEM record gets outcome due to first-match rule.
    assert tracked[1]["outcome_status"] == "WON"
    assert tracked[1]["notes"] == "positive response"

    # Second OEM record remains default pending.
    assert tracked[2]["outcome_status"] == "PENDING"
    assert tracked[2]["notes"] == ""


def test_track_outcomes_rejects_unmatched_outcome() -> None:
    payload = _valid_payload()
    payload["outcomes"] = [{"icp": "UPFITTER", "outcome_status": "LOST", "notes": "no fit"}]

    with pytest.raises(
        ValueError,
        match=r"outcomes\[0\] has no matching pipeline record for icp: UPFITTER",
    ):
        track_outcomes(payload)


def test_track_outcomes_is_deterministic_for_identical_input() -> None:
    payload = _valid_payload()

    first = track_outcomes(payload)
    second = track_outcomes(payload)

    assert first == second


def test_track_outcomes_rejects_invalid_top_level_shapes() -> None:
    with pytest.raises(ValueError, match="input must be a mapping"):
        track_outcomes(["bad"])

    with pytest.raises(ValueError, match="company_id must be a non-empty string"):
        track_outcomes({"pipeline_records": [], "outcomes": []})

    with pytest.raises(ValueError, match="pipeline_records must be a list"):
        track_outcomes({"company_id": "cmp-001", "pipeline_records": {}, "outcomes": []})

    with pytest.raises(ValueError, match="outcomes must be a list"):
        track_outcomes({"company_id": "cmp-001", "pipeline_records": [], "outcomes": {}})


def test_track_outcomes_rejects_invalid_pipeline_record_and_outcome_shapes() -> None:
    with pytest.raises(
        ValueError,
        match=r"pipeline_records\[0\] missing required field: timing",
    ):
        track_outcomes(
            {
                "company_id": "cmp-001",
                "pipeline_records": [
                    {
                        "company_id": "cmp-001",
                        "icp": "DLR",
                        "priority": "HIGH",
                        "opportunity_score": 90.0,
                        "reason": "Expansion",
                        "stage": "READY",
                        "pipeline_status": "OPEN",
                    }
                ],
                "outcomes": [],
            }
        )

    with pytest.raises(
        ValueError,
        match=r"pipeline_records\[0\]\.priority must be one of: HIGH, MEDIUM, LOW",
    ):
        payload = _valid_payload()
        payload["pipeline_records"][0]["priority"] = "URGENT"
        track_outcomes(payload)

    with pytest.raises(
        ValueError,
        match=r"pipeline_records\[0\]\.timing must be one of: IMMEDIATE, 7_DAYS, 30_DAYS",
    ):
        payload = _valid_payload()
        payload["pipeline_records"][0]["timing"] = "14_DAYS"
        track_outcomes(payload)

    with pytest.raises(
        ValueError,
        match=r"pipeline_records\[0\]\.stage must be READY",
    ):
        payload = _valid_payload()
        payload["pipeline_records"][0]["stage"] = "PENDING"
        track_outcomes(payload)

    with pytest.raises(
        ValueError,
        match=r"pipeline_records\[0\]\.pipeline_status must be OPEN",
    ):
        payload = _valid_payload()
        payload["pipeline_records"][0]["pipeline_status"] = "CLOSED"
        track_outcomes(payload)

    with pytest.raises(
        ValueError,
        match=r"outcomes\[0\] missing required field: notes",
    ):
        payload = _valid_payload()
        payload["outcomes"] = [{"icp": "DLR", "outcome_status": "WON"}]
        track_outcomes(payload)

    with pytest.raises(
        ValueError,
        match=r"outcomes\[0\]\.outcome_status must be one of: LOST, NO_RESPONSE, WON",
    ):
        payload = _valid_payload()
        payload["outcomes"] = [{"icp": "DLR", "outcome_status": "MAYBE", "notes": "x"}]
        track_outcomes(payload)


def test_track_outcomes_rejects_non_string_outcome_notes() -> None:
    payload = _valid_payload()
    payload["outcomes"] = [{"icp": "DLR", "outcome_status": "LOST", "notes": 123}]

    with pytest.raises(ValueError, match=r"outcomes\[0\]\.notes must be a string"):
        track_outcomes(payload)