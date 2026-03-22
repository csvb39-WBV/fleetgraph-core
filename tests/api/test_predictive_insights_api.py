from __future__ import annotations

import pathlib
import sys

import pytest


PROJECT_ROOT = pathlib.Path(__file__).resolve().parents[2]
SRC_ROOT = PROJECT_ROOT / "src"

if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))


from fleetgraph_core.api.predictive_insights_api import build_predictive_insights


def _valid_payload() -> dict[str, object]:
    return {
        "company_id": "cmp-001",
        "opportunities": [
            {"icp": "DLR", "opportunity_score": 92.0, "reason": "Expansion"},
            {"icp": "OEM", "opportunity_score": 75.0, "reason": "Procurement"},
            {"icp": "LEASING", "opportunity_score": 92.0, "reason": "Operations"},
        ],
        "timed_prospects": [
            {
                "company_id": "cmp-001",
                "icp": "DLR",
                "priority": "HIGH",
                "opportunity_score": 92.0,
                "reason": "Expansion",
                "timing": "IMMEDIATE",
            },
            {
                "company_id": "cmp-001",
                "icp": "OEM",
                "priority": "MEDIUM",
                "opportunity_score": 75.0,
                "reason": "Procurement",
                "timing": "7_DAYS",
            },
        ],
    }


def test_build_predictive_insights_returns_expected_shape_and_metrics() -> None:
    result = build_predictive_insights(_valid_payload())

    assert set(result.keys()) == {
        "company_id",
        "predictive_insights",
        "opportunities",
        "timed_prospects",
    }
    assert result["company_id"] == "cmp-001"
    assert result["predictive_insights"] == {
        "opportunity_count": 3,
        "timed_prospect_count": 2,
        "highest_opportunity_score": 92.0,
        "immediate_action_count": 1,
        "next_best_icp": "DLR",
    }


def test_build_predictive_insights_preserves_exact_input_order() -> None:
    payload = _valid_payload()
    payload["opportunities"] = [
        {"icp": "B", "opportunity_score": 10.0, "reason": "r2"},
        {"icp": "A", "opportunity_score": 20.0, "reason": "r1"},
    ]
    payload["timed_prospects"] = [
        {
            "company_id": "cmp-001",
            "icp": "B",
            "priority": "LOW",
            "opportunity_score": 10.0,
            "reason": "r2",
            "timing": "30_DAYS",
        },
        {
            "company_id": "cmp-001",
            "icp": "A",
            "priority": "HIGH",
            "opportunity_score": 20.0,
            "reason": "r1",
            "timing": "IMMEDIATE",
        },
    ]

    result = build_predictive_insights(payload)

    assert [item["icp"] for item in result["opportunities"]] == ["B", "A"]
    assert [item["icp"] for item in result["timed_prospects"]] == ["B", "A"]


def test_build_predictive_insights_handles_empty_opportunities_with_none_next_best() -> None:
    payload = {
        "company_id": "cmp-002",
        "opportunities": [],
        "timed_prospects": [],
    }

    result = build_predictive_insights(payload)

    assert result["predictive_insights"] == {
        "opportunity_count": 0,
        "timed_prospect_count": 0,
        "highest_opportunity_score": 0.0,
        "immediate_action_count": 0,
        "next_best_icp": "NONE",
    }


def test_build_predictive_insights_is_deterministic_for_identical_input() -> None:
    payload = _valid_payload()

    first = build_predictive_insights(payload)
    second = build_predictive_insights(payload)

    assert first == second


def test_build_predictive_insights_stable_failure_behavior() -> None:
    bad_payload = {
        "company_id": "cmp-err",
        "opportunities": {},
        "timed_prospects": [],
    }

    messages: list[str] = []
    for _ in range(3):
        with pytest.raises(ValueError) as caught:
            build_predictive_insights(bad_payload)
        messages.append(str(caught.value))

    assert messages == [
        "opportunities must be a list",
        "opportunities must be a list",
        "opportunities must be a list",
    ]


def test_build_predictive_insights_rejects_invalid_top_level_shapes() -> None:
    with pytest.raises(ValueError, match="input must be a mapping"):
        build_predictive_insights(["bad"])

    with pytest.raises(ValueError, match="company_id must be a non-empty string"):
        build_predictive_insights({"opportunities": [], "timed_prospects": []})

    with pytest.raises(ValueError, match="timed_prospects must be a list"):
        build_predictive_insights(
            {
                "company_id": "cmp-001",
                "opportunities": [],
                "timed_prospects": {},
            }
        )


def test_build_predictive_insights_rejects_invalid_nested_structures() -> None:
    with pytest.raises(
        ValueError,
        match=r"opportunities\[0\] missing required field: reason",
    ):
        payload = _valid_payload()
        payload["opportunities"] = [{"icp": "DLR", "opportunity_score": 90.0}]
        build_predictive_insights(payload)

    with pytest.raises(
        ValueError,
        match=r"timed_prospects\[0\]\.company_id must match top-level company_id",
    ):
        payload = _valid_payload()
        payload["timed_prospects"] = [
            {
                "company_id": "cmp-other",
                "icp": "DLR",
                "priority": "HIGH",
                "opportunity_score": 90.0,
                "reason": "Expansion",
                "timing": "IMMEDIATE",
            }
        ]
        build_predictive_insights(payload)

    with pytest.raises(
        ValueError,
        match=r"timed_prospects\[0\]\.timing must be one of: IMMEDIATE, 7_DAYS, 30_DAYS",
    ):
        payload = _valid_payload()
        payload["timed_prospects"] = [
            {
                "company_id": "cmp-001",
                "icp": "DLR",
                "priority": "HIGH",
                "opportunity_score": 90.0,
                "reason": "Expansion",
                "timing": "14_DAYS",
            }
        ]
        build_predictive_insights(payload)
