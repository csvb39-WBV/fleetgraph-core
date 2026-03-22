from __future__ import annotations

import pathlib
import sys

import pytest


PROJECT_ROOT = pathlib.Path(__file__).resolve().parents[2]
SRC_ROOT = PROJECT_ROOT / "src"

if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))


from fleetgraph_core.api.rfp_panel_api import build_rfp_panel


def _valid_payload() -> dict[str, object]:
    return {
        "company_id": "cmp-001",
        "opportunities": [
            {"icp": "DLR", "opportunity_score": 90.0, "reason": "Expansion"},
            {"icp": "OEM", "opportunity_score": 75.0, "reason": "Procurement"},
            {"icp": "LEASING", "opportunity_score": 90.0, "reason": "Operations"},
        ],
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
                "opportunity_score": 75.0,
                "reason": "Procurement",
                "timing": "7_DAYS",
                "stage": "READY",
                "pipeline_status": "OPEN",
            },
        ],
    }


def test_build_rfp_panel_returns_expected_shape_and_metrics() -> None:
    result = build_rfp_panel(_valid_payload())

    assert set(result.keys()) == {
        "company_id",
        "rfp_panel",
        "opportunities",
        "pipeline_records",
    }
    assert result["company_id"] == "cmp-001"
    assert result["rfp_panel"] == {
        "opportunity_count": 3,
        "pipeline_record_count": 2,
        "high_priority_pipeline_count": 1,
        "top_opportunity_icp": "DLR",
    }


def test_build_rfp_panel_preserves_exact_input_order() -> None:
    payload = _valid_payload()
    payload["opportunities"] = [
        {"icp": "B", "opportunity_score": 10.0, "reason": "r2"},
        {"icp": "A", "opportunity_score": 20.0, "reason": "r1"},
    ]
    payload["pipeline_records"] = [
        {
            "company_id": "cmp-001",
            "icp": "B",
            "priority": "LOW",
            "opportunity_score": 10.0,
            "reason": "r2",
            "timing": "30_DAYS",
            "stage": "READY",
            "pipeline_status": "OPEN",
        },
        {
            "company_id": "cmp-001",
            "icp": "A",
            "priority": "HIGH",
            "opportunity_score": 20.0,
            "reason": "r1",
            "timing": "IMMEDIATE",
            "stage": "READY",
            "pipeline_status": "OPEN",
        },
    ]

    result = build_rfp_panel(payload)

    assert [item["icp"] for item in result["opportunities"]] == ["B", "A"]
    assert [item["icp"] for item in result["pipeline_records"]] == ["B", "A"]


def test_build_rfp_panel_handles_empty_opportunities_with_none_top_icp() -> None:
    payload = {
        "company_id": "cmp-002",
        "opportunities": [],
        "pipeline_records": [],
    }

    result = build_rfp_panel(payload)

    assert result["rfp_panel"] == {
        "opportunity_count": 0,
        "pipeline_record_count": 0,
        "high_priority_pipeline_count": 0,
        "top_opportunity_icp": "NONE",
    }


def test_build_rfp_panel_is_deterministic_for_identical_input() -> None:
    payload = _valid_payload()

    first = build_rfp_panel(payload)
    second = build_rfp_panel(payload)

    assert first == second


def test_build_rfp_panel_stable_failure_behavior() -> None:
    payload = {
        "company_id": "cmp-err",
        "opportunities": {},
        "pipeline_records": [],
    }

    messages: list[str] = []
    for _ in range(3):
        with pytest.raises(ValueError) as caught:
            build_rfp_panel(payload)
        messages.append(str(caught.value))

    assert messages == [
        "opportunities must be a list",
        "opportunities must be a list",
        "opportunities must be a list",
    ]


def test_build_rfp_panel_rejects_invalid_top_level_shapes() -> None:
    with pytest.raises(ValueError, match="input must be a mapping"):
        build_rfp_panel(["bad"])

    with pytest.raises(ValueError, match="company_id must be a non-empty string"):
        build_rfp_panel({"opportunities": [], "pipeline_records": []})

    with pytest.raises(ValueError, match="pipeline_records must be a list"):
        build_rfp_panel(
            {
                "company_id": "cmp-001",
                "opportunities": [],
                "pipeline_records": {},
            }
        )


def test_build_rfp_panel_rejects_invalid_nested_structures() -> None:
    with pytest.raises(
        ValueError,
        match=r"opportunities\[0\] missing required field: reason",
    ):
        payload = _valid_payload()
        payload["opportunities"] = [{"icp": "DLR", "opportunity_score": 90.0}]
        build_rfp_panel(payload)

    with pytest.raises(
        ValueError,
        match=r"pipeline_records\[0\]\.company_id must match top-level company_id",
    ):
        payload = _valid_payload()
        payload["pipeline_records"] = [
            {
                "company_id": "cmp-other",
                "icp": "DLR",
                "priority": "HIGH",
                "opportunity_score": 90.0,
                "reason": "Expansion",
                "timing": "IMMEDIATE",
                "stage": "READY",
                "pipeline_status": "OPEN",
            }
        ]
        build_rfp_panel(payload)

    with pytest.raises(
        ValueError,
        match=r"pipeline_records\[0\]\.timing must be one of: IMMEDIATE, 7_DAYS, 30_DAYS",
    ):
        payload = _valid_payload()
        payload["pipeline_records"] = [
            {
                "company_id": "cmp-001",
                "icp": "DLR",
                "priority": "HIGH",
                "opportunity_score": 90.0,
                "reason": "Expansion",
                "timing": "14_DAYS",
                "stage": "READY",
                "pipeline_status": "OPEN",
            }
        ]
        build_rfp_panel(payload)
