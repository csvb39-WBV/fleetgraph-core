from __future__ import annotations

import pathlib
import sys

import pytest


PROJECT_ROOT = pathlib.Path(__file__).resolve().parents[2]
SRC_ROOT = PROJECT_ROOT / "src"

if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))


from fleetgraph_core.api.priority_dashboard_api import build_priority_dashboard


def test_build_priority_dashboard_returns_expected_shape_and_counts() -> None:
    payload = {
        "company_id": "cmp-001",
        "pipeline_records": [
            {
                "company_id": "cmp-001",
                "icp": "DLR",
                "priority": "HIGH",
                "opportunity_score": 92.0,
                "reason": "Expansion",
                "timing": "IMMEDIATE",
                "stage": "READY",
                "pipeline_status": "OPEN",
            },
            {
                "company_id": "cmp-001",
                "icp": "OEM",
                "priority": "MEDIUM",
                "opportunity_score": 66.0,
                "reason": "Procurement",
                "timing": "7_DAYS",
                "stage": "READY",
                "pipeline_status": "OPEN",
            },
            {
                "company_id": "cmp-001",
                "icp": "LEASING",
                "priority": "LOW",
                "opportunity_score": 30.0,
                "reason": "No ICP-aligned signals",
                "timing": "30_DAYS",
                "stage": "READY",
                "pipeline_status": "OPEN",
            },
        ],
    }

    result = build_priority_dashboard(payload)

    assert set(result.keys()) == {"company_id", "dashboard_summary", "records"}
    assert result["company_id"] == "cmp-001"
    assert result["dashboard_summary"] == {
        "total_records": 3,
        "high_priority_count": 1,
        "medium_priority_count": 1,
        "low_priority_count": 1,
    }
    assert len(result["records"]) == 3


def test_build_priority_dashboard_preserves_input_record_order() -> None:
    result = build_priority_dashboard(
        {
            "company_id": "cmp-002",
            "pipeline_records": [
                {
                    "company_id": "cmp-002",
                    "icp": "ZETA",
                    "priority": "LOW",
                    "opportunity_score": 10.0,
                    "reason": "r1",
                    "timing": "30_DAYS",
                    "stage": "READY",
                    "pipeline_status": "OPEN",
                },
                {
                    "company_id": "cmp-002",
                    "icp": "ALPHA",
                    "priority": "HIGH",
                    "opportunity_score": 99.0,
                    "reason": "r2",
                    "timing": "IMMEDIATE",
                    "stage": "READY",
                    "pipeline_status": "OPEN",
                },
            ],
        }
    )

    assert [record["icp"] for record in result["records"]] == ["ZETA", "ALPHA"]


def test_build_priority_dashboard_is_deterministic_for_identical_input() -> None:
    payload = {
        "company_id": "cmp-003",
        "pipeline_records": [
            {
                "company_id": "cmp-003",
                "icp": "DLR",
                "priority": "HIGH",
                "opportunity_score": 88.5,
                "reason": "Expansion + Permits",
                "timing": "IMMEDIATE",
                "stage": "READY",
                "pipeline_status": "OPEN",
            }
        ],
    }

    first = build_priority_dashboard(payload)
    second = build_priority_dashboard(payload)

    assert first == second


def test_build_priority_dashboard_hard_fails_consistently_for_malformed_input() -> None:
    errors: list[str] = []
    for _ in range(3):
        with pytest.raises(ValueError) as caught:
            build_priority_dashboard({"company_id": "cmp-err", "pipeline_records": {}})
        errors.append(str(caught.value))

    assert errors == [
        "pipeline_records must be a list",
        "pipeline_records must be a list",
        "pipeline_records must be a list",
    ]


def test_build_priority_dashboard_rejects_non_mapping_input() -> None:
    with pytest.raises(ValueError, match="input must be a mapping"):
        build_priority_dashboard(["bad"])


def test_build_priority_dashboard_rejects_missing_company_id() -> None:
    with pytest.raises(ValueError, match="company_id must be a non-empty string"):
        build_priority_dashboard({"pipeline_records": []})


def test_build_priority_dashboard_rejects_non_mapping_record_entry() -> None:
    with pytest.raises(ValueError, match=r"pipeline_records\[0\] must be a mapping"):
        build_priority_dashboard({"company_id": "cmp-001", "pipeline_records": ["bad"]})


def test_build_priority_dashboard_rejects_missing_required_record_field() -> None:
    with pytest.raises(
        ValueError,
        match=r"pipeline_records\[0\] missing required field: timing",
    ):
        build_priority_dashboard(
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
            }
        )


def test_build_priority_dashboard_rejects_invalid_priority_and_stage_status_timing() -> None:
    with pytest.raises(
        ValueError,
        match=r"pipeline_records\[0\]\.priority must be one of: HIGH, MEDIUM, LOW",
    ):
        build_priority_dashboard(
            {
                "company_id": "cmp-001",
                "pipeline_records": [
                    {
                        "company_id": "cmp-001",
                        "icp": "DLR",
                        "priority": "URGENT",
                        "opportunity_score": 90.0,
                        "reason": "Expansion",
                        "timing": "IMMEDIATE",
                        "stage": "READY",
                        "pipeline_status": "OPEN",
                    }
                ],
            }
        )

    with pytest.raises(
        ValueError,
        match=r"pipeline_records\[0\]\.timing must be one of: IMMEDIATE, 7_DAYS, 30_DAYS",
    ):
        build_priority_dashboard(
            {
                "company_id": "cmp-001",
                "pipeline_records": [
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
                ],
            }
        )

    with pytest.raises(
        ValueError,
        match=r"pipeline_records\[0\]\.stage must be READY",
    ):
        build_priority_dashboard(
            {
                "company_id": "cmp-001",
                "pipeline_records": [
                    {
                        "company_id": "cmp-001",
                        "icp": "DLR",
                        "priority": "HIGH",
                        "opportunity_score": 90.0,
                        "reason": "Expansion",
                        "timing": "IMMEDIATE",
                        "stage": "PENDING",
                        "pipeline_status": "OPEN",
                    }
                ],
            }
        )

    with pytest.raises(
        ValueError,
        match=r"pipeline_records\[0\]\.pipeline_status must be OPEN",
    ):
        build_priority_dashboard(
            {
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
                        "pipeline_status": "CLOSED",
                    }
                ],
            }
        )
