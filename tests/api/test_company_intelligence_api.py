from __future__ import annotations

import pathlib
import sys

import pytest


PROJECT_ROOT = pathlib.Path(__file__).resolve().parents[2]
SRC_ROOT = PROJECT_ROOT / "src"

if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))


from fleetgraph_core.api.company_intelligence_api import build_company_intelligence


def _valid_payload() -> dict[str, object]:
    return {
        "company_id": "cmp-001",
        "opportunities": [
            {"icp": "DLR", "opportunity_score": 91.0, "reason": "Expansion"},
            {"icp": "OEM", "opportunity_score": 65.0, "reason": "Procurement"},
        ],
        "prospects": [
            {
                "company_id": "cmp-001",
                "icp": "DLR",
                "priority": "HIGH",
                "opportunity_score": 91.0,
                "reason": "Expansion",
            },
            {
                "company_id": "cmp-001",
                "icp": "OEM",
                "priority": "MEDIUM",
                "opportunity_score": 65.0,
                "reason": "Procurement",
            },
        ],
        "pipeline_records": [
            {
                "company_id": "cmp-001",
                "icp": "DLR",
                "priority": "HIGH",
                "opportunity_score": 91.0,
                "reason": "Expansion",
                "timing": "IMMEDIATE",
                "stage": "READY",
                "pipeline_status": "OPEN",
            },
            {
                "company_id": "cmp-001",
                "icp": "OEM",
                "priority": "MEDIUM",
                "opportunity_score": 65.0,
                "reason": "Procurement",
                "timing": "7_DAYS",
                "stage": "READY",
                "pipeline_status": "OPEN",
            },
        ],
    }


def test_build_company_intelligence_returns_expected_shape_counts_and_metrics() -> None:
    result = build_company_intelligence(_valid_payload())

    assert set(result.keys()) == {
        "company_id",
        "company_intelligence",
        "opportunities",
        "prospects",
        "pipeline_records",
    }
    assert result["company_id"] == "cmp-001"
    assert result["company_intelligence"] == {
        "opportunity_count": 2,
        "prospect_count": 2,
        "pipeline_record_count": 2,
        "highest_opportunity_score": 91.0,
        "highest_priority": "HIGH",
    }


def test_build_company_intelligence_preserves_exact_input_order() -> None:
    payload = _valid_payload()
    payload["opportunities"] = [
        {"icp": "B", "opportunity_score": 10.0, "reason": "r2"},
        {"icp": "A", "opportunity_score": 20.0, "reason": "r1"},
    ]
    payload["prospects"] = [
        {
            "company_id": "cmp-001",
            "icp": "B",
            "priority": "LOW",
            "opportunity_score": 10.0,
            "reason": "r2",
        },
        {
            "company_id": "cmp-001",
            "icp": "A",
            "priority": "MEDIUM",
            "opportunity_score": 20.0,
            "reason": "r1",
        },
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
            "priority": "MEDIUM",
            "opportunity_score": 20.0,
            "reason": "r1",
            "timing": "7_DAYS",
            "stage": "READY",
            "pipeline_status": "OPEN",
        },
    ]

    result = build_company_intelligence(payload)

    assert [item["icp"] for item in result["opportunities"]] == ["B", "A"]
    assert [item["icp"] for item in result["prospects"]] == ["B", "A"]
    assert [item["icp"] for item in result["pipeline_records"]] == ["B", "A"]


def test_build_company_intelligence_uses_fallback_metrics_when_no_data() -> None:
    payload = {
        "company_id": "cmp-002",
        "opportunities": [],
        "prospects": [],
        "pipeline_records": [],
    }

    result = build_company_intelligence(payload)

    assert result["company_intelligence"] == {
        "opportunity_count": 0,
        "prospect_count": 0,
        "pipeline_record_count": 0,
        "highest_opportunity_score": 0.0,
        "highest_priority": "NONE",
    }


def test_build_company_intelligence_is_deterministic_for_identical_input() -> None:
    payload = _valid_payload()

    first = build_company_intelligence(payload)
    second = build_company_intelligence(payload)

    assert first == second


def test_build_company_intelligence_has_stable_failure_behavior() -> None:
    errors: list[str] = []
    bad_payload = {
        "company_id": "cmp-err",
        "opportunities": {},
        "prospects": [],
        "pipeline_records": [],
    }
    for _ in range(3):
        with pytest.raises(ValueError) as caught:
            build_company_intelligence(bad_payload)
        errors.append(str(caught.value))

    assert errors == [
        "opportunities must be a list",
        "opportunities must be a list",
        "opportunities must be a list",
    ]


def test_build_company_intelligence_rejects_invalid_top_level_shapes() -> None:
    with pytest.raises(ValueError, match="input must be a mapping"):
        build_company_intelligence(["bad"])

    with pytest.raises(ValueError, match="company_id must be a non-empty string"):
        build_company_intelligence(
            {
                "opportunities": [],
                "prospects": [],
                "pipeline_records": [],
            }
        )

    with pytest.raises(ValueError, match="prospects must be a list"):
        build_company_intelligence(
            {
                "company_id": "cmp-001",
                "opportunities": [],
                "prospects": {},
                "pipeline_records": [],
            }
        )

    with pytest.raises(ValueError, match="pipeline_records must be a list"):
        build_company_intelligence(
            {
                "company_id": "cmp-001",
                "opportunities": [],
                "prospects": [],
                "pipeline_records": {},
            }
        )


def test_build_company_intelligence_rejects_invalid_opportunity_prospect_and_pipeline_records() -> None:
    with pytest.raises(ValueError, match=r"opportunities\[0\] missing required field: reason"):
        payload = _valid_payload()
        payload["opportunities"] = [{"icp": "DLR", "opportunity_score": 90.0}]
        build_company_intelligence(payload)

    with pytest.raises(
        ValueError,
        match=r"prospects\[0\]\.company_id must match top-level company_id",
    ):
        payload = _valid_payload()
        payload["prospects"] = [
            {
                "company_id": "cmp-other",
                "icp": "DLR",
                "priority": "HIGH",
                "opportunity_score": 90.0,
                "reason": "Expansion",
            }
        ]
        build_company_intelligence(payload)

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
        build_company_intelligence(payload)
