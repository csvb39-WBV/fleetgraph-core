from __future__ import annotations

import pathlib
import sys

import pytest


PROJECT_ROOT = pathlib.Path(__file__).resolve().parents[2]
SRC_ROOT = PROJECT_ROOT / "src"

if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))


from fleetgraph_core.intelligence.prospect_engine import build_prospects


def test_build_prospects_returns_one_prospect_per_opportunity() -> None:
    result = build_prospects(
        {
            "company_id": "cmp-001",
            "opportunities": [
                {"icp": "DLR", "opportunity_score": 92.0, "reason": "Expansion"},
                {"icp": "OEM", "opportunity_score": 41.0, "reason": "Procurement"},
            ],
        }
    )

    assert result["company_id"] == "cmp-001"
    prospects = result["prospects"]
    assert isinstance(prospects, list)
    assert len(prospects) == 2


def test_build_prospects_applies_priority_thresholds_exactly() -> None:
    result = build_prospects(
        {
            "company_id": "cmp-002",
            "opportunities": [
                {"icp": "A", "opportunity_score": 80.0, "reason": "r1"},
                {"icp": "B", "opportunity_score": 79.99, "reason": "r2"},
                {"icp": "C", "opportunity_score": 50.0, "reason": "r3"},
                {"icp": "D", "opportunity_score": 49.99, "reason": "r4"},
            ],
        }
    )

    priorities = {entry["icp"]: entry["priority"] for entry in result["prospects"]}
    assert priorities == {
        "A": "HIGH",
        "B": "MEDIUM",
        "C": "MEDIUM",
        "D": "LOW",
    }


def test_build_prospects_orders_by_priority_then_score_then_icp() -> None:
    result = build_prospects(
        {
            "company_id": "cmp-003",
            "opportunities": [
                {"icp": "OEM", "opportunity_score": 80.0, "reason": "r"},
                {"icp": "DLR", "opportunity_score": 92.0, "reason": "r"},
                {"icp": "AAA", "opportunity_score": 80.0, "reason": "r"},
                {"icp": "UPFITTER", "opportunity_score": 55.0, "reason": "r"},
                {"icp": "LEASING", "opportunity_score": 40.0, "reason": "r"},
            ],
        }
    )

    assert result["prospects"] == [
        {
            "company_id": "cmp-003",
            "icp": "DLR",
            "priority": "HIGH",
            "opportunity_score": 92.0,
            "reason": "r",
        },
        {
            "company_id": "cmp-003",
            "icp": "AAA",
            "priority": "HIGH",
            "opportunity_score": 80.0,
            "reason": "r",
        },
        {
            "company_id": "cmp-003",
            "icp": "OEM",
            "priority": "HIGH",
            "opportunity_score": 80.0,
            "reason": "r",
        },
        {
            "company_id": "cmp-003",
            "icp": "UPFITTER",
            "priority": "MEDIUM",
            "opportunity_score": 55.0,
            "reason": "r",
        },
        {
            "company_id": "cmp-003",
            "icp": "LEASING",
            "priority": "LOW",
            "opportunity_score": 40.0,
            "reason": "r",
        },
    ]


def test_build_prospects_is_deterministic_for_identical_input() -> None:
    payload = {
        "company_id": "cmp-004",
        "opportunities": [
            {"icp": "OEM", "opportunity_score": 70.5, "reason": "Ops"},
            {"icp": "DLR", "opportunity_score": 92.0, "reason": "Expansion"},
        ],
    }

    first = build_prospects(payload)
    second = build_prospects(payload)

    assert first == second


def test_build_prospects_rejects_non_mapping_input() -> None:
    with pytest.raises(ValueError, match="input must be a mapping"):
        build_prospects(["bad"])


def test_build_prospects_rejects_missing_or_empty_company_id() -> None:
    with pytest.raises(ValueError, match="company_id must be a non-empty string"):
        build_prospects({"opportunities": []})


def test_build_prospects_rejects_non_list_opportunities() -> None:
    with pytest.raises(ValueError, match="opportunities must be a list"):
        build_prospects({"company_id": "cmp-001", "opportunities": {}})


def test_build_prospects_rejects_non_mapping_opportunity_entry() -> None:
    with pytest.raises(ValueError, match=r"opportunities\[0\] must be a mapping"):
        build_prospects({"company_id": "cmp-001", "opportunities": ["bad"]})


def test_build_prospects_rejects_missing_required_opportunity_field() -> None:
    with pytest.raises(
        ValueError,
        match=r"opportunities\[0\] missing required field: reason",
    ):
        build_prospects(
            {
                "company_id": "cmp-001",
                "opportunities": [{"icp": "DLR", "opportunity_score": 10.0}],
            }
        )


def test_build_prospects_rejects_invalid_opportunity_field_types() -> None:
    with pytest.raises(
        ValueError,
        match=r"opportunities\[0\]\.opportunity_score must be numeric",
    ):
        build_prospects(
            {
                "company_id": "cmp-001",
                "opportunities": [
                    {"icp": "DLR", "opportunity_score": "high", "reason": "r"}
                ],
            }
        )