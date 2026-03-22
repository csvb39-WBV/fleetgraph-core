from __future__ import annotations

import pathlib
import sys

import pytest


PROJECT_ROOT = pathlib.Path(__file__).resolve().parents[2]
SRC_ROOT = PROJECT_ROOT / "src"

if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))


from fleetgraph_core.intelligence.timing_engine import assign_timing


def test_assign_timing_maps_priority_to_expected_timing() -> None:
    result = assign_timing(
        {
            "company_id": "cmp-001",
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
                    "opportunity_score": 65.5,
                    "reason": "Procurement",
                },
                {
                    "company_id": "cmp-001",
                    "icp": "LEASING",
                    "priority": "LOW",
                    "opportunity_score": 42.0,
                    "reason": "General",
                },
            ],
        }
    )

    assert result == {
        "company_id": "cmp-001",
        "timed_prospects": [
            {
                "company_id": "cmp-001",
                "icp": "DLR",
                "priority": "HIGH",
                "opportunity_score": 91.0,
                "reason": "Expansion",
                "timing": "IMMEDIATE",
            },
            {
                "company_id": "cmp-001",
                "icp": "OEM",
                "priority": "MEDIUM",
                "opportunity_score": 65.5,
                "reason": "Procurement",
                "timing": "7_DAYS",
            },
            {
                "company_id": "cmp-001",
                "icp": "LEASING",
                "priority": "LOW",
                "opportunity_score": 42.0,
                "reason": "General",
                "timing": "30_DAYS",
            },
        ],
    }


def test_assign_timing_preserves_exact_input_order() -> None:
    result = assign_timing(
        {
            "company_id": "cmp-002",
            "prospects": [
                {
                    "company_id": "cmp-002",
                    "icp": "C",
                    "priority": "LOW",
                    "opportunity_score": 40.0,
                    "reason": "r3",
                },
                {
                    "company_id": "cmp-002",
                    "icp": "A",
                    "priority": "HIGH",
                    "opportunity_score": 90.0,
                    "reason": "r1",
                },
                {
                    "company_id": "cmp-002",
                    "icp": "B",
                    "priority": "MEDIUM",
                    "opportunity_score": 60.0,
                    "reason": "r2",
                },
            ],
        }
    )

    assert [entry["icp"] for entry in result["timed_prospects"]] == ["C", "A", "B"]


def test_assign_timing_is_deterministic_for_identical_input() -> None:
    payload = {
        "company_id": "cmp-003",
        "prospects": [
            {
                "company_id": "cmp-003",
                "icp": "UPFITTER",
                "priority": "HIGH",
                "opportunity_score": 88.0,
                "reason": "Permits",
            }
        ],
    }

    first = assign_timing(payload)
    second = assign_timing(payload)

    assert first == second


def test_assign_timing_rejects_non_mapping_input() -> None:
    with pytest.raises(ValueError, match="input must be a mapping"):
        assign_timing(["invalid"])


def test_assign_timing_rejects_missing_or_empty_company_id() -> None:
    with pytest.raises(ValueError, match="company_id must be a non-empty string"):
        assign_timing({"prospects": []})


def test_assign_timing_rejects_non_list_prospects() -> None:
    with pytest.raises(ValueError, match="prospects must be a list"):
        assign_timing({"company_id": "cmp-001", "prospects": {}})


def test_assign_timing_rejects_non_mapping_prospect_entry() -> None:
    with pytest.raises(ValueError, match=r"prospects\[0\] must be a mapping"):
        assign_timing({"company_id": "cmp-001", "prospects": ["bad"]})


def test_assign_timing_rejects_missing_required_prospect_field() -> None:
    with pytest.raises(
        ValueError,
        match=r"prospects\[0\] missing required field: reason",
    ):
        assign_timing(
            {
                "company_id": "cmp-001",
                "prospects": [
                    {
                        "company_id": "cmp-001",
                        "icp": "DLR",
                        "priority": "HIGH",
                        "opportunity_score": 90.0,
                    }
                ],
            }
        )


def test_assign_timing_rejects_invalid_priority() -> None:
    with pytest.raises(
        ValueError,
        match=r"prospects\[0\]\.priority must be one of: HIGH, MEDIUM, LOW",
    ):
        assign_timing(
            {
                "company_id": "cmp-001",
                "prospects": [
                    {
                        "company_id": "cmp-001",
                        "icp": "DLR",
                        "priority": "URGENT",
                        "opportunity_score": 90.0,
                        "reason": "Expansion",
                    }
                ],
            }
        )


def test_assign_timing_rejects_non_numeric_score() -> None:
    with pytest.raises(
        ValueError,
        match=r"prospects\[0\]\.opportunity_score must be numeric",
    ):
        assign_timing(
            {
                "company_id": "cmp-001",
                "prospects": [
                    {
                        "company_id": "cmp-001",
                        "icp": "DLR",
                        "priority": "HIGH",
                        "opportunity_score": "90",
                        "reason": "Expansion",
                    }
                ],
            }
        )


def test_assign_timing_rejects_company_id_mismatch() -> None:
    with pytest.raises(
        ValueError,
        match=r"prospects\[0\]\.company_id must match top-level company_id",
    ):
        assign_timing(
            {
                "company_id": "cmp-001",
                "prospects": [
                    {
                        "company_id": "cmp-999",
                        "icp": "DLR",
                        "priority": "HIGH",
                        "opportunity_score": 90.0,
                        "reason": "Expansion",
                    }
                ],
            }
        )