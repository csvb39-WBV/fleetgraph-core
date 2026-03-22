from __future__ import annotations

import pathlib
import sys

import pytest


PROJECT_ROOT = pathlib.Path(__file__).resolve().parents[2]
SRC_ROOT = PROJECT_ROOT / "src"

if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))


from fleetgraph_core.intelligence.pipeline_tracker import build_pipeline_records


def test_build_pipeline_records_generates_one_record_per_timed_prospect() -> None:
    result = build_pipeline_records(
        {
            "company_id": "cmp-001",
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
                    "opportunity_score": 63.5,
                    "reason": "Procurement",
                    "timing": "7_DAYS",
                },
            ],
        }
    )

    assert result["company_id"] == "cmp-001"
    records = result["pipeline_records"]
    assert isinstance(records, list)
    assert len(records) == 2


def test_build_pipeline_records_applies_deterministic_defaults() -> None:
    result = build_pipeline_records(
        {
            "company_id": "cmp-002",
            "timed_prospects": [
                {
                    "company_id": "cmp-002",
                    "icp": "LEASING",
                    "priority": "LOW",
                    "opportunity_score": 42.0,
                    "reason": "General",
                    "timing": "30_DAYS",
                }
            ],
        }
    )

    assert result["pipeline_records"][0]["stage"] == "READY"
    assert result["pipeline_records"][0]["pipeline_status"] == "OPEN"


def test_build_pipeline_records_preserves_exact_input_order() -> None:
    result = build_pipeline_records(
        {
            "company_id": "cmp-003",
            "timed_prospects": [
                {
                    "company_id": "cmp-003",
                    "icp": "C",
                    "priority": "LOW",
                    "opportunity_score": 30.0,
                    "reason": "r3",
                    "timing": "30_DAYS",
                },
                {
                    "company_id": "cmp-003",
                    "icp": "A",
                    "priority": "HIGH",
                    "opportunity_score": 95.0,
                    "reason": "r1",
                    "timing": "IMMEDIATE",
                },
                {
                    "company_id": "cmp-003",
                    "icp": "B",
                    "priority": "MEDIUM",
                    "opportunity_score": 60.0,
                    "reason": "r2",
                    "timing": "7_DAYS",
                },
            ],
        }
    )

    assert [entry["icp"] for entry in result["pipeline_records"]] == ["C", "A", "B"]


def test_build_pipeline_records_is_deterministic_for_identical_input() -> None:
    payload = {
        "company_id": "cmp-004",
        "timed_prospects": [
            {
                "company_id": "cmp-004",
                "icp": "UPFITTER",
                "priority": "HIGH",
                "opportunity_score": 88.0,
                "reason": "Permits",
                "timing": "IMMEDIATE",
            }
        ],
    }

    first = build_pipeline_records(payload)
    second = build_pipeline_records(payload)

    assert first == second


def test_build_pipeline_records_rejects_non_mapping_input() -> None:
    with pytest.raises(ValueError, match="input must be a mapping"):
        build_pipeline_records(["bad"])


def test_build_pipeline_records_rejects_missing_or_empty_company_id() -> None:
    with pytest.raises(ValueError, match="company_id must be a non-empty string"):
        build_pipeline_records({"timed_prospects": []})


def test_build_pipeline_records_rejects_non_list_timed_prospects() -> None:
    with pytest.raises(ValueError, match="timed_prospects must be a list"):
        build_pipeline_records({"company_id": "cmp-001", "timed_prospects": {}})


def test_build_pipeline_records_rejects_non_mapping_timed_prospect() -> None:
    with pytest.raises(ValueError, match=r"timed_prospects\[0\] must be a mapping"):
        build_pipeline_records({"company_id": "cmp-001", "timed_prospects": ["bad"]})


def test_build_pipeline_records_rejects_missing_required_field() -> None:
    with pytest.raises(
        ValueError,
        match=r"timed_prospects\[0\] missing required field: timing",
    ):
        build_pipeline_records(
            {
                "company_id": "cmp-001",
                "timed_prospects": [
                    {
                        "company_id": "cmp-001",
                        "icp": "DLR",
                        "priority": "HIGH",
                        "opportunity_score": 90.0,
                        "reason": "Expansion",
                    }
                ],
            }
        )


def test_build_pipeline_records_rejects_invalid_priority() -> None:
    with pytest.raises(
        ValueError,
        match=r"timed_prospects\[0\]\.priority must be one of: HIGH, MEDIUM, LOW",
    ):
        build_pipeline_records(
            {
                "company_id": "cmp-001",
                "timed_prospects": [
                    {
                        "company_id": "cmp-001",
                        "icp": "DLR",
                        "priority": "URGENT",
                        "opportunity_score": 90.0,
                        "reason": "Expansion",
                        "timing": "IMMEDIATE",
                    }
                ],
            }
        )


def test_build_pipeline_records_rejects_invalid_timing() -> None:
    with pytest.raises(
        ValueError,
        match=r"timed_prospects\[0\]\.timing must be one of: IMMEDIATE, 7_DAYS, 30_DAYS",
    ):
        build_pipeline_records(
            {
                "company_id": "cmp-001",
                "timed_prospects": [
                    {
                        "company_id": "cmp-001",
                        "icp": "DLR",
                        "priority": "HIGH",
                        "opportunity_score": 90.0,
                        "reason": "Expansion",
                        "timing": "14_DAYS",
                    }
                ],
            }
        )


def test_build_pipeline_records_rejects_non_numeric_score() -> None:
    with pytest.raises(
        ValueError,
        match=r"timed_prospects\[0\]\.opportunity_score must be numeric",
    ):
        build_pipeline_records(
            {
                "company_id": "cmp-001",
                "timed_prospects": [
                    {
                        "company_id": "cmp-001",
                        "icp": "DLR",
                        "priority": "HIGH",
                        "opportunity_score": "90",
                        "reason": "Expansion",
                        "timing": "IMMEDIATE",
                    }
                ],
            }
        )


def test_build_pipeline_records_rejects_company_id_mismatch() -> None:
    with pytest.raises(
        ValueError,
        match=r"timed_prospects\[0\]\.company_id must match top-level company_id",
    ):
        build_pipeline_records(
            {
                "company_id": "cmp-001",
                "timed_prospects": [
                    {
                        "company_id": "cmp-999",
                        "icp": "DLR",
                        "priority": "HIGH",
                        "opportunity_score": 90.0,
                        "reason": "Expansion",
                        "timing": "IMMEDIATE",
                    }
                ],
            }
        )