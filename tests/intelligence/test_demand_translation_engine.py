from __future__ import annotations

import pathlib
import sys

import pytest


PROJECT_ROOT = pathlib.Path(__file__).resolve().parents[2]
SRC_ROOT = PROJECT_ROOT / "src"

if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))


from fleetgraph_core.intelligence.demand_translation_engine import translate_demand


def test_translate_demand_returns_expected_output_for_dlr() -> None:
    result = translate_demand(
        {
            "company_id": "cmp-001",
            "icp_type": "DLR",
            "signals": [
                {"signal_category": "EXPANSION", "signal_value": "new site", "valid": True},
                {"signal_category": "PERMITS", "signal_value": 3, "valid": True},
            ],
        }
    )

    # DLR max weight = 1.0 + 0.9 + 0.8 + 0.7 = 3.4, matched = 1.8
    assert result == {
        "icp": "DLR",
        "opportunity_score": 52.94,
        "reason": "Expansion + Permits",
    }


def test_translate_demand_excludes_valid_false_signals() -> None:
    result = translate_demand(
        {
            "company_id": "cmp-001",
            "icp_type": "OEM",
            "signals": [
                {"signal_category": "PROCUREMENT", "signal_value": True, "valid": False},
                {"signal_category": "EXPANSION", "signal_value": "growth", "valid": True},
            ],
        }
    )

    # OEM max weight = 1.0 + 0.9 + 0.7 + 0.6 = 3.2, matched = 0.9
    assert result == {
        "icp": "OEM",
        "opportunity_score": 28.12,
        "reason": "Expansion",
    }


def test_translate_demand_returns_no_alignment_reason_when_no_match() -> None:
    result = translate_demand(
        {
            "company_id": "cmp-001",
            "icp_type": "LEASING",
            "signals": [
                {"signal_category": "COMPLIANCE", "signal_value": "filed", "valid": True},
            ],
        }
    )

    assert result == {
        "icp": "LEASING",
        "opportunity_score": 0.0,
        "reason": "No ICP-aligned signals",
    }


def test_translate_demand_is_deterministic_for_identical_input() -> None:
    payload = {
        "company_id": "cmp-001",
        "icp_type": "UPFITTER",
        "signals": [
            {"signal_category": "PERMITS", "signal_value": 1, "valid": True},
            {"signal_category": "EXPANSION", "signal_value": "opening", "valid": True},
            {"signal_category": "PERMITS", "signal_value": 2, "valid": True},
        ],
    }

    first = translate_demand(payload)
    second = translate_demand(payload)

    assert first == second


def test_translate_demand_rejects_non_mapping_input() -> None:
    with pytest.raises(ValueError, match="input must be a mapping"):
        translate_demand(["invalid"])


def test_translate_demand_rejects_missing_or_empty_company_id() -> None:
    with pytest.raises(ValueError, match="company_id must be a non-empty string"):
        translate_demand(
            {
                "icp_type": "DLR",
                "signals": [],
            }
        )


def test_translate_demand_rejects_missing_or_empty_icp_type() -> None:
    with pytest.raises(ValueError, match="icp_type must be a non-empty string"):
        translate_demand(
            {
                "company_id": "cmp-001",
                "signals": [],
                "icp_type": "   ",
            }
        )


def test_translate_demand_rejects_non_list_signals() -> None:
    with pytest.raises(ValueError, match="signals must be a list"):
        translate_demand(
            {
                "company_id": "cmp-001",
                "icp_type": "DLR",
                "signals": {"signal_category": "EXPANSION"},
            }
        )


def test_translate_demand_rejects_unsupported_icp() -> None:
    with pytest.raises(ValueError, match="unsupported icp_type: UNKNOWN"):
        translate_demand(
            {
                "company_id": "cmp-001",
                "icp_type": "UNKNOWN",
                "signals": [],
            }
        )


def test_translate_demand_rejects_non_mapping_signal() -> None:
    with pytest.raises(ValueError, match=r"signals\[0\] must be a mapping"):
        translate_demand(
            {
                "company_id": "cmp-001",
                "icp_type": "DLR",
                "signals": ["bad"],
            }
        )


def test_translate_demand_rejects_signal_missing_required_field() -> None:
    with pytest.raises(
        ValueError,
        match=r"signals\[0\] missing required field: signal_value",
    ):
        translate_demand(
            {
                "company_id": "cmp-001",
                "icp_type": "DLR",
                "signals": [
                    {"signal_category": "EXPANSION", "valid": True},
                ],
            }
        )


def test_translate_demand_rejects_signal_valid_not_boolean() -> None:
    with pytest.raises(ValueError, match=r"signals\[0\]\.valid must be a boolean"):
        translate_demand(
            {
                "company_id": "cmp-001",
                "icp_type": "DLR",
                "signals": [
                    {
                        "signal_category": "EXPANSION",
                        "signal_value": "growth",
                        "valid": "yes",
                    },
                ],
            }
        )


def test_translate_demand_rejects_signal_value_invalid_type() -> None:
    with pytest.raises(
        ValueError,
        match=r"signals\[0\]\.signal_value must be string, int, float, or bool",
    ):
        translate_demand(
            {
                "company_id": "cmp-001",
                "icp_type": "DLR",
                "signals": [
                    {
                        "signal_category": "EXPANSION",
                        "signal_value": {"bad": "type"},
                        "valid": True,
                    },
                ],
            }
        )
