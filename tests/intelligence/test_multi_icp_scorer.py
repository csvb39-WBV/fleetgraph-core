from __future__ import annotations

import pathlib
import sys

import pytest


PROJECT_ROOT = pathlib.Path(__file__).resolve().parents[2]
SRC_ROOT = PROJECT_ROOT / "src"

if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))


from fleetgraph_core.intelligence.demand_translation_engine import (
    ICP_TRANSLATION_REGISTRY,
)
from fleetgraph_core.intelligence.multi_icp_scorer import (
    SUPPORTED_ICPS,
    score_multi_icp,
)


def test_score_multi_icp_returns_one_result_per_supported_icp() -> None:
    result = score_multi_icp(
        {
            "company_id": "cmp-001",
            "signals": [
                {"signal_category": "EXPANSION", "signal_value": "growth", "valid": True},
                {"signal_category": "PERMITS", "signal_value": 2, "valid": True},
                {"signal_category": "PROCUREMENT", "signal_value": True, "valid": True},
            ],
        }
    )

    assert result["company_id"] == "cmp-001"
    opportunities = result["opportunities"]
    assert isinstance(opportunities, list)
    assert len(opportunities) == len(SUPPORTED_ICPS)

    returned_icps = {entry["icp"] for entry in opportunities}
    assert returned_icps == set(SUPPORTED_ICPS)


def test_supported_icp_set_matches_mb2_exactly() -> None:
    assert set(SUPPORTED_ICPS) == set(ICP_TRANSLATION_REGISTRY.keys())
    assert set(SUPPORTED_ICPS) == {"DLR", "UPFITTER", "LEASING", "OEM"}


def test_score_multi_icp_orders_by_score_desc_then_icp_asc() -> None:
    result = score_multi_icp(
        {
            "company_id": "cmp-002",
            "signals": [
                {"signal_category": "EXPANSION", "signal_value": "growth", "valid": True},
                {"signal_category": "PERMITS", "signal_value": 1, "valid": True},
            ],
        }
    )

    opportunities = result["opportunities"]

    score_pairs = [
        (float(entry["opportunity_score"]), str(entry["icp"])) for entry in opportunities
    ]
    assert score_pairs == sorted(score_pairs, key=lambda item: (-item[0], item[1]))


def test_score_multi_icp_is_deterministic_for_identical_input() -> None:
    payload = {
        "company_id": "cmp-003",
        "signals": [
            {"signal_category": "EXPANSION", "signal_value": "growth", "valid": True},
            {"signal_category": "OPERATIONS", "signal_value": "uptick", "valid": True},
            {"signal_category": "PROCUREMENT", "signal_value": True, "valid": True},
            {"signal_category": "ESG", "signal_value": "report", "valid": False},
        ],
    }

    first = score_multi_icp(payload)
    second = score_multi_icp(payload)

    assert first == second


def test_score_multi_icp_rejects_non_mapping_input() -> None:
    with pytest.raises(ValueError, match="input must be a mapping"):
        score_multi_icp(["invalid"])


def test_score_multi_icp_rejects_missing_company_id() -> None:
    with pytest.raises(ValueError, match="company_id must be a non-empty string"):
        score_multi_icp(
            {
                "signals": [],
            }
        )


def test_score_multi_icp_rejects_non_list_signals() -> None:
    with pytest.raises(ValueError, match="signals must be a list"):
        score_multi_icp(
            {
                "company_id": "cmp-004",
                "signals": {"signal_category": "EXPANSION"},
            }
        )


def test_score_multi_icp_hard_fails_malformed_signal_structure() -> None:
    with pytest.raises(
        ValueError,
        match=r"signals\[0\] missing required field: signal_value",
    ):
        score_multi_icp(
            {
                "company_id": "cmp-005",
                "signals": [
                    {"signal_category": "EXPANSION", "valid": True},
                ],
            }
        )