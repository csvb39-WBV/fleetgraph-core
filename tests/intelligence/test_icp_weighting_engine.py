from __future__ import annotations

import pathlib
import sys

import pytest


PROJECT_ROOT = pathlib.Path(__file__).resolve().parents[2]
SRC_ROOT = PROJECT_ROOT / "src"

if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))


from fleetgraph_core.intelligence.icp_weighting_engine import score_opportunity


def test_score_opportunity_returns_expected_output_for_complete_signal_data() -> None:
    result = score_opportunity(
        icp_matrix={
            "industry": 0.8,
            "location": 0.5,
            "company_size": 0.7,
            "revenue": 0.6,
        },
        signal_data={
            "company": "Company X",
            "industry": "Tech",
            "location": "New York",
            "company_size": "Large",
            "revenue": 50_000_000,
        },
    )

    assert result == {
        "icp": "Tech",
        "opportunity_score": 100.0,
        "reason": "Industry + Location + Company Size + Revenue",
    }


def test_score_opportunity_calculates_weighted_score_for_partial_alignment() -> None:
    result = score_opportunity(
        icp_matrix={
            "industry": 0.8,
            "location": 0.5,
            "company_size": 0.7,
            "revenue": 0.6,
        },
        signal_data={
            "company": "Company X",
            "industry": "Tech",
            "location": "",
            "company_size": "Large",
            "revenue": 0,
        },
    )

    # Matched weights: industry (0.8) + company_size (0.7) = 1.5 out of 2.6.
    assert result == {
        "icp": "Tech",
        "opportunity_score": 57.69,
        "reason": "Industry + Company Size",
    }


def test_score_opportunity_is_deterministic_for_equivalent_inputs() -> None:
    icp_matrix = {
        "industry": 0.8,
        "location": 0.5,
        "company_size": 0.7,
        "revenue": 0.6,
    }
    signal_data = {
        "company": "Company X",
        "industry": "Tech",
        "location": "New York",
        "company_size": "Large",
        "revenue": 50_000_000,
    }

    first = score_opportunity(icp_matrix=icp_matrix, signal_data=signal_data)
    second = score_opportunity(icp_matrix=icp_matrix, signal_data=signal_data)

    assert first == second


@pytest.mark.parametrize("value", [None, "invalid", 123])
def test_score_opportunity_rejects_non_mapping_icp_matrix(value: object) -> None:
    with pytest.raises(ValueError, match="icp_matrix must be a mapping"):
        score_opportunity(icp_matrix=value, signal_data={})


@pytest.mark.parametrize("value", [None, "invalid", 123])
def test_score_opportunity_rejects_non_mapping_signal_data(value: object) -> None:
    with pytest.raises(ValueError, match="signal_data must be a mapping"):
        score_opportunity(icp_matrix={}, signal_data=value)


def test_score_opportunity_rejects_missing_required_icp_field() -> None:
    with pytest.raises(
        ValueError,
        match="icp_matrix is missing required fields: revenue",
    ):
        score_opportunity(
            icp_matrix={
                "industry": 0.8,
                "location": 0.5,
                "company_size": 0.7,
            },
            signal_data={
                "industry": "Tech",
                "location": "New York",
                "company_size": "Large",
                "revenue": 50_000_000,
            },
        )


def test_score_opportunity_rejects_missing_required_signal_field() -> None:
    with pytest.raises(
        ValueError,
        match="signal_data is missing required fields: company_size",
    ):
        score_opportunity(
            icp_matrix={
                "industry": 0.8,
                "location": 0.5,
                "company_size": 0.7,
                "revenue": 0.6,
            },
            signal_data={
                "industry": "Tech",
                "location": "New York",
                "revenue": 50_000_000,
            },
        )


def test_score_opportunity_rejects_non_numeric_weight() -> None:
    with pytest.raises(
        ValueError,
        match=r"icp_matrix\[industry\] must be a numeric weight",
    ):
        score_opportunity(
            icp_matrix={
                "industry": "high",
                "location": 0.5,
                "company_size": 0.7,
                "revenue": 0.6,
            },
            signal_data={
                "industry": "Tech",
                "location": "New York",
                "company_size": "Large",
                "revenue": 50_000_000,
            },
        )


def test_score_opportunity_rejects_negative_weight() -> None:
    with pytest.raises(
        ValueError,
        match=r"icp_matrix\[revenue\] must be greater than or equal to 0",
    ):
        score_opportunity(
            icp_matrix={
                "industry": 0.8,
                "location": 0.5,
                "company_size": 0.7,
                "revenue": -0.1,
            },
            signal_data={
                "industry": "Tech",
                "location": "New York",
                "company_size": "Large",
                "revenue": 50_000_000,
            },
        )


def test_score_opportunity_returns_no_alignment_reason_when_all_values_missing() -> None:
    result = score_opportunity(
        icp_matrix={
            "industry": 0.8,
            "location": 0.5,
            "company_size": 0.7,
            "revenue": 0.6,
        },
        signal_data={
            "industry": "",
            "location": "",
            "company_size": "",
            "revenue": 0,
        },
    )

    assert result == {
        "icp": "",
        "opportunity_score": 0.0,
        "reason": "No ICP alignment",
    }