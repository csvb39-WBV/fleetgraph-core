from __future__ import annotations

import pathlib
import sys

import pytest


PROJECT_ROOT = pathlib.Path(__file__).resolve().parents[2]
SRC_ROOT = PROJECT_ROOT / "src"

if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))


from fleetgraph_core.intelligence.outreach_generator import generate_outreach


def test_generate_outreach_returns_one_record_per_prospect() -> None:
    payload = {
        "company_id": "cmp-001",
        "prospects": [
            {
                "company_id": "cmp-001",
                "icp": "DLR",
                "priority": "HIGH",
                "opportunity_score": 92.0,
                "reason": "Expansion + Permits",
            },
            {
                "company_id": "cmp-001",
                "icp": "OEM",
                "priority": "MEDIUM",
                "opportunity_score": 65.0,
                "reason": "Procurement",
            },
        ],
    }

    result = generate_outreach(payload)

    assert result["company_id"] == "cmp-001"
    outreach = result["outreach"]
    assert isinstance(outreach, list)
    assert len(outreach) == 2


def test_generate_outreach_includes_required_fields_and_reason_reference() -> None:
    reason = "Expansion + Hiring"
    result = generate_outreach(
        {
            "company_id": "cmp-001",
            "prospects": [
                {
                    "company_id": "cmp-001",
                    "icp": "LEASING",
                    "priority": "HIGH",
                    "opportunity_score": 87.5,
                    "reason": reason,
                }
            ],
        }
    )

    entry = result["outreach"][0]
    assert set(entry.keys()) == {"icp", "priority", "subject", "message", "talk_track"}
    assert entry["icp"] == "LEASING"
    assert entry["priority"] == "HIGH"
    assert reason in entry["subject"]
    assert reason in entry["message"]
    assert reason in entry["talk_track"]


def test_generate_outreach_is_deterministic_for_identical_input() -> None:
    payload = {
        "company_id": "cmp-002",
        "prospects": [
            {
                "company_id": "cmp-002",
                "icp": "UPFITTER",
                "priority": "LOW",
                "opportunity_score": 40.0,
                "reason": "No ICP-aligned signals",
            }
        ],
    }

    first = generate_outreach(payload)
    second = generate_outreach(payload)

    assert first == second


def test_generate_outreach_rejects_non_mapping_input() -> None:
    with pytest.raises(ValueError, match="input must be a mapping"):
        generate_outreach(["bad"])


def test_generate_outreach_rejects_missing_or_empty_company_id() -> None:
    with pytest.raises(ValueError, match="company_id must be a non-empty string"):
        generate_outreach({"prospects": []})


def test_generate_outreach_rejects_non_list_prospects() -> None:
    with pytest.raises(ValueError, match="prospects must be a list"):
        generate_outreach({"company_id": "cmp-001", "prospects": {}})


def test_generate_outreach_rejects_non_mapping_prospect_entry() -> None:
    with pytest.raises(ValueError, match=r"prospects\[0\] must be a mapping"):
        generate_outreach({"company_id": "cmp-001", "prospects": ["bad"]})


def test_generate_outreach_rejects_missing_required_prospect_field() -> None:
    with pytest.raises(
        ValueError,
        match=r"prospects\[0\] missing required field: reason",
    ):
        generate_outreach(
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


def test_generate_outreach_rejects_invalid_priority() -> None:
    with pytest.raises(
        ValueError,
        match=r"prospects\[0\]\.priority must be one of: HIGH, MEDIUM, LOW",
    ):
        generate_outreach(
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


def test_generate_outreach_rejects_company_id_mismatch() -> None:
    with pytest.raises(
        ValueError,
        match=r"prospects\[0\]\.company_id must match top-level company_id",
    ):
        generate_outreach(
            {
                "company_id": "cmp-001",
                "prospects": [
                    {
                        "company_id": "cmp-002",
                        "icp": "DLR",
                        "priority": "HIGH",
                        "opportunity_score": 90.0,
                        "reason": "Expansion",
                    }
                ],
            }
        )