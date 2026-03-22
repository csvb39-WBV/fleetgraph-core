from __future__ import annotations

import pathlib
import sys

import pytest


PROJECT_ROOT = pathlib.Path(__file__).resolve().parents[2]
SRC_ROOT = PROJECT_ROOT / "src"

if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))


from fleetgraph_core.intelligence.multi_icp_scorer import score_multi_icp
from fleetgraph_core.intelligence.outreach_generator import generate_outreach
from fleetgraph_core.intelligence.pipeline_tracker import build_pipeline_records
from fleetgraph_core.intelligence.prospect_engine import build_prospects
from fleetgraph_core.intelligence.signal_aggregator import aggregate_signals
from fleetgraph_core.intelligence.timing_engine import assign_timing


def test_contract_alignment_across_all_required_boundaries() -> None:
    company_id = "cmp-contract-001"
    raw_signals = [
        {"source": "EXPANSION", "data": {"evidence": "new facility"}},
        {"source": "PERMITS", "data": {"count": 2}},
        {"source": "PROCUREMENT", "data": {"rfp": True}},
    ]

    # Boundary 1: signal_aggregator -> multi_icp_scorer
    aggregate_output = aggregate_signals(raw_signals)
    assert set(aggregate_output.keys()) == {"aggregated_signals"}
    assert isinstance(aggregate_output["aggregated_signals"], list)
    assert len(aggregate_output["aggregated_signals"]) == len(raw_signals)
    for entry in aggregate_output["aggregated_signals"]:
        assert set(entry.keys()) == {"source", "normalized_data", "valid"}
        assert isinstance(entry["source"], str)
        assert isinstance(entry["normalized_data"], dict)
        assert isinstance(entry["valid"], bool)

    scorer_signals = [
        {
            "signal_category": str(entry["source"]),
            "signal_value": str(entry["normalized_data"]),
            "valid": bool(entry["valid"]),
        }
        for entry in aggregate_output["aggregated_signals"]
    ]
    for entry in scorer_signals:
        assert set(entry.keys()) == {"signal_category", "signal_value", "valid"}
        assert isinstance(entry["signal_category"], str)
        assert isinstance(entry["signal_value"], str)
        assert isinstance(entry["valid"], bool)

    # Determinism check on this boundary.
    scorer_payload = {"company_id": company_id, "signals": scorer_signals}
    multi_icp_first = score_multi_icp(scorer_payload)
    multi_icp_second = score_multi_icp(scorer_payload)
    assert multi_icp_first == multi_icp_second

    # Boundary 2: multi_icp_scorer -> prospect_engine
    assert set(multi_icp_first.keys()) == {"company_id", "opportunities"}
    assert multi_icp_first["company_id"] == company_id
    assert isinstance(multi_icp_first["opportunities"], list)
    assert len(multi_icp_first["opportunities"]) > 0
    for opportunity in multi_icp_first["opportunities"]:
        assert set(opportunity.keys()) == {"icp", "opportunity_score", "reason"}
        assert isinstance(opportunity["icp"], str)
        assert isinstance(opportunity["opportunity_score"], (int, float))
        assert isinstance(opportunity["reason"], str)

    prospects_output = build_prospects(
        {
            "company_id": multi_icp_first["company_id"],
            "opportunities": multi_icp_first["opportunities"],
        }
    )

    # Boundary 3: prospect_engine -> outreach_generator
    assert set(prospects_output.keys()) == {"company_id", "prospects"}
    assert prospects_output["company_id"] == company_id
    assert isinstance(prospects_output["prospects"], list)
    for prospect in prospects_output["prospects"]:
        assert set(prospect.keys()) == {
            "company_id",
            "icp",
            "priority",
            "opportunity_score",
            "reason",
        }
        assert isinstance(prospect["company_id"], str)
        assert isinstance(prospect["icp"], str)
        assert isinstance(prospect["priority"], str)
        assert isinstance(prospect["opportunity_score"], (int, float))
        assert isinstance(prospect["reason"], str)

    outreach_output = generate_outreach(
        {
            "company_id": prospects_output["company_id"],
            "prospects": prospects_output["prospects"],
        }
    )
    assert set(outreach_output.keys()) == {"company_id", "outreach"}
    assert outreach_output["company_id"] == company_id
    assert isinstance(outreach_output["outreach"], list)
    for item in outreach_output["outreach"]:
        assert set(item.keys()) == {"icp", "priority", "subject", "message", "talk_track"}

    # Boundary 4: prospect_engine -> timing_engine
    timed_output = assign_timing(
        {
            "company_id": prospects_output["company_id"],
            "prospects": prospects_output["prospects"],
        }
    )
    assert set(timed_output.keys()) == {"company_id", "timed_prospects"}
    assert timed_output["company_id"] == company_id
    assert isinstance(timed_output["timed_prospects"], list)
    for timed in timed_output["timed_prospects"]:
        assert set(timed.keys()) == {
            "company_id",
            "icp",
            "priority",
            "opportunity_score",
            "reason",
            "timing",
        }

    # Boundary 5: timing_engine -> pipeline_tracker
    pipeline_output = build_pipeline_records(
        {
            "company_id": timed_output["company_id"],
            "timed_prospects": timed_output["timed_prospects"],
        }
    )
    assert set(pipeline_output.keys()) == {"company_id", "pipeline_records"}
    assert pipeline_output["company_id"] == company_id
    assert isinstance(pipeline_output["pipeline_records"], list)
    for record in pipeline_output["pipeline_records"]:
        assert set(record.keys()) == {
            "company_id",
            "icp",
            "priority",
            "opportunity_score",
            "reason",
            "timing",
            "stage",
            "pipeline_status",
        }


def test_contract_hard_failures_for_missing_fields_wrong_types_and_malformed_structures() -> None:
    with pytest.raises(ValueError, match="signals must be a list"):
        score_multi_icp({"company_id": "cmp-001", "signals": {"bad": True}})

    with pytest.raises(ValueError, match=r"signals\[0\] missing required field: signal_value"):
        score_multi_icp(
            {
                "company_id": "cmp-001",
                "signals": [{"signal_category": "EXPANSION", "valid": True}],
            }
        )

    with pytest.raises(ValueError, match="opportunities must be a list"):
        build_prospects({"company_id": "cmp-001", "opportunities": {"bad": True}})

    with pytest.raises(ValueError, match=r"prospects\[0\]\.priority must be one of: HIGH, MEDIUM, LOW"):
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

    with pytest.raises(ValueError, match=r"prospects\[0\] missing required field: reason"):
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