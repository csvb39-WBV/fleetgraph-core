from __future__ import annotations

import pathlib
import sys


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


def _run_pipeline(company_id: str, raw_signals: list[dict[str, object]]) -> dict[str, object]:
    aggregated = aggregate_signals(raw_signals)

    # Bridge normalized aggregate shape into the scorer contract.
    scored_signals = [
        {
            "signal_category": str(entry["source"]),
            "signal_value": str(entry["normalized_data"]),
            "valid": bool(entry["valid"]),
        }
        for entry in aggregated["aggregated_signals"]
    ]

    multi_icp = score_multi_icp(
        {
            "company_id": company_id,
            "signals": scored_signals,
        }
    )
    prospects = build_prospects(
        {
            "company_id": company_id,
            "opportunities": multi_icp["opportunities"],
        }
    )
    outreach = generate_outreach(
        {
            "company_id": company_id,
            "prospects": prospects["prospects"],
        }
    )
    timed = assign_timing(
        {
            "company_id": company_id,
            "prospects": prospects["prospects"],
        }
    )
    pipeline = build_pipeline_records(
        {
            "company_id": company_id,
            "timed_prospects": timed["timed_prospects"],
        }
    )

    return {
        "aggregated": aggregated,
        "multi_icp": multi_icp,
        "prospects": prospects,
        "outreach": outreach,
        "timed": timed,
        "pipeline": pipeline,
    }


def test_end_to_end_pipeline_is_deterministic_and_structurally_valid() -> None:
    company_id = "cmp-e2e-001"
    raw_signals = [
        {
            "source": "EXPANSION",
            "data": {"evidence": "new site opening"},
        },
        {
            "source": "PERMITS",
            "data": {"count": 3},
        },
        {
            "source": "PROCUREMENT",
            "data": {"rfp": True},
        },
    ]

    result1 = _run_pipeline(company_id, raw_signals)
    result2 = _run_pipeline(company_id, raw_signals)

    # Critical deterministic replay check.
    assert result1 == result2

    multi_icp = result1["multi_icp"]
    prospects = result1["prospects"]
    outreach = result1["outreach"]
    timed = result1["timed"]
    pipeline = result1["pipeline"]

    # Stage validation: scorer output exists and has expected structure.
    assert multi_icp["company_id"] == company_id
    assert isinstance(multi_icp["opportunities"], list)
    assert len(multi_icp["opportunities"]) > 0
    for opportunity in multi_icp["opportunities"]:
        assert set(opportunity.keys()) == {"icp", "opportunity_score", "reason"}

    # Stage validation: prospects exist.
    assert prospects["company_id"] == company_id
    assert isinstance(prospects["prospects"], list)
    assert len(prospects["prospects"]) > 0

    # Stage validation: outreach exists.
    assert outreach["company_id"] == company_id
    assert isinstance(outreach["outreach"], list)
    assert len(outreach["outreach"]) == len(prospects["prospects"])

    # Stage validation: timed prospects exist.
    assert timed["company_id"] == company_id
    assert isinstance(timed["timed_prospects"], list)
    assert len(timed["timed_prospects"]) == len(prospects["prospects"])

    # Stage validation: pipeline records exist.
    assert pipeline["company_id"] == company_id
    assert isinstance(pipeline["pipeline_records"], list)
    assert len(pipeline["pipeline_records"]) == len(timed["timed_prospects"])

    # Continuity check: company_id is identical across all stages.
    assert multi_icp["company_id"] == company_id
    assert prospects["company_id"] == company_id
    assert outreach["company_id"] == company_id
    assert timed["company_id"] == company_id
    assert pipeline["company_id"] == company_id

    # Final output contract validation.
    assert set(pipeline.keys()) == {"company_id", "pipeline_records"}
    assert pipeline["company_id"] == company_id
    assert isinstance(pipeline["pipeline_records"], list)