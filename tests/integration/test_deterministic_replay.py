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


def _run_full_pipeline(company_id: str, raw_signals: object) -> dict[str, object]:
    aggregated = aggregate_signals(raw_signals)

    scorer_signals = [
        {
            "signal_category": str(entry["source"]),
            "signal_value": str(entry["normalized_data"]),
            "valid": bool(entry["valid"]),
        }
        for entry in aggregated["aggregated_signals"]
    ]

    multi_icp = score_multi_icp({"company_id": company_id, "signals": scorer_signals})
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


def test_full_pipeline_is_stable_across_three_replays() -> None:
    company_id = "cmp-replay-001"
    raw_signals = [
        {"source": "EXPANSION", "data": {"evidence": "new site"}},
        {"source": "PERMITS", "data": {"count": 4}},
        {"source": "PROCUREMENT", "data": {"rfp": True}},
    ]

    run1 = _run_full_pipeline(company_id, raw_signals)
    run2 = _run_full_pipeline(company_id, raw_signals)
    run3 = _run_full_pipeline(company_id, raw_signals)

    # Full output stability across all runs.
    assert run1 == run2
    assert run2 == run3

    # Final pipeline record stability.
    assert run1["pipeline"]["pipeline_records"] == run2["pipeline"]["pipeline_records"]
    assert run2["pipeline"]["pipeline_records"] == run3["pipeline"]["pipeline_records"]

    # Ordering stability across runs.
    icp_order_1 = [record["icp"] for record in run1["pipeline"]["pipeline_records"]]
    icp_order_2 = [record["icp"] for record in run2["pipeline"]["pipeline_records"]]
    icp_order_3 = [record["icp"] for record in run3["pipeline"]["pipeline_records"]]
    assert icp_order_1 == icp_order_2 == icp_order_3

    # Company continuity stability across runs and stages.
    for result in (run1, run2, run3):
        assert result["multi_icp"]["company_id"] == company_id
        assert result["prospects"]["company_id"] == company_id
        assert result["outreach"]["company_id"] == company_id
        assert result["timed"]["company_id"] == company_id
        assert result["pipeline"]["company_id"] == company_id

    # Outreach stability across runs.
    assert run1["outreach"]["outreach"] == run2["outreach"]["outreach"]
    assert run2["outreach"]["outreach"] == run3["outreach"]["outreach"]

    # Timing value stability across runs.
    timings1 = [entry["timing"] for entry in run1["timed"]["timed_prospects"]]
    timings2 = [entry["timing"] for entry in run2["timed"]["timed_prospects"]]
    timings3 = [entry["timing"] for entry in run3["timed"]["timed_prospects"]]
    assert timings1 == timings2 == timings3


def test_malformed_replay_input_hard_fails_consistently() -> None:
    bad_signals = {"not": "a list"}

    errors: list[tuple[type[BaseException], str]] = []
    for _ in range(3):
        with pytest.raises(Exception) as caught:
            _run_full_pipeline("cmp-replay-err", bad_signals)
        assert isinstance(caught.value, ValueError)
        errors.append((type(caught.value), str(caught.value)))

    assert errors[0] == errors[1] == errors[2]
    assert errors[0][1] == "signals must be a list"