from __future__ import annotations

import pathlib
import sys

import pytest


PROJECT_ROOT = pathlib.Path(__file__).resolve().parents[2]
SRC_ROOT = PROJECT_ROOT / "src"

if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))


from fleetgraph_core.api.company_intelligence_api import build_company_intelligence
from fleetgraph_core.api.predictive_insights_api import build_predictive_insights
from fleetgraph_core.api.priority_dashboard_api import build_priority_dashboard
from fleetgraph_core.api.rfp_panel_api import build_rfp_panel
from fleetgraph_core.feedback.model_adjustment_engine import build_model_adjustments
from fleetgraph_core.feedback.outcome_tracker import track_outcomes
from fleetgraph_core.feedback.signal_effectiveness_analyzer import (
    analyze_signal_effectiveness,
)
from fleetgraph_core.intelligence.multi_icp_scorer import score_multi_icp
from fleetgraph_core.intelligence.outreach_generator import generate_outreach
from fleetgraph_core.intelligence.pipeline_tracker import build_pipeline_records
from fleetgraph_core.intelligence.prospect_engine import build_prospects
from fleetgraph_core.intelligence.signal_aggregator import aggregate_signals
from fleetgraph_core.intelligence.timing_engine import assign_timing


def _stable_input() -> dict[str, object]:
    return {
        "company_id": "cmp-replay-001",
        "raw_signals": [
            {"source": "permits", "data": {"permit_count": 4, "region": "west"}},
            {"source": "operations", "data": {"uptime": "high", "fleet_age": 2}},
            {"source": "procurement", "data": "rfp released"},
            {"source": "expansion", "data": {"new_sites": 1}},
        ],
        "scoring_signals": [
            {"signal_category": "EXPANSION", "signal_value": "growth", "valid": True},
            {"signal_category": "PERMITS", "signal_value": 4, "valid": True},
            {"signal_category": "PROCUREMENT", "signal_value": True, "valid": True},
            {"signal_category": "OPERATIONS", "signal_value": "uptick", "valid": True},
        ],
        "outcomes": [
            {"icp": "DLR", "outcome_status": "WON", "notes": "converted quickly"},
            {"icp": "OEM", "outcome_status": "NO_RESPONSE", "notes": "follow-up queued"},
        ],
    }


def _run_full_stack(payload: dict[str, object]) -> dict[str, object]:
    company_id = str(payload["company_id"])

    aggregated = aggregate_signals(payload["raw_signals"])
    scored = score_multi_icp({"company_id": company_id, "signals": payload["scoring_signals"]})
    prospects = build_prospects({"company_id": company_id, "opportunities": scored["opportunities"]})
    outreach = generate_outreach({"company_id": company_id, "prospects": prospects["prospects"]})
    timed = assign_timing({"company_id": company_id, "prospects": prospects["prospects"]})
    pipeline = build_pipeline_records(
        {
            "company_id": company_id,
            "timed_prospects": timed["timed_prospects"],
        }
    )
    priority_dashboard = build_priority_dashboard(
        {
            "company_id": company_id,
            "pipeline_records": pipeline["pipeline_records"],
        }
    )
    company_intelligence = build_company_intelligence(
        {
            "company_id": company_id,
            "opportunities": scored["opportunities"],
            "prospects": prospects["prospects"],
            "pipeline_records": pipeline["pipeline_records"],
        }
    )
    predictive_insights = build_predictive_insights(
        {
            "company_id": company_id,
            "opportunities": scored["opportunities"],
            "timed_prospects": timed["timed_prospects"],
        }
    )
    rfp_panel = build_rfp_panel(
        {
            "company_id": company_id,
            "opportunities": scored["opportunities"],
            "pipeline_records": pipeline["pipeline_records"],
        }
    )
    tracked_outcomes = track_outcomes(
        {
            "company_id": company_id,
            "pipeline_records": pipeline["pipeline_records"],
            "outcomes": payload["outcomes"],
        }
    )
    model_adjustments = build_model_adjustments(
        {
            "company_id": company_id,
            "tracked_outcomes": tracked_outcomes["tracked_outcomes"],
        }
    )
    signal_effectiveness = analyze_signal_effectiveness(
        {
            "company_id": company_id,
            "tracked_outcomes": tracked_outcomes["tracked_outcomes"],
        }
    )

    return {
        "aggregated": aggregated,
        "scored": scored,
        "prospects": prospects,
        "outreach": outreach,
        "timed": timed,
        "pipeline": pipeline,
        "priority_dashboard": priority_dashboard,
        "company_intelligence": company_intelligence,
        "predictive_insights": predictive_insights,
        "rfp_panel": rfp_panel,
        "tracked_outcomes": tracked_outcomes,
        "model_adjustments": model_adjustments,
        "signal_effectiveness": signal_effectiveness,
    }


def test_deterministic_full_stack_replay_three_identical_runs() -> None:
    payload = _stable_input()
    run_one = _run_full_stack(payload)
    run_two = _run_full_stack(payload)
    run_three = _run_full_stack(payload)

    assert run_one["aggregated"] == run_two["aggregated"] == run_three["aggregated"]
    assert run_one["scored"] == run_two["scored"] == run_three["scored"]
    assert run_one["prospects"] == run_two["prospects"] == run_three["prospects"]
    assert run_one["outreach"] == run_two["outreach"] == run_three["outreach"]
    assert run_one["timed"] == run_two["timed"] == run_three["timed"]
    assert run_one["pipeline"] == run_two["pipeline"] == run_three["pipeline"]
    assert (
        run_one["priority_dashboard"]
        == run_two["priority_dashboard"]
        == run_three["priority_dashboard"]
    )
    assert (
        run_one["company_intelligence"]
        == run_two["company_intelligence"]
        == run_three["company_intelligence"]
    )
    assert (
        run_one["predictive_insights"]
        == run_two["predictive_insights"]
        == run_three["predictive_insights"]
    )
    assert run_one["rfp_panel"] == run_two["rfp_panel"] == run_three["rfp_panel"]
    assert (
        run_one["tracked_outcomes"]
        == run_two["tracked_outcomes"]
        == run_three["tracked_outcomes"]
    )
    assert (
        run_one["model_adjustments"]
        == run_two["model_adjustments"]
        == run_three["model_adjustments"]
    )
    assert (
        run_one["signal_effectiveness"]
        == run_two["signal_effectiveness"]
        == run_three["signal_effectiveness"]
    )

    company_id = payload["company_id"]
    stage_company_ids = {
        run_one["scored"]["company_id"],
        run_one["prospects"]["company_id"],
        run_one["outreach"]["company_id"],
        run_one["timed"]["company_id"],
        run_one["pipeline"]["company_id"],
        run_one["priority_dashboard"]["company_id"],
        run_one["company_intelligence"]["company_id"],
        run_one["predictive_insights"]["company_id"],
        run_one["rfp_panel"]["company_id"],
        run_one["tracked_outcomes"]["company_id"],
        run_one["model_adjustments"]["company_id"],
        run_one["signal_effectiveness"]["company_id"],
    }
    assert stage_company_ids == {company_id}

    assert [item["icp"] for item in run_one["scored"]["opportunities"]] == [
        item["icp"] for item in run_two["scored"]["opportunities"]
    ] == [item["icp"] for item in run_three["scored"]["opportunities"]]

    assert [item["icp"] for item in run_one["prospects"]["prospects"]] == [
        item["icp"] for item in run_two["prospects"]["prospects"]
    ] == [item["icp"] for item in run_three["prospects"]["prospects"]]

    assert [item["icp"] for item in run_one["pipeline"]["pipeline_records"]] == [
        item["icp"] for item in run_two["pipeline"]["pipeline_records"]
    ] == [item["icp"] for item in run_three["pipeline"]["pipeline_records"]]

    assert run_one["priority_dashboard"]["dashboard_summary"] == run_two["priority_dashboard"][
        "dashboard_summary"
    ] == run_three["priority_dashboard"]["dashboard_summary"]
    assert run_one["company_intelligence"]["company_intelligence"] == run_two[
        "company_intelligence"
    ]["company_intelligence"] == run_three["company_intelligence"]["company_intelligence"]
    assert run_one["predictive_insights"]["predictive_insights"] == run_two[
        "predictive_insights"
    ]["predictive_insights"] == run_three["predictive_insights"]["predictive_insights"]
    assert run_one["rfp_panel"]["rfp_panel"] == run_two["rfp_panel"]["rfp_panel"] == run_three[
        "rfp_panel"
    ]["rfp_panel"]

    assert run_one["model_adjustments"]["model_adjustments"] == run_two["model_adjustments"][
        "model_adjustments"
    ] == run_three["model_adjustments"]["model_adjustments"]
    assert run_one["signal_effectiveness"]["signal_effectiveness"] == run_two[
        "signal_effectiveness"
    ]["signal_effectiveness"] == run_three["signal_effectiveness"]["signal_effectiveness"]


def test_deterministic_full_stack_replay_has_stable_downstream_failure_behavior() -> None:
    error_messages: list[str] = []

    for _ in range(3):
        with pytest.raises(ValueError) as caught:
            build_priority_dashboard(
                {
                    "company_id": "cmp-replay-001",
                    "pipeline_records": {},
                }
            )
        error_messages.append(str(caught.value))

    assert error_messages == [
        "pipeline_records must be a list",
        "pipeline_records must be a list",
        "pipeline_records must be a list",
    ]