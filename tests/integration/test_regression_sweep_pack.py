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


def _sweep_input() -> dict[str, object]:
    return {
        "company_id": "cmp-reg-001",
        "raw_signals": [
            {"source": "permits", "data": {"permit_count": 3, "region": "west"}},
            {"source": "procurement", "data": "rfp activity up"},
            {"source": "expansion", "data": {"new_sites": 2}},
        ],
        "scoring_signals": [
            {"signal_category": "EXPANSION", "signal_value": "growth", "valid": True},
            {"signal_category": "PERMITS", "signal_value": 3, "valid": True},
            {"signal_category": "PROCUREMENT", "signal_value": True, "valid": True},
        ],
        "outcomes": [
            {"icp": "DLR", "outcome_status": "WON", "notes": "accepted pilot"}
        ],
    }


def _run_regression_sweep(payload: dict[str, object]) -> dict[str, object]:
    company_id = str(payload["company_id"])

    aggregated = aggregate_signals(payload["raw_signals"])

    scored = score_multi_icp(
        {
            "company_id": company_id,
            "signals": payload["scoring_signals"],
        }
    )

    prospects = build_prospects(
        {
            "company_id": company_id,
            "opportunities": scored["opportunities"],
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


def test_regression_sweep_core_pipeline_and_layers() -> None:
    payload = _sweep_input()
    result = _run_regression_sweep(payload)
    company_id = payload["company_id"]

    assert result["scored"]["opportunities"]
    assert result["prospects"]["prospects"]
    assert result["outreach"]["outreach"]
    assert result["timed"]["timed_prospects"]
    assert result["pipeline"]["pipeline_records"]

    assert set(result["priority_dashboard"].keys()) == {
        "company_id",
        "dashboard_summary",
        "records",
    }
    assert set(result["company_intelligence"].keys()) == {
        "company_id",
        "company_intelligence",
        "opportunities",
        "prospects",
        "pipeline_records",
    }
    assert set(result["predictive_insights"].keys()) == {
        "company_id",
        "predictive_insights",
        "opportunities",
        "timed_prospects",
    }
    assert set(result["rfp_panel"].keys()) == {
        "company_id",
        "rfp_panel",
        "opportunities",
        "pipeline_records",
    }

    assert set(result["tracked_outcomes"].keys()) == {"company_id", "tracked_outcomes"}
    assert set(result["model_adjustments"].keys()) == {"company_id", "model_adjustments"}
    assert set(result["signal_effectiveness"].keys()) == {
        "company_id",
        "signal_effectiveness",
    }

    assert result["aggregated"]["aggregated_signals"]
    assert result["tracked_outcomes"]["tracked_outcomes"]

    assert result["scored"]["company_id"] == company_id
    assert result["prospects"]["company_id"] == company_id
    assert result["outreach"]["company_id"] == company_id
    assert result["timed"]["company_id"] == company_id
    assert result["pipeline"]["company_id"] == company_id
    assert result["priority_dashboard"]["company_id"] == company_id
    assert result["company_intelligence"]["company_id"] == company_id
    assert result["predictive_insights"]["company_id"] == company_id
    assert result["rfp_panel"]["company_id"] == company_id
    assert result["tracked_outcomes"]["company_id"] == company_id
    assert result["model_adjustments"]["company_id"] == company_id
    assert result["signal_effectiveness"]["company_id"] == company_id

    for entry in result["prospects"]["prospects"]:
        assert set(entry.keys()) == {
            "company_id",
            "icp",
            "priority",
            "opportunity_score",
            "reason",
        }
        assert entry["company_id"] == company_id

    for entry in result["pipeline"]["pipeline_records"]:
        assert entry["stage"] == "READY"
        assert entry["pipeline_status"] == "OPEN"
        assert entry["company_id"] == company_id


def test_regression_sweep_replay_is_deterministic() -> None:
    payload = _sweep_input()

    first = _run_regression_sweep(payload)
    second = _run_regression_sweep(payload)

    assert first["aggregated"] == second["aggregated"]
    assert first["scored"] == second["scored"]
    assert first["pipeline"] == second["pipeline"]
    assert first["priority_dashboard"] == second["priority_dashboard"]
    assert first["tracked_outcomes"] == second["tracked_outcomes"]
    assert first["signal_effectiveness"] == second["signal_effectiveness"]


def test_regression_sweep_failure_path_strict_validation_holds() -> None:
    with pytest.raises(
        ValueError,
        match=r"timed_prospects\[0\] missing required field: timing",
    ):
        build_pipeline_records(
            {
                "company_id": "cmp-reg-001",
                "timed_prospects": [
                    {
                        "company_id": "cmp-reg-001",
                        "icp": "DLR",
                        "priority": "HIGH",
                        "opportunity_score": 90.0,
                        "reason": "Expansion",
                    }
                ],
            }
        )