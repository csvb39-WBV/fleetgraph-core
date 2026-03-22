from __future__ import annotations

import importlib
import pathlib
import subprocess
import sys


PROJECT_ROOT = pathlib.Path(__file__).resolve().parents[2]
SRC_ROOT = PROJECT_ROOT / "src"

if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))


PACKAGE_NAMES = (
    "fleetgraph_core.infrastructure",
    "fleetgraph_core.intelligence",
    "fleetgraph_core.api",
    "fleetgraph_core.feedback",
)

REPRESENTATIVE_MODULES = (
    "fleetgraph_core.infrastructure.interfaces",
    "fleetgraph_core.intelligence.signal_aggregator",
    "fleetgraph_core.intelligence.multi_icp_scorer",
    "fleetgraph_core.api.priority_dashboard_api",
    "fleetgraph_core.api.company_intelligence_api",
    "fleetgraph_core.feedback.outcome_tracker",
    "fleetgraph_core.feedback.model_adjustment_engine",
)


def _import_modules_in_order(module_names: tuple[str, ...]) -> dict[str, object]:
    imported: dict[str, object] = {}
    for module_name in module_names:
        imported[module_name] = importlib.import_module(module_name)
    return imported


def test_cross_package_imports_succeed_and_symbols_are_accessible() -> None:
    imported = _import_modules_in_order(PACKAGE_NAMES + REPRESENTATIVE_MODULES)

    assert hasattr(imported["fleetgraph_core.infrastructure.interfaces"], "SnapshotStorageInterface")
    assert hasattr(imported["fleetgraph_core.intelligence.signal_aggregator"], "aggregate_signals")
    assert hasattr(imported["fleetgraph_core.intelligence.multi_icp_scorer"], "score_multi_icp")
    assert hasattr(imported["fleetgraph_core.api.priority_dashboard_api"], "build_priority_dashboard")
    assert hasattr(imported["fleetgraph_core.api.company_intelligence_api"], "build_company_intelligence")
    assert hasattr(imported["fleetgraph_core.feedback.outcome_tracker"], "track_outcomes")
    assert hasattr(imported["fleetgraph_core.feedback.model_adjustment_engine"], "build_model_adjustments")


def test_import_order_sequences_do_not_break_module_usability() -> None:
    sequence_a = (
        "fleetgraph_core.infrastructure.interfaces",
        "fleetgraph_core.intelligence.signal_aggregator",
        "fleetgraph_core.api.priority_dashboard_api",
        "fleetgraph_core.feedback.outcome_tracker",
    )
    sequence_b = (
        "fleetgraph_core.feedback.model_adjustment_engine",
        "fleetgraph_core.api.company_intelligence_api",
        "fleetgraph_core.intelligence.multi_icp_scorer",
        "fleetgraph_core.infrastructure.interfaces",
    )
    sequence_c = (
        "fleetgraph_core.intelligence.prospect_engine",
        "fleetgraph_core.feedback.signal_effectiveness_analyzer",
        "fleetgraph_core.api.predictive_insights_api",
        "fleetgraph_core.intelligence.timing_engine",
        "fleetgraph_core.api.rfp_panel_api",
    )

    imports_a = _import_modules_in_order(sequence_a)
    imports_b = _import_modules_in_order(sequence_b)
    imports_c = _import_modules_in_order(sequence_c)

    assert hasattr(imports_a["fleetgraph_core.intelligence.signal_aggregator"], "aggregate_signals")
    assert hasattr(imports_a["fleetgraph_core.api.priority_dashboard_api"], "build_priority_dashboard")
    assert hasattr(imports_a["fleetgraph_core.feedback.outcome_tracker"], "track_outcomes")

    assert hasattr(imports_b["fleetgraph_core.feedback.model_adjustment_engine"], "build_model_adjustments")
    assert hasattr(imports_b["fleetgraph_core.api.company_intelligence_api"], "build_company_intelligence")
    assert hasattr(imports_b["fleetgraph_core.intelligence.multi_icp_scorer"], "score_multi_icp")

    assert hasattr(imports_c["fleetgraph_core.intelligence.prospect_engine"], "build_prospects")
    assert hasattr(imports_c["fleetgraph_core.feedback.signal_effectiveness_analyzer"], "analyze_signal_effectiveness")
    assert hasattr(imports_c["fleetgraph_core.api.predictive_insights_api"], "build_predictive_insights")
    assert hasattr(imports_c["fleetgraph_core.intelligence.timing_engine"], "assign_timing")
    assert hasattr(imports_c["fleetgraph_core.api.rfp_panel_api"], "build_rfp_panel")


def test_reload_stability_keeps_expected_symbols_available() -> None:
    module_names = (
        "fleetgraph_core.infrastructure.interfaces",
        "fleetgraph_core.intelligence.multi_icp_scorer",
        "fleetgraph_core.api.company_intelligence_api",
        "fleetgraph_core.feedback.outcome_tracker",
    )

    for module_name in module_names:
        module = importlib.import_module(module_name)
        before = sorted(name for name in dir(module) if not name.startswith("__"))
        reloaded = importlib.reload(module)
        after = sorted(name for name in dir(reloaded) if not name.startswith("__"))
        assert before == after

    interfaces = importlib.import_module("fleetgraph_core.infrastructure.interfaces")
    scorer = importlib.import_module("fleetgraph_core.intelligence.multi_icp_scorer")
    company_api = importlib.import_module("fleetgraph_core.api.company_intelligence_api")
    outcomes = importlib.import_module("fleetgraph_core.feedback.outcome_tracker")

    assert callable(getattr(interfaces, "validate_mapping"))
    assert callable(getattr(scorer, "score_multi_icp"))
    assert callable(getattr(company_api, "build_company_intelligence"))
    assert callable(getattr(outcomes, "track_outcomes"))


def test_fresh_process_can_import_cross_package_set() -> None:
    command = [
        sys.executable,
        "-c",
        (
            "import importlib, sys; "
            f"sys.path.insert(0, {str(SRC_ROOT)!r}); "
            "mods = ["
            "'fleetgraph_core.infrastructure.interfaces',"
            "'fleetgraph_core.intelligence.signal_aggregator',"
            "'fleetgraph_core.intelligence.multi_icp_scorer',"
            "'fleetgraph_core.api.priority_dashboard_api',"
            "'fleetgraph_core.api.company_intelligence_api',"
            "'fleetgraph_core.feedback.outcome_tracker',"
            "'fleetgraph_core.feedback.model_adjustment_engine'"
            "]; "
            "[importlib.import_module(m) for m in mods]; "
            "print('ok')"
        ),
    ]
    result = subprocess.run(command, capture_output=True, text=True, check=True)
    assert result.stdout.strip() == "ok"