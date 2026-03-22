from __future__ import annotations

import importlib
import pathlib
import subprocess
import sys
from types import ModuleType


PROJECT_ROOT = pathlib.Path(__file__).resolve().parents[2]
SRC_ROOT = PROJECT_ROOT / "src"

if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))


def _import_module(name: str) -> ModuleType:
    return importlib.import_module(name)


def test_infrastructure_export_surface_is_consistent_and_accessible() -> None:
    module = _import_module("fleetgraph_core.infrastructure")

    assert hasattr(module, "__all__")
    exported_names = getattr(module, "__all__")
    assert isinstance(exported_names, list)
    assert len(exported_names) > 0

    # Structural defect guard: every name in __all__ must exist on the module.
    missing_names = [name for name in exported_names if not hasattr(module, name)]
    assert missing_names == []

    # Validate key downstream-facing symbols are accessible.
    expected_names = {
        "SnapshotStorageInterface",
        "QueuePublisherInterface",
        "QueueConsumerInterface",
        "SchedulerTriggerInterface",
        "ConfigProviderInterface",
        "ObservabilityEmitterInterface",
        "validate_snapshot_content_type",
        "validate_observability_level",
        "validate_non_empty_string",
        "validate_mapping",
    }
    for name in expected_names:
        assert name in exported_names
        assert hasattr(module, name)


def test_intelligence_and_api_empty_init_surfaces_import_safely_and_stably() -> None:
    intelligence = _import_module("fleetgraph_core.intelligence")
    api = _import_module("fleetgraph_core.api")

    # Empty __init__ surfaces should import safely and not require __all__.
    assert hasattr(intelligence, "__file__")
    assert hasattr(api, "__file__")
    assert not hasattr(intelligence, "__all__")
    assert not hasattr(api, "__all__")

    # Deterministic replay: repeated imports/reloads should preserve stable surface.
    intelligence_names_before = sorted(name for name in dir(intelligence) if not name.startswith("__"))
    api_names_before = sorted(name for name in dir(api) if not name.startswith("__"))

    intelligence_reloaded = importlib.reload(intelligence)
    api_reloaded = importlib.reload(api)

    intelligence_names_after = sorted(name for name in dir(intelligence_reloaded) if not name.startswith("__"))
    api_names_after = sorted(name for name in dir(api_reloaded) if not name.startswith("__"))

    assert intelligence_names_before == intelligence_names_after
    assert api_names_before == api_names_after


def test_feedback_namespace_surface_imports_and_exposes_expected_submodules() -> None:
    # feedback currently exists as a namespace package without __init__.py.
    feedback = _import_module("fleetgraph_core.feedback")
    assert hasattr(feedback, "__path__")

    # __file__ is commonly absent for namespace packages.
    assert not hasattr(feedback, "__all__")

    # Known downstream modules under feedback should import cleanly.
    outcome_tracker = _import_module("fleetgraph_core.feedback.outcome_tracker")
    adjustment = _import_module("fleetgraph_core.feedback.model_adjustment_engine")
    analyzer = _import_module("fleetgraph_core.feedback.signal_effectiveness_analyzer")

    assert hasattr(outcome_tracker, "track_outcomes")
    assert hasattr(adjustment, "build_model_adjustments")
    assert hasattr(analyzer, "analyze_signal_effectiveness")


def test_repeated_import_cycles_are_stable_for_target_package_surfaces() -> None:
    package_names = (
        "fleetgraph_core.infrastructure",
        "fleetgraph_core.intelligence",
        "fleetgraph_core.api",
        "fleetgraph_core.feedback",
    )

    baseline: dict[str, tuple[str, ...]] = {}
    for name in package_names:
        module = _import_module(name)
        public_names = tuple(sorted(item for item in dir(module) if not item.startswith("__")))
        baseline[name] = public_names

    for _ in range(3):
        for name in package_names:
            module = _import_module(name)
            stable_names = tuple(sorted(item for item in dir(module) if not item.startswith("__")))
            assert stable_names == baseline[name]


def test_surface_imports_do_not_require_external_dependencies() -> None:
    command = [
        sys.executable,
        "-c",
        (
            "import importlib, sys; "
            f"sys.path.insert(0, {str(SRC_ROOT)!r}); "
            "importlib.import_module('fleetgraph_core.infrastructure'); "
            "importlib.import_module('fleetgraph_core.intelligence'); "
            "importlib.import_module('fleetgraph_core.api'); "
            "importlib.import_module('fleetgraph_core.feedback'); "
            "print('ok')"
        ),
    ]
    result = subprocess.run(command, capture_output=True, text=True, check=True)

    assert result.stdout.strip() == "ok"