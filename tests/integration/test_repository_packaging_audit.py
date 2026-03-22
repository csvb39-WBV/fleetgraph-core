from __future__ import annotations

import importlib
import subprocess
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]

EXPECTED_ENV_TEMPLATE_FILES = {
    ".env.development.example",
    ".env.staging.example",
    ".env.production.example",
}
EXPECTED_RUNTIME_FILES = {
    "src/fleetgraph_core/config/runtime_config.py",
    "src/fleetgraph_core/runtime/runtime_bootstrap.py",
    "src/fleetgraph_core/runtime/runtime_external_api.py",
    "src/fleetgraph_core/runtime/runtime_health_api.py",
    "src/fleetgraph_core/runtime/runtime_http_api.py",
}
EXPECTED_TEST_FILES = {
    "tests/runtime/test_runtime_bootstrap.py",
    "tests/runtime/test_runtime_external_api.py",
    "tests/runtime/test_runtime_health_api.py",
    "tests/runtime/test_runtime_http_api.py",
    "tests/integration/test_runtime_boundary_audit.py",
    "tests/integration/test_runtime_http_contract_audit.py",
}
EXPECTED_IMPORT_MODULES = {
    "fleetgraph_core.config.runtime_config",
    "fleetgraph_core.runtime.runtime_bootstrap",
    "fleetgraph_core.runtime.runtime_external_api",
    "fleetgraph_core.runtime.runtime_health_api",
    "fleetgraph_core.runtime.runtime_http_api",
}


def _run_git(*args: str) -> str:
    completed = subprocess.run(
        ["git", *args],
        cwd=REPO_ROOT,
        text=True,
        capture_output=True,
        check=True,
    )
    return completed.stdout.strip()


def _collect_packaging_snapshot() -> dict:
    tracked_env_files = set(filter(None, _run_git("ls-files", "--", ".env*.example").splitlines()))
    runtime_exists = {path: (REPO_ROOT / path).is_file() for path in EXPECTED_RUNTIME_FILES}
    test_exists = {path: (REPO_ROOT / path).is_file() for path in EXPECTED_TEST_FILES}
    import_success = {}
    for module_name in EXPECTED_IMPORT_MODULES:
        try:
            importlib.import_module(module_name)
            import_success[module_name] = True
        except Exception:
            import_success[module_name] = False

    return {
        "tracked_env_files": tracked_env_files,
        "runtime_exists": runtime_exists,
        "test_exists": test_exists,
        "import_success": import_success,
    }


def test_required_env_template_files_exist_and_are_tracked() -> None:
    for relative_path in EXPECTED_ENV_TEMPLATE_FILES:
        assert (REPO_ROOT / relative_path).is_file()

    tracked_env_files = set(filter(None, _run_git("ls-files", "--", ".env*.example").splitlines()))
    assert tracked_env_files == EXPECTED_ENV_TEMPLATE_FILES


def test_required_runtime_boundary_files_exist() -> None:
    for relative_path in EXPECTED_RUNTIME_FILES:
        assert (REPO_ROOT / relative_path).is_file()


def test_required_runtime_and_integration_test_files_exist() -> None:
    for relative_path in EXPECTED_TEST_FILES:
        assert (REPO_ROOT / relative_path).is_file()


def test_critical_runtime_modules_import_successfully() -> None:
    for module_name in EXPECTED_IMPORT_MODULES:
        imported_module = importlib.import_module(module_name)
        assert imported_module is not None


def test_runtime_http_module_importable_and_callable_boundary() -> None:
    module = importlib.import_module("fleetgraph_core.runtime.runtime_http_api")
    assert hasattr(module, "app")
    assert module.app is not None


def test_packaging_snapshot_is_deterministic() -> None:
    first = _collect_packaging_snapshot()
    second = _collect_packaging_snapshot()
    assert first == second


def test_repository_state_not_mutated_by_packaging_audit() -> None:
    status_before = _run_git("status", "--short", "--untracked-files=all")
    _ = _collect_packaging_snapshot()
    status_after = _run_git("status", "--short", "--untracked-files=all")
    assert status_before == status_after


def test_exact_expected_file_sets_enforced() -> None:
    snapshot = _collect_packaging_snapshot()

    assert set(snapshot["runtime_exists"].keys()) == EXPECTED_RUNTIME_FILES
    assert set(snapshot["test_exists"].keys()) == EXPECTED_TEST_FILES
    assert snapshot["tracked_env_files"] == EXPECTED_ENV_TEMPLATE_FILES
    assert all(snapshot["runtime_exists"].values())
    assert all(snapshot["test_exists"].values())
    assert all(snapshot["import_success"].values())
