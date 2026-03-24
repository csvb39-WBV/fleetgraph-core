from __future__ import annotations

import importlib
import subprocess
import zipfile
from pathlib import Path
from tempfile import TemporaryDirectory
from pathlib import PurePosixPath


REPO_ROOT = Path(__file__).resolve().parents[2]
AUTHORITATIVE_ARTIFACT_COMMAND = (
    "git",
    "archive",
    "--worktree-attributes",
    "--format=zip",
    "--output=<artifact>.zip",
    "HEAD",
)

EXPECTED_ENV_TEMPLATE_FILES = {
    ".env.development.example",
    ".env.staging.example",
    ".env.production.example",
}
EXPECTED_REPOSITORY_RELEASE_CONTROL_FILES = {
    "README.md",
    "requirements.txt",
    "pytest.ini",
    "pyproject.toml",
    ".gitattributes",
}
EXPECTED_ARTIFACT_RELEASE_CONTROL_FILES = {
    "README.md",
    "requirements.txt",
    "pytest.ini",
    "pyproject.toml",
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
DISALLOWED_ARTIFACT_DIRECTORY_NAMES = {
    "__pycache__",
    ".pytest_cache",
    ".mypy_cache",
    ".ruff_cache",
    "htmlcov",
}
DISALLOWED_ARTIFACT_FILE_SUFFIXES = (
    ".pyc",
    ".pyo",
    ".pyd",
)


def _run_git(*args: str) -> str:
    completed = subprocess.run(
        ["git", *args],
        cwd=REPO_ROOT,
        text=True,
        capture_output=True,
        check=True,
    )
    return completed.stdout.strip()


def _build_release_artifact(artifact_path: Path) -> Path:
    subprocess.run(
        [
            "git",
            "archive",
            "--worktree-attributes",
            "--format=zip",
            f"--output={artifact_path}",
            "HEAD",
        ],
        cwd=REPO_ROOT,
        text=True,
        capture_output=True,
        check=True,
    )
    return artifact_path


def _read_archive_paths(artifact_path: Path) -> set[str]:
    with zipfile.ZipFile(artifact_path) as archive:
        return set(archive.namelist())


def _find_disallowed_artifact_paths(archive_paths: set[str]) -> set[str]:
    disallowed_paths = set()

    for archive_path in archive_paths:
        path = PurePosixPath(archive_path)
        if any(part in DISALLOWED_ARTIFACT_DIRECTORY_NAMES for part in path.parts):
            disallowed_paths.add(archive_path)
            continue

        name = path.name
        if name.endswith(DISALLOWED_ARTIFACT_FILE_SUFFIXES):
            disallowed_paths.add(archive_path)
            continue
        if name == ".coverage" or name.startswith(".coverage."):
            disallowed_paths.add(archive_path)

    return disallowed_paths


def _collect_repository_packaging_snapshot() -> dict:
    env_template_exists = {
        path: (REPO_ROOT / path).is_file() for path in EXPECTED_ENV_TEMPLATE_FILES
    }
    release_control_exists = {
        path: (REPO_ROOT / path).is_file()
        for path in EXPECTED_REPOSITORY_RELEASE_CONTROL_FILES
    }
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
        "env_template_exists": env_template_exists,
        "release_control_exists": release_control_exists,
        "runtime_exists": runtime_exists,
        "test_exists": test_exists,
        "import_success": import_success,
    }


def _collect_release_artifact_snapshot(artifact_path: Path) -> dict:
    archive_paths = _read_archive_paths(artifact_path)

    return {
        "archive_paths": archive_paths,
        "env_template_files": {
            path for path in archive_paths if path in EXPECTED_ENV_TEMPLATE_FILES
        },
        "release_control_files": {
            path for path in archive_paths if path in EXPECTED_ARTIFACT_RELEASE_CONTROL_FILES
        },
        "runtime_files": {
            path for path in archive_paths if path in EXPECTED_RUNTIME_FILES
        },
        "test_files": {
            path for path in archive_paths if path in EXPECTED_TEST_FILES
        },
        "disallowed_paths": _find_disallowed_artifact_paths(archive_paths),
    }


def test_authoritative_artifact_command_is_zip_compatible() -> None:
    assert AUTHORITATIVE_ARTIFACT_COMMAND == (
        "git",
        "archive",
        "--worktree-attributes",
        "--format=zip",
        "--output=<artifact>.zip",
        "HEAD",
    )


def test_required_env_template_files_exist_in_repository_checkout() -> None:
    for relative_path in EXPECTED_ENV_TEMPLATE_FILES:
        assert (REPO_ROOT / relative_path).is_file()


def test_required_release_control_files_exist_in_repository_checkout() -> None:
    for relative_path in EXPECTED_REPOSITORY_RELEASE_CONTROL_FILES:
        assert (REPO_ROOT / relative_path).is_file()


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


def test_release_artifact_contains_expected_intentional_content() -> None:
    with TemporaryDirectory() as tmp_dir:
        artifact_path = _build_release_artifact(Path(tmp_dir) / "fleetgraph-core.zip")
        snapshot = _collect_release_artifact_snapshot(artifact_path)

    assert snapshot["env_template_files"] == EXPECTED_ENV_TEMPLATE_FILES
    assert snapshot["release_control_files"] == EXPECTED_ARTIFACT_RELEASE_CONTROL_FILES
    assert snapshot["runtime_files"] == EXPECTED_RUNTIME_FILES
    assert snapshot["test_files"] == EXPECTED_TEST_FILES


def test_release_artifact_excludes_git_metadata_and_junk() -> None:
    with TemporaryDirectory() as tmp_dir:
        artifact_path = _build_release_artifact(Path(tmp_dir) / "fleetgraph-core.zip")
        snapshot = _collect_release_artifact_snapshot(artifact_path)

    assert not any(
        path == ".git" or path.startswith(".git/")
        for path in snapshot["archive_paths"]
    )
    assert snapshot["disallowed_paths"] == set()


def test_packaging_snapshot_is_deterministic() -> None:
    first = _collect_repository_packaging_snapshot()
    second = _collect_repository_packaging_snapshot()
    assert first == second


def test_release_artifact_snapshot_is_deterministic() -> None:
    with TemporaryDirectory() as tmp_dir:
        first_artifact = _build_release_artifact(Path(tmp_dir) / "first.zip")
        second_artifact = _build_release_artifact(Path(tmp_dir) / "second.zip")
        first = _collect_release_artifact_snapshot(first_artifact)
        second = _collect_release_artifact_snapshot(second_artifact)

    assert first == second


def test_repository_state_not_mutated_by_packaging_audit() -> None:
    status_before = _run_git("status", "--short", "--untracked-files=all")
    _ = _collect_repository_packaging_snapshot()
    with TemporaryDirectory() as tmp_dir:
        _build_release_artifact(Path(tmp_dir) / "fleetgraph-core.zip")
    status_after = _run_git("status", "--short", "--untracked-files=all")
    assert status_before == status_after


def test_exact_expected_file_sets_enforced() -> None:
    repository_snapshot = _collect_repository_packaging_snapshot()
    with TemporaryDirectory() as tmp_dir:
        artifact_path = _build_release_artifact(Path(tmp_dir) / "fleetgraph-core.zip")
        artifact_snapshot = _collect_release_artifact_snapshot(artifact_path)

    assert set(repository_snapshot["env_template_exists"].keys()) == EXPECTED_ENV_TEMPLATE_FILES
    assert (
        set(repository_snapshot["release_control_exists"].keys())
        == EXPECTED_REPOSITORY_RELEASE_CONTROL_FILES
    )
    assert set(repository_snapshot["runtime_exists"].keys()) == EXPECTED_RUNTIME_FILES
    assert set(repository_snapshot["test_exists"].keys()) == EXPECTED_TEST_FILES
    assert all(repository_snapshot["env_template_exists"].values())
    assert all(repository_snapshot["release_control_exists"].values())
    assert all(repository_snapshot["runtime_exists"].values())
    assert all(repository_snapshot["test_exists"].values())
    assert all(repository_snapshot["import_success"].values())

    assert artifact_snapshot["env_template_files"] == EXPECTED_ENV_TEMPLATE_FILES
    assert artifact_snapshot["release_control_files"] == EXPECTED_ARTIFACT_RELEASE_CONTROL_FILES
    assert artifact_snapshot["runtime_files"] == EXPECTED_RUNTIME_FILES
    assert artifact_snapshot["test_files"] == EXPECTED_TEST_FILES
    assert artifact_snapshot["disallowed_paths"] == set()
