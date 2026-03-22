from __future__ import annotations

from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
GITIGNORE_PATH = REPO_ROOT / ".gitignore"

REQUIRED_GITIGNORE_LINES = [
    "__pycache__/",
    "*.py[cod]",
    ".pytest_cache/",
    ".mypy_cache/",
    ".ruff_cache/",
    ".coverage",
    ".coverage.*",
    "htmlcov/",
    ".venv/",
    "venv/",
    "env/",
    "ENV/",
    ".env",
    ".env.*",
    ".vscode/",
    ".idea/",
    ".DS_Store",
    "Thumbs.db",
]

REQUIRED_IGNORED_PATH_EXAMPLES = [
    "__pycache__/module.cpython-314.pyc",
    "src/fleetgraph_core/__pycache__/example.cpython-314.pyc",
    ".pytest_cache/README.md",
    ".mypy_cache/cache.json",
    ".ruff_cache/index",
    ".coverage",
    "htmlcov/index.html",
    ".venv/bin/python",
    "venv/bin/activate",
    "env/bin/python",
    "ENV/bin/python",
    ".env",
    ".env.local",
    ".vscode/settings.json",
    ".idea/workspace.xml",
    ".DS_Store",
    "Thumbs.db",
]

REQUIRED_NOT_IGNORED_PATH_EXAMPLES = [
    "src/fleetgraph_core/api/relationship_signal_service.py",
    "tests/infrastructure/test_repository_hygiene_foundation.py",
    "frontend/fleetgraph-ui/src/App.jsx",
    "docs/architecture/overview.md",
]


def _read_gitignore_lines() -> list[str]:
    return GITIGNORE_PATH.read_text(encoding="utf-8").splitlines()


def _matches_gitignore_pattern(relative_path: str, pattern: str) -> bool:
    path = relative_path.strip("/")
    rule = pattern.strip()

    if not rule or rule.startswith("#"):
        return False

    if rule.endswith("/"):
        directory_name = rule.rstrip("/")
        path_parts = Path(path).parts
        return directory_name in path_parts

    return Path(path).match(rule)


def _is_ignored_by_gitignore(relative_path: str) -> bool:
    for pattern in _read_gitignore_lines():
        if _matches_gitignore_pattern(relative_path, pattern):
            return True
    return False


def test_gitignore_exists() -> None:
    assert GITIGNORE_PATH.is_file(), "Root .gitignore file is missing"


def test_gitignore_contains_required_rules() -> None:
    gitignore_lines = _read_gitignore_lines()
    missing_rules = [
        rule for rule in REQUIRED_GITIGNORE_LINES if rule not in gitignore_lines
    ]
    assert not missing_rules, (
        "Missing required .gitignore rules: " + ", ".join(missing_rules)
    )


def test_required_noise_paths_are_ignored() -> None:
    not_ignored = [
        path
        for path in REQUIRED_IGNORED_PATH_EXAMPLES
        if not _is_ignored_by_gitignore(path)
    ]
    assert not not_ignored, (
        "Expected paths not ignored by .gitignore: " + ", ".join(not_ignored)
    )


def test_required_repo_paths_are_not_ignored() -> None:
    incorrectly_ignored = [
        path
        for path in REQUIRED_NOT_IGNORED_PATH_EXAMPLES
        if _is_ignored_by_gitignore(path)
    ]
    assert not incorrectly_ignored, (
        "Required repo paths incorrectly ignored by .gitignore: "
        + ", ".join(incorrectly_ignored)
    )


def test_gitignore_validation_is_stable() -> None:
    first_lines = _read_gitignore_lines()
    second_lines = _read_gitignore_lines()
    assert first_lines == second_lines
