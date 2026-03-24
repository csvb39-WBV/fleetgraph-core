from __future__ import annotations

from copy import deepcopy
from pathlib import Path
import tomllib


def _pyproject_path() -> Path:
    return Path(__file__).resolve().parents[1] / "pyproject.toml"


def _parse_pyproject() -> dict[str, object]:
    path = _pyproject_path()
    with path.open("rb") as file_obj:
        return tomllib.load(file_obj)


def test_pyproject_toml_exists() -> None:
    assert _pyproject_path().exists()


def test_required_project_metadata_fields_exist() -> None:
    parsed = _parse_pyproject()

    assert "project" in parsed
    assert "name" in parsed["project"]
    assert "version" in parsed["project"]
    assert "requires-python" in parsed["project"]


def test_version_exists_and_non_empty() -> None:
    parsed = _parse_pyproject()
    version = parsed["project"]["version"]

    assert isinstance(version, str)
    assert version != ""


def test_project_name_exists_and_non_empty() -> None:
    parsed = _parse_pyproject()
    project_name = parsed["project"]["name"]

    assert isinstance(project_name, str)
    assert project_name != ""


def test_requires_python_exists() -> None:
    parsed = _parse_pyproject()

    assert "requires-python" in parsed["project"]


def test_deterministic_repeated_parse_behavior() -> None:
    first = _parse_pyproject()
    second = _parse_pyproject()

    assert first == second


def test_no_mutation_of_parsed_data_in_tests() -> None:
    parsed = _parse_pyproject()
    snapshot = deepcopy(parsed)

    _ = parsed["project"]["name"]
    _ = parsed["project"]["version"]
    _ = parsed["project"]["requires-python"]

    assert parsed == snapshot
