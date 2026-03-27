from __future__ import annotations

import copy
import pathlib
import sys

import pytest


PROJECT_ROOT = pathlib.Path(__file__).resolve().parents[2]
SRC_ROOT = PROJECT_ROOT / "src"

if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))


from fleetgraph.runtime.runtime_config import build_default_runtime_config, build_runtime_config


def _config(tmp_path: pathlib.Path, **overrides: object) -> dict[str, object]:
    config: dict[str, object] = {
        "run_date": "2026-03-27",
        "output_directory": str(tmp_path / "outputs"),
        "cache_path": str(tmp_path / "cache" / "signal_cache.json"),
        "max_queries_per_run": 7,
        "max_results_per_query": 5,
        "connector_timeout_seconds": 5.0,
        "connector_max_retries": 2,
    }
    config.update(overrides)
    return config


def test_runtime_config_validation_and_normalization(tmp_path: pathlib.Path) -> None:
    result = build_runtime_config(_config(tmp_path))

    assert result == {
        "run_date": "2026-03-27",
        "output_directory": str((tmp_path / "outputs").resolve()),
        "cache_path": str((tmp_path / "cache" / "signal_cache.json").resolve()),
        "max_queries_per_run": 7,
        "max_results_per_query": 5,
        "connector_timeout_seconds": 5.0,
        "connector_max_retries": 2,
    }


def test_runtime_config_invalid_rejection(tmp_path: pathlib.Path) -> None:
    with pytest.raises(ValueError, match="invalid_max_queries_per_run"):
        build_runtime_config(_config(tmp_path, max_queries_per_run=0))

    with pytest.raises(ValueError, match="invalid_config"):
        build_runtime_config({"run_date": "2026-03-27"})


def test_runtime_config_no_mutation(tmp_path: pathlib.Path) -> None:
    config = _config(tmp_path)
    snapshot = copy.deepcopy(config)

    _ = build_runtime_config(config)

    assert config == snapshot


def test_default_runtime_config_contract(tmp_path: pathlib.Path) -> None:
    result = build_default_runtime_config(tmp_path)

    assert result["run_date"] == "1970-01-01"
    assert result["max_queries_per_run"] == 7
    assert result["max_results_per_query"] == 5
