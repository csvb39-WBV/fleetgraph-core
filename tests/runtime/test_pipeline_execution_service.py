from __future__ import annotations

import copy
import json
import pathlib
import sys


PROJECT_ROOT = pathlib.Path(__file__).resolve().parents[2]
SRC_ROOT = PROJECT_ROOT / "src"

if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))


from fleetgraph.runtime.pipeline_execution_service import execute_signal_pipeline


def _runtime_config(tmp_path: pathlib.Path, **overrides: object) -> dict[str, object]:
    config: dict[str, object] = {
        "run_date": "2026-03-27",
        "output_directory": str(tmp_path / "outputs"),
        "cache_path": str(tmp_path / "cache" / "signal_cache.json"),
        "max_queries_per_run": 7,
        "max_results_per_query": 5,
        "connector_timeout_seconds": 5.0,
        "connector_max_retries": 1,
    }
    config.update(overrides)
    return config


class SuccessTransport:
    def __call__(self, query: str, result_limit: int, timeout_seconds: float) -> list[dict[str, str]]:
        if "mechanics lien" in query:
            return [{
                "title": "Acme Construction LLC sued in mechanics lien filing",
                "snippet": "Filed on 2026-03-27 after project payment dispute.",
                "url": "https://example.com/acme",
            }]
        if "audit findings" in query:
            return [{
                "title": "Beacon Builders audit findings released",
                "snippet": "2026-03-27 report cites cost overruns.",
                "url": "https://example.com/beacon",
            }]
        return [{
            "title": "Civic Contractors dispute escalates",
            "snippet": "2026-03-27 project delay dispute posted.",
            "url": "https://example.com/civic",
        }]


class FailureTransport:
    def __call__(self, query: str, result_limit: int, timeout_seconds: float) -> list[dict[str, str]]:
        raise RuntimeError("transport_failed")


def test_runtime_execution_success_path(tmp_path: pathlib.Path) -> None:
    result = execute_signal_pipeline(
        _runtime_config(tmp_path),
        transport=SuccessTransport(),
        current_time=100,
    )

    assert result["ok"] is True
    assert result["manifest"]["status"] == "success"
    assert result["manifest"]["query_count_executed"] == 7
    assert result["manifest"]["cache_hits"] == 0
    assert result["manifest"]["cache_misses"] == 7
    assert pathlib.Path(result["csv_path"]).exists() is True
    assert pathlib.Path(result["manifest_path"]).exists() is True


def test_runtime_execution_failure_path(tmp_path: pathlib.Path) -> None:
    result = execute_signal_pipeline(
        _runtime_config(tmp_path),
        transport=FailureTransport(),
        current_time=100,
    )

    assert result["ok"] is False
    assert result["manifest"]["status"] == "failed"
    assert result["manifest"]["error_code"] == "transport_failed"
    assert pathlib.Path(result["manifest_path"]).exists() is True


def test_runtime_execution_deterministic_manifest_output(tmp_path: pathlib.Path) -> None:
    first = execute_signal_pipeline(
        _runtime_config(tmp_path / "first"),
        transport=SuccessTransport(),
        current_time=100,
    )
    second = execute_signal_pipeline(
        _runtime_config(tmp_path / "second"),
        transport=SuccessTransport(),
        current_time=100,
    )

    first_manifest = dict(first["manifest"])
    second_manifest = dict(second["manifest"])
    first_manifest["csv_path"] = "normalized"
    second_manifest["csv_path"] = "normalized"

    assert first_manifest == second_manifest


def test_runtime_execution_no_mutation(tmp_path: pathlib.Path) -> None:
    config = _runtime_config(tmp_path)
    snapshot = copy.deepcopy(config)

    _ = execute_signal_pipeline(
        config,
        transport=SuccessTransport(),
        current_time=100,
    )

    assert config == snapshot
