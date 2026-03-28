from __future__ import annotations

import copy
import json
import pathlib
import sys

import pytest


PROJECT_ROOT = pathlib.Path(__file__).resolve().parents[2]
SRC_ROOT = PROJECT_ROOT / "src"

if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))


import fleetgraph.runtime.pipeline_execution_service as pipeline_execution_service
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
        if query == "lawsuit filed against contractor company major project":
            return [{
                "title": "Lawsuit filed against Atlas Build Group",
                "snippet": "Complaint filed against Atlas Build Group on 2026-03-27.",
                "url": "https://example.com/atlas-lawsuit",
                "source_provider": "duckduckgo_api",
            }]
        if query == "mechanics lien filed against company services group":
            return [{
                "title": "Mechanics lien filed against Summit Concrete Services",
                "snippet": "Mechanics lien filed against Summit Concrete Services on 2026-03-27.",
                "url": "https://example.com/summit-lien",
                "source_provider": "duckduckgo_html",
            }]
        if query == "developer sued contractor project delay infrastructure":
            return [{
                "title": "Developer sued Harbor Steel Partners over project delay",
                "snippet": "Developer sued Harbor Steel Partners over an infrastructure project delay on 2026-03-27.",
                "url": "https://example.com/harbor-delay",
                "source_provider": "rss_news",
            }]
        if query == "contractor default notice issued developer public project":
            return [{
                "title": "Ridge Utility Group default notice issued on public project",
                "snippet": "Ridge Utility Group received a default notice on 2026-03-27.",
                "url": "https://example.com/ridge-default",
                "source_provider": "duckduckgo_api",
            }]
        if query == "audit investigation company project firm holdings":
            return [{
                "title": "Beacon Masonry Services audit investigation announced",
                "snippet": "Audit investigation announced for Beacon Masonry Services on March 27, 2026.",
                "url": "https://example.com/beacon-audit",
                "source_provider": "rss_news",
            }]
        if query == "federal investigation announced contractor infrastructure project":
            return [{
                "title": "Federal investigation announced against Civic Bridge Builders",
                "snippet": "Federal investigation announced against Civic Bridge Builders on 2026-03-27.",
                "url": "https://example.com/civic-government",
                "source_provider": "duckduckgo_api",
            }]
        return [{
            "title": "Subpoena issued to Meridian Counsel in litigation",
            "snippet": "Subpoena issued to Meridian Counsel in litigation on 2026-03-27.",
            "url": "https://example.com/meridian-subpoena",
            "source_provider": "duckduckgo_html",
        }]


class FailureTransport:
    def __call__(self, query: str, result_limit: int, timeout_seconds: float) -> list[dict[str, str]]:
        raise RuntimeError("transport_failed")


class EmptyResultsTransport:
    def __call__(self, query: str, result_limit: int, timeout_seconds: float) -> list[dict[str, str]]:
        return []


class LowSignalTransport:
    def __call__(self, query: str, result_limit: int, timeout_seconds: float) -> list[dict[str, str]]:
        return [{
            "title": "Civic Contractors routine bulletin posted",
            "snippet": "2026-03-27 routine notice posted.",
            "url": "https://example.com/civic-review",
            "source_provider": "duckduckgo_html",
        }]


def test_runtime_execution_success_path(tmp_path: pathlib.Path) -> None:
    result = execute_signal_pipeline(
        _runtime_config(tmp_path),
        transport=SuccessTransport(),
        current_time=100,
    )

    debug_payload = json.loads(pathlib.Path(result["debug_path"]).read_text(encoding="utf-8"))

    assert result["ok"] is True
    assert result["manifest"]["status"] == "success"
    assert result["manifest"]["query_count_executed"] == 7
    assert result["manifest"]["cache_hits"] == 0
    assert result["manifest"]["cache_misses"] == 7
    assert result["manifest"]["source_success_count"] == 7
    assert result["manifest"]["suppressed_result_count"] == 0
    assert result["manifest"]["raw_results_count"] == 7
    assert result["manifest"]["extracted_signal_count"] == 7
    assert result["manifest"]["deduplicated_signal_count"] == 7
    assert result["manifest"]["retained_signal_count"] >= 5
    assert result["manifest"]["exported_signal_count"] >= 2
    assert len(result["primary_signals"]) >= 2
    assert pathlib.Path(result["csv_path"]).exists() is True
    assert pathlib.Path(result["manifest_path"]).exists() is True
    assert pathlib.Path(result["debug_path"]).exists() is True
    assert debug_payload["suppressed_result_count"] == 0
    assert debug_payload["raw_results_count"] == 7
    assert debug_payload["extracted_signal_count"] == 7
    assert debug_payload["deduplicated_signal_count"] == 7
    assert debug_payload["retained_signal_count"] >= 5
    assert debug_payload["primary_signal_count"] >= 2
    assert len(debug_payload["sample_raw_results"]) == 5
    assert len(debug_payload["sample_extracted_signals"]) == 5
    assert len(debug_payload["query_execution"]) == 7
    assert debug_payload["query_execution"][0] == {
        "query": "lawsuit filed against contractor company major project",
        "source_used": "duckduckgo_api",
        "result_count": 1,
        "suppressed_count": 0,
        "error_code": None,
    }


def test_runtime_execution_failure_path(tmp_path: pathlib.Path) -> None:
    result = execute_signal_pipeline(
        _runtime_config(tmp_path),
        transport=FailureTransport(),
        current_time=100,
    )

    debug_payload = json.loads(pathlib.Path(result["debug_path"]).read_text(encoding="utf-8"))

    assert result["ok"] is False
    assert result["manifest"]["status"] == "failed"
    assert result["manifest"]["error_code"] == "transport_failed"
    assert pathlib.Path(result["manifest_path"]).exists() is True
    assert pathlib.Path(result["debug_path"]).exists() is True
    assert debug_payload["suppressed_result_count"] == 0
    assert debug_payload["raw_results_count"] == 0
    assert debug_payload["query_execution"][0] == {
        "query": "lawsuit filed against contractor company major project",
        "source_used": "none",
        "result_count": 0,
        "suppressed_count": 0,
        "error_code": "transport_failed",
    }


def test_zero_deduplicated_signals_detected(tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(pipeline_execution_service, "deduplicate_signals", lambda signals: [])

    result = execute_signal_pipeline(
        _runtime_config(tmp_path),
        transport=SuccessTransport(),
        current_time=100,
    )

    assert result["ok"] is False
    assert result["error_code"] == "no_signals_detected"
    assert result["manifest"]["status"] == "failed"
    assert result["manifest"]["error_code"] == "no_signals_detected"


def test_zero_primary_signals_detected(tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        pipeline_execution_service,
        "get_ordered_query_definitions",
        lambda: [
            {
                "query_id": "government_weak",
                "signal_type": "government",
                "query": "routine bulletin contractor",
                "priority_weight": 1,
                "max_results": 1,
                "intent_type": "event_based",
            }
        ],
    )

    result = execute_signal_pipeline(
        _runtime_config(tmp_path, max_queries_per_run=1, max_results_per_query=1),
        transport=LowSignalTransport(),
        current_time=100,
    )

    assert result["ok"] is False
    assert result["error_code"] == "no_primary_signals_detected"
    assert result["manifest"]["status"] == "failed"
    assert result["manifest"]["error_code"] == "no_primary_signals_detected"


def test_connector_empty_manifest_failure_output(tmp_path: pathlib.Path) -> None:
    result = execute_signal_pipeline(
        _runtime_config(tmp_path),
        transport=EmptyResultsTransport(),
        current_time=100,
    )

    debug_payload = json.loads(pathlib.Path(result["debug_path"]).read_text(encoding="utf-8"))

    assert result["ok"] is False
    assert result["error_code"] == "no_results_returned"
    assert result["manifest"]["status"] == "failed"
    assert result["manifest"]["error_code"] == "no_results_returned"
    assert debug_payload["query_execution"][0] == {
        "query": "lawsuit filed against contractor company major project",
        "source_used": "none",
        "result_count": 0,
        "suppressed_count": 0,
        "error_code": "no_results_returned",
    }


def test_runtime_execution_with_suppressed_educational_results_retains_event_results(tmp_path: pathlib.Path) -> None:
    def source_fetcher(provider: str, url: str, timeout_seconds: float) -> str:
        if provider == "duckduckgo_api":
            return '{"Results": [{"Heading": "Construction litigation guide", "AbstractText": "Complete guide and FAQ for contractor disputes.", "FirstURL": "https://example.com/guide"}, {"Heading": "Lawsuit filed against Atlas Build Group", "AbstractText": "Complaint filed against Atlas Build Group on 2026-03-27.", "FirstURL": "https://example.com/atlas-lawsuit"}], "RelatedTopics": []}'
        return '{"Results": [{"Heading": "Federal investigation announced against Civic Bridge Builders", "AbstractText": "Federal investigation announced against Civic Bridge Builders on 2026-03-27.", "FirstURL": "https://example.com/civic"}], "RelatedTopics": []}'

    result = execute_signal_pipeline(
        _runtime_config(tmp_path),
        source_fetcher=source_fetcher,
        current_time=100,
    )

    debug_payload = json.loads(pathlib.Path(result["debug_path"]).read_text(encoding="utf-8"))

    assert result["ok"] is True
    assert result["manifest"]["suppressed_result_count"] >= 1
    assert debug_payload["suppressed_result_count"] >= 1
    assert debug_payload["query_execution"][0]["suppressed_count"] >= 1
    assert any(signal["company"] == "Atlas Build Group" for signal in result["primary_signals"])


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
    first_debug = json.loads(pathlib.Path(first["debug_path"]).read_text(encoding="utf-8"))
    second_debug = json.loads(pathlib.Path(second["debug_path"]).read_text(encoding="utf-8"))
    first_manifest["csv_path"] = "normalized"
    second_manifest["csv_path"] = "normalized"

    assert first_manifest == second_manifest
    assert first_debug == second_debug


def test_runtime_execution_no_mutation(tmp_path: pathlib.Path) -> None:
    config = _runtime_config(tmp_path)
    snapshot = copy.deepcopy(config)

    _ = execute_signal_pipeline(
        config,
        transport=SuccessTransport(),
        current_time=100,
    )

    assert config == snapshot
