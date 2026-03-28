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


PRIMARY_RESULTS = {
    "company sued over project delays": {
        "title": "Atlas Build Group sued over project delays",
        "snippet": "Lawsuit filed against Atlas Build Group on 2026-03-27.",
        "url": "https://example.com/atlas-delays",
        "source_provider": "duckduckgo_api",
    },
    "contractor payment lawsuit filed project": {
        "title": "Harbor Concrete Partners payment lawsuit filed",
        "snippet": "Contractor payment lawsuit filed against Harbor Concrete Partners on 2026-03-27.",
        "url": "https://example.com/harbor-payment",
        "source_provider": "duckduckgo_html",
    },
    "developer dispute construction project": {
        "title": "Ridge Developers dispute construction project",
        "snippet": "Ridge Developers dispute construction project after payment delay on 2026-03-27.",
        "url": "https://example.com/ridge-dispute",
        "source_provider": "rss_news",
    },
    "mechanics lien filed against company services group": {
        "title": "Mechanics lien filed against Summit Concrete Services",
        "snippet": "Mechanics lien filed against Summit Concrete Services on 2026-03-27.",
        "url": "https://example.com/summit-lien",
        "source_provider": "duckduckgo_html",
    },
    "contractor default notice issued developer public project": {
        "title": "Ridge Utility Group default notice issued",
        "snippet": "Default notice issued to Ridge Utility Group on 2026-03-27.",
        "url": "https://example.com/ridge-default",
        "source_provider": "duckduckgo_api",
    },
    "federal investigation announced contractor infrastructure project": {
        "title": "Federal investigation announced against Civic Bridge Builders",
        "snippet": "Federal investigation announced against Civic Bridge Builders on 2026-03-27.",
        "url": "https://example.com/civic-investigation",
        "source_provider": "rss_news",
    },
    "fleet company accident investigation lawsuit": {
        "title": "Northstar Logistics accident investigation lawsuit",
        "snippet": "Accident investigation lawsuit filed against Northstar Logistics on 2026-03-27.",
        "url": "https://example.com/northstar-lawsuit",
        "source_provider": "duckduckgo_api",
    },
    "logistics company contract dispute filed": {
        "title": "Freightpath Logistics contract dispute filed",
        "snippet": "Contract dispute filed against Freightpath Logistics on 2026-03-27.",
        "url": "https://example.com/freightpath-dispute",
        "source_provider": "duckduckgo_html",
    },
    "trucking company investigation announced": {
        "title": "Beacon Hauling investigation announced",
        "snippet": "Investigation announced for Beacon Hauling on 2026-03-27.",
        "url": "https://example.com/beacon-hauling-investigation",
        "source_provider": "rss_news",
    },
    "service company lawsuit filed client dispute": {
        "title": "Signal Field Services lawsuit filed in client dispute",
        "snippet": "Lawsuit filed against Signal Field Services on 2026-03-27.",
        "url": "https://example.com/signal-field-lawsuit",
        "source_provider": "duckduckgo_api",
    },
    "field service company investigation announced": {
        "title": "FieldOps Service Group investigation announced",
        "snippet": "Investigation announced for FieldOps Service Group on 2026-03-27.",
        "url": "https://example.com/fieldops-investigation",
        "source_provider": "duckduckgo_html",
    },
    "manufacturing company supplier dispute lawsuit": {
        "title": "Cobalt Manufacturing supplier dispute lawsuit",
        "snippet": "Supplier dispute lawsuit filed against Cobalt Manufacturing on 2026-03-27.",
        "url": "https://example.com/cobalt-lawsuit",
        "source_provider": "rss_news",
    },
    "supply chain company contract dispute filed": {
        "title": "Harbor Supply Group contract dispute filed",
        "snippet": "Contract dispute filed against Harbor Supply Group on 2026-03-27.",
        "url": "https://example.com/harbor-supply-dispute",
        "source_provider": "duckduckgo_api",
    },
    "vendor dispute company investigation": {
        "title": "Meridian Vendor Group investigation opened after dispute",
        "snippet": "Investigation opened for Meridian Vendor Group after contract dispute on 2026-03-27.",
        "url": "https://example.com/meridian-vendor-investigation",
        "source_provider": "duckduckgo_html",
    },
}
FALLBACK_RESULTS = {
    "company sued project": {
        "title": "Atlas Build Group sued on project",
        "snippet": "Lawsuit filed against Atlas Build Group on 2026-03-27.",
        "url": "https://example.com/fallback-atlas-lawsuit",
        "source_provider": "duckduckgo_api",
    },
    "contractor delay lawsuit": {
        "title": "Harbor Concrete Partners delay lawsuit",
        "snippet": "Delay lawsuit filed against Harbor Concrete Partners on 2026-03-27.",
        "url": "https://example.com/fallback-harbor-delay",
        "source_provider": "duckduckgo_html",
    },
    "investigation announced company": {
        "title": "Beacon Masonry Services investigation announced",
        "snippet": "Investigation announced for Beacon Masonry Services on 2026-03-27.",
        "url": "https://example.com/fallback-beacon-investigation",
        "source_provider": "rss_news",
    },
    "default notice contractor": {
        "title": "Ridge Utility Group default notice",
        "snippet": "Default notice issued to Ridge Utility Group on 2026-03-27.",
        "url": "https://example.com/fallback-ridge-default",
        "source_provider": "duckduckgo_api",
    },
    "subpoena issued company": {
        "title": "Subpoena issued to Meridian Counsel Group",
        "snippet": "Subpoena issued to Meridian Counsel Group on 2026-03-27.",
        "url": "https://example.com/fallback-meridian-subpoena",
        "source_provider": "duckduckgo_html",
    },
}


def _runtime_config(tmp_path: pathlib.Path, **overrides: object) -> dict[str, object]:
    config: dict[str, object] = {
        "run_date": "2026-03-27",
        "output_directory": str(tmp_path / "outputs"),
        "cache_path": str(tmp_path / "cache" / "signal_cache.json"),
        "max_queries_per_run": 14,
        "max_results_per_query": 5,
        "connector_timeout_seconds": 5.0,
        "connector_max_retries": 1,
    }
    config.update(overrides)
    return config


class SuccessTransport:
    def __call__(self, query: str, result_limit: int, timeout_seconds: float) -> list[dict[str, str]]:
        return [PRIMARY_RESULTS[query]]


class PrimaryRejectedFallbackSuccessTransport:
    def __call__(self, query: str, result_limit: int, timeout_seconds: float) -> list[dict[str, str]]:
        if query in PRIMARY_RESULTS:
            return [
                {
                    "title": "Routine company bulletin posted",
                    "snippet": "Routine bulletin for company operations on 2026-03-27.",
                    "url": f"https://example.com/primary-rejected/{query.replace(' ', '-')}",
                    "source_provider": "duckduckgo_html",
                }
            ]
        return [FALLBACK_RESULTS[query]]


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


def test_runtime_execution_primary_pack_success_path(tmp_path: pathlib.Path) -> None:
    result = execute_signal_pipeline(
        _runtime_config(tmp_path),
        transport=SuccessTransport(),
        current_time=100,
    )

    debug_payload = json.loads(pathlib.Path(result["debug_path"]).read_text(encoding="utf-8"))

    assert result["ok"] is True
    assert result["manifest"]["status"] == "success"
    assert result["manifest"]["query_count_executed"] == 14
    assert result["manifest"]["cache_hits"] == 0
    assert result["manifest"]["cache_misses"] == 14
    assert result["manifest"]["source_success_count"] == 14
    assert result["manifest"]["suppressed_result_count"] == 0
    assert result["manifest"]["filtered_out_generic_company_count"] == 0
    assert result["manifest"]["fallback_triggered"] is False
    assert result["manifest"]["raw_results_count"] == 14
    assert result["manifest"]["extracted_signal_count"] == 14
    assert result["manifest"]["deduplicated_signal_count"] == 14
    assert result["manifest"]["retained_signal_count"] >= 5
    assert result["manifest"]["exported_signal_count"] >= 2
    assert debug_payload["query_pack_used"] == "primary"
    assert debug_payload["fallback_triggered"] is False
    assert len(debug_payload["query_execution"]) == 14
    assert all(entry["query_pack"] == "primary" for entry in debug_payload["query_execution"])


def test_runtime_execution_fallback_pack_triggered_when_primary_yields_zero(tmp_path: pathlib.Path) -> None:
    result = execute_signal_pipeline(
        _runtime_config(tmp_path),
        transport=PrimaryRejectedFallbackSuccessTransport(),
        current_time=100,
    )

    debug_payload = json.loads(pathlib.Path(result["debug_path"]).read_text(encoding="utf-8"))

    assert result["ok"] is True
    assert result["manifest"]["fallback_triggered"] is True
    assert result["manifest"]["query_count_executed"] == 19
    assert result["manifest"]["filtered_out_generic_company_count"] == 14
    assert debug_payload["query_pack_used"] == "fallback"
    assert debug_payload["fallback_triggered"] is True
    assert any(entry["query_pack"] == "fallback" for entry in debug_payload["query_execution"])
    assert any(signal["company"] == "Atlas Build Group" for signal in result["primary_signals"])


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
    assert result["manifest"]["fallback_triggered"] is False
    assert debug_payload["query_pack_used"] == "primary"
    assert debug_payload["fallback_triggered"] is False
    assert debug_payload["query_execution"][0]["error_code"] == "transport_failed"


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
    assert result["manifest"]["fallback_triggered"] is False


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
    monkeypatch.setattr(
        pipeline_execution_service,
        "get_fallback_query_definitions",
        lambda: [],
    )

    result = execute_signal_pipeline(
        _runtime_config(tmp_path, max_queries_per_run=1, max_results_per_query=1),
        transport=LowSignalTransport(),
        current_time=100,
    )

    assert result["ok"] is False
    assert result["error_code"] == "no_signals_detected"
    assert result["manifest"]["fallback_triggered"] is True


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
    assert result["manifest"]["fallback_triggered"] is False
    assert debug_payload["query_execution"][0] == {
        "query_pack": "primary",
        "query": "company sued over project delays",
        "source_used": "none",
        "result_count": 0,
        "suppressed_count": 0,
        "error_code": "no_results_returned",
    }


def test_runtime_execution_with_suppressed_educational_results_retains_event_results(tmp_path: pathlib.Path) -> None:
    def source_fetcher(provider: str, url: str, timeout_seconds: float) -> str:
        if provider == "duckduckgo_api":
            return '{"Results": [{"Heading": "Construction dispute explained", "AbstractText": "Lawsuit filed against Atlas Build Group on 2026-03-27.", "FirstURL": "https://example.com/explained"}], "RelatedTopics": []}'
        return '{"Results": [{"Heading": "Atlas Build Group sued over project delays", "AbstractText": "Lawsuit filed against Atlas Build Group on 2026-03-27.", "FirstURL": "https://example.com/atlas"}], "RelatedTopics": []}'

    result = execute_signal_pipeline(
        _runtime_config(tmp_path),
        source_fetcher=source_fetcher,
        current_time=100,
    )

    debug_payload = json.loads(pathlib.Path(result["debug_path"]).read_text(encoding="utf-8"))

    assert result["ok"] is True
    assert result["manifest"]["suppressed_result_count"] == 0
    assert debug_payload["query_pack_used"] == "primary"
    assert debug_payload["fallback_triggered"] is False


def test_runtime_execution_deterministic_fallback_output(tmp_path: pathlib.Path) -> None:
    first = execute_signal_pipeline(
        _runtime_config(tmp_path / "first"),
        transport=PrimaryRejectedFallbackSuccessTransport(),
        current_time=100,
    )
    second = execute_signal_pipeline(
        _runtime_config(tmp_path / "second"),
        transport=PrimaryRejectedFallbackSuccessTransport(),
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
