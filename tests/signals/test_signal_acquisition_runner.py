from __future__ import annotations

from pathlib import Path

from fleetgraph.cache.result_cache import ResultCache
from fleetgraph.connectors.web_search_connector import WebSearchConnector
from fleetgraph.signals.signal_acquisition_runner import run_signal_acquisition


class RecordingTransport:
    def __init__(self) -> None:
        self.calls: list[tuple[str, int, float]] = []

    def __call__(self, query: str, result_limit: int, timeout_seconds: float) -> list[dict[str, str]]:
        self.calls.append((query, result_limit, timeout_seconds))
        if "mechanics lien" in query:
            return [
                {
                    "title": "Acme Construction LLC sued in mechanics lien filing",
                    "snippet": "Filed on 2026-03-26 after project payment dispute.",
                    "url": "https://example.com/acme-lien",
                    "source_provider": "duckduckgo_api",
                }
            ]
        if "audit construction company" in query:
            return [
                {
                    "title": "Beacon Builders audit findings released",
                    "snippet": "March 12, 2026 report cites cost overruns.",
                    "url": "https://example.com/beacon-audit",
                    "source_provider": "rss_news",
                }
            ]
        return [
            {
                "title": "Civic Contractors government investigation announced",
                "snippet": "2026-03-20 review concerns procurement conduct.",
                "url": "https://example.com/civic-investigation",
                "source_provider": "duckduckgo_html",
            }
        ]


def test_acquisition_runner_deterministic_output(tmp_path: Path) -> None:
    cache = ResultCache(tmp_path / "cache.json", current_time=100)
    transport = RecordingTransport()
    connector = WebSearchConnector(transport=transport)

    first = run_signal_acquisition(
        cache=cache,
        connector=connector,
        max_queries_per_run=7,
        max_results_per_query=5,
    )
    second = run_signal_acquisition(
        cache=ResultCache(tmp_path / "cache.json", current_time=100),
        connector=WebSearchConnector(transport=transport),
        max_queries_per_run=7,
        max_results_per_query=5,
    )

    assert first == second
    assert all(signal["confidence_score"] is None for signal in first)
    assert all(signal["priority"] is None for signal in first)
    assert [signal["company"] for signal in first] == [
        "Civic Contractors",
        "Acme Construction LLC",
        "Beacon Builders",
    ]


def test_acquisition_runner_cache_path_behavior(tmp_path: Path) -> None:
    cache_path = tmp_path / "cache.json"
    transport = RecordingTransport()
    connector = WebSearchConnector(transport=transport)

    _ = run_signal_acquisition(
        cache=ResultCache(cache_path, current_time=100),
        connector=connector,
        max_queries_per_run=7,
        max_results_per_query=5,
    )
    first_call_count = len(transport.calls)

    _ = run_signal_acquisition(
        cache=ResultCache(cache_path, current_time=200),
        connector=WebSearchConnector(transport=transport),
        max_queries_per_run=7,
        max_results_per_query=5,
    )

    assert len(transport.calls) == first_call_count


def test_acquisition_runner_output_contract(tmp_path: Path) -> None:
    transport = RecordingTransport()
    signals = run_signal_acquisition(
        cache=ResultCache(tmp_path / "cache.json", current_time=100),
        connector=WebSearchConnector(transport=transport),
        max_queries_per_run=7,
        max_results_per_query=5,
    )

    assert all(set(signal.keys()) == {
        "company",
        "signal_type",
        "event_summary",
        "source",
        "date_detected",
        "confidence_score",
        "priority",
        "raw_text",
    } for signal in signals)
