from __future__ import annotations

from fleetgraph.cache.result_cache import ResultCache
from fleetgraph.control.dedup_engine import deduplicate_signals
from fleetgraph.control.query_budget_controller import validate_query_budget
from fleetgraph.connectors.web_search_connector import WebSearchConnector
from fleetgraph.signals.query_library import get_ordered_query_definitions
from fleetgraph.signals.signal_extractor import extract_signal


def run_signal_acquisition(
    *,
    cache: ResultCache,
    connector: WebSearchConnector,
    max_queries_per_run: int,
    max_results_per_query: int,
) -> list[dict[str, object]]:
    query_definitions = validate_query_budget(
        get_ordered_query_definitions(),
        max_queries_per_run=max_queries_per_run,
        max_results_per_query=max_results_per_query,
    )

    signals: list[dict[str, object]] = []
    for query_definition in query_definitions:
        query = query_definition["query"]
        result_limit = query_definition["max_results"]
        cached_results = cache.get(query)
        if cached_results is None:
            cached_results = connector.search(query, result_limit=result_limit)
            cache.set(query, cached_results)
        for result_item in cached_results:
            signals.append(
                extract_signal(
                    result_item,
                    signal_type=query_definition["signal_type"],
                )
            )
    return deduplicate_signals(signals)
