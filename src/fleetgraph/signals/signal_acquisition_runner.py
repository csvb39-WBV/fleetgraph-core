
from __future__ import annotations

from fleetgraph.cache.result_cache import ResultCache
from fleetgraph.control.dedup_engine import deduplicate_signals
from fleetgraph.control.query_budget_controller import validate_query_budget
from fleetgraph.connectors.web_search_connector import WebSearchConnector
from fleetgraph.signals.query_library import get_ordered_query_definitions
from fleetgraph.signals.signal_extractor import extract_signal, get_signal_rejection_reason


def _resolve_acquisition_query(query_definition: dict[str, object]) -> str:
    query_id = str(query_definition["query_id"])
    query = str(query_definition["query"])

    if query_id == "government_audit_investigation_contractor":
        return "audit investigation announced contractor project"

    return query


def run_signal_acquisition(
    *,
    cache: ResultCache,
    connector: WebSearchConnector,
    max_queries_per_run: int,
    max_results_per_query: int,
) -> list[dict[str, object]]:
    query_definitions = validate_query_budget(
        get_ordered_query_definitions()[:max_queries_per_run],
        max_queries_per_run=max_queries_per_run,
        max_results_per_query=max_results_per_query,
    )

    signals: list[dict[str, object]] = []
    for query_definition in query_definitions:
        query = _resolve_acquisition_query(query_definition)
        result_limit = int(query_definition["max_results"])

        cached_results = cache.get(query)
        if cached_results is None:
            cached_results = connector.search(query, result_limit=result_limit)
            cache.set(query, cached_results)

        for result_item in cached_results:
            signal = extract_signal(
                result_item,
                signal_type=str(query_definition["signal_type"]),
            )

            rejection_reason = get_signal_rejection_reason(signal)
            if rejection_reason == "generic_company":
                continue

            normalized_signal = {
                "company": str(signal["company"]).strip(),
                "signal_type": str(signal["signal_type"]).strip(),
                "event_summary": str(signal["event_summary"]).strip(),
                "source": str(signal["source"]).strip(),
                "date_detected": str(signal["date_detected"]).strip() or "unknown",
                "confidence_score": None,
                "priority": None,
                "raw_text": str(signal["raw_text"]).strip(),
            }

            if normalized_signal["event_summary"] == "":
                normalized_signal["event_summary"] = normalized_signal["raw_text"]

            if normalized_signal["source"] == "":
                normalized_signal["source"] = "unknown"

            signals.append(normalized_signal)

    return deduplicate_signals(signals)