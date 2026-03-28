
from __future__ import annotations

from fleetgraph.cache.result_cache import ResultCache
from fleetgraph.connectors.web_search_connector import WebSearchConnector
from fleetgraph.control.dedup_engine import deduplicate_signals
from fleetgraph.control.query_budget_controller import validate_query_budget
from fleetgraph.output.csv_exporter import export_signals_to_csv
from fleetgraph.output.signal_output_formatter import format_signals
from fleetgraph.runtime.output_persistence import resolve_output_paths, write_debug_report, write_manifest
from fleetgraph.runtime.run_manifest import build_run_manifest
from fleetgraph.runtime.runtime_config import build_runtime_config
from fleetgraph.signals.query_library import get_fallback_query_definitions, get_ordered_query_definitions
from fleetgraph.signals.signal_extractor import extract_signal, get_signal_rejection_reason
from fleetgraph.signals.signal_filter_engine import filter_signals
from fleetgraph.signals.signal_scoring_engine import score_signals


def _build_debug_report(
    *,
    query_definitions: list[dict[str, object]],
    query_execution: list[dict[str, object]],
    query_pack_used: str,
    fallback_triggered: bool,
    suppressed_result_count: int,
    filtered_out_generic_company_count: int,
    raw_results: list[dict[str, str]],
    extracted_signals: list[dict[str, object]],
    deduplicated_signals: list[dict[str, object]],
    retained_signals: list[dict[str, object]],
    primary_signals: list[dict[str, object]],
) -> dict[str, object]:
    return {
        "queries": [
            {
                "query_pack": query_definition["query_pack"],
                "query_id": query_definition["query_id"],
                "query": query_definition["query"],
                "signal_type": query_definition["signal_type"],
                "max_results": query_definition["max_results"],
                "intent_type": query_definition["intent_type"],
            }
            for query_definition in query_definitions
        ],
        "query_pack_used": query_pack_used,
        "fallback_triggered": fallback_triggered,
        "suppressed_result_count": suppressed_result_count,
        "filtered_out_generic_company_count": filtered_out_generic_company_count,
        "query_execution": [
            {
                "query_pack": entry["query_pack"],
                "query": entry["query"],
                "source_used": entry["source_used"],
                "result_count": entry["result_count"],
                "suppressed_count": entry["suppressed_count"],
                "error_code": entry["error_code"],
            }
            for entry in query_execution
        ],
        "raw_results_count": len(raw_results),
        "extracted_signal_count": len(extracted_signals),
        "deduplicated_signal_count": len(deduplicated_signals),
        "retained_signal_count": len(retained_signals),
        "primary_signal_count": len(primary_signals),
        "sample_raw_results": [
            {
                "title": result_item["title"],
                "snippet": result_item["snippet"],
                "url": result_item["url"],
                "source_provider": result_item["source_provider"],
            }
            for result_item in raw_results[:5]
        ],
        "sample_extracted_signals": [
            {
                "company": signal["company"],
                "signal_type": signal["signal_type"],
                "event_summary": signal["event_summary"],
                "source": signal["source"],
                "date_detected": signal["date_detected"],
                "confidence_score": signal["confidence_score"],
                "priority": signal["priority"],
                "raw_text": signal["raw_text"],
            }
            for signal in extracted_signals[:5]
        ],
    }


def execute_signal_pipeline(
    runtime_config: dict,
    *,
    transport: object | None = None,
    source_fetcher: object | None = None,
    current_time: int = 0,
) -> dict[str, object]:
    validated_runtime_config = build_runtime_config(runtime_config)
    output_paths = resolve_output_paths(validated_runtime_config["output_directory"])
    cache = ResultCache(
        validated_runtime_config["cache_path"],
        current_time=current_time,
    )
    connector = WebSearchConnector(
        timeout_seconds=validated_runtime_config["connector_timeout_seconds"],
        max_retries=validated_runtime_config["connector_max_retries"],
        transport=transport,
        source_fetcher=source_fetcher,
    )

    primary_query_definitions = validate_query_budget(
        get_ordered_query_definitions()[: validated_runtime_config["max_queries_per_run"]],
        max_queries_per_run=validated_runtime_config["max_queries_per_run"],
        max_results_per_query=validated_runtime_config["max_results_per_query"],
    )
    fallback_query_definitions = validate_query_budget(
        get_fallback_query_definitions()[: validated_runtime_config["max_queries_per_run"]],
        max_queries_per_run=validated_runtime_config["max_queries_per_run"],
        max_results_per_query=validated_runtime_config["max_results_per_query"],
    )

    cache_hits = 0
    cache_misses = 0
    source_success_count = 0
    suppressed_result_count = 0
    filtered_out_generic_company_count = 0
    fallback_triggered = False
    query_pack_used = "primary"
    raw_results: list[dict[str, str]] = []
    extracted_signals: list[dict[str, object]] = []
    deduplicated_signals: list[dict[str, object]] = []
    retained_signals: list[dict[str, object]] = []
    primary_signals: list[dict[str, object]] = []
    executed_query_definitions: list[dict[str, object]] = []
    query_execution: list[dict[str, object]] = []

    def run_query_pack(query_definitions: list[dict[str, object]], *, query_pack: str) -> list[dict[str, object]]:
        nonlocal cache_hits
        nonlocal cache_misses
        nonlocal source_success_count
        nonlocal suppressed_result_count
        nonlocal filtered_out_generic_company_count

        pack_signals: list[dict[str, object]] = []
        for query_definition in query_definitions:
            query = query_definition["query"]
            result_limit = query_definition["max_results"]
            executed_query_definitions.append(
                {
                    "query_pack": query_pack,
                    "query_id": query_definition["query_id"],
                    "query": query_definition["query"],
                    "signal_type": query_definition["signal_type"],
                    "max_results": query_definition["max_results"],
                    "intent_type": query_definition["intent_type"],
                }
            )
            cached_results = cache.get(query)
            if cached_results is None:
                cache_misses += 1
                try:
                    cached_results = connector.search(query, result_limit=result_limit)
                except Exception as exc:
                    last_search_metadata = connector.get_last_search_metadata()
                    suppressed_result_count += int(last_search_metadata["suppressed_count"])
                    query_execution.append(
                        {
                            "query_pack": query_pack,
                            "query": query,
                            "source_used": last_search_metadata["source_used"],
                            "result_count": int(last_search_metadata["result_count"]),
                            "suppressed_count": int(last_search_metadata["suppressed_count"]),
                            "error_code": str(exc),
                        }
                    )
                    raise
                last_search_metadata = connector.get_last_search_metadata()
                suppressed_result_count += int(last_search_metadata["suppressed_count"])
                cache.set(query, cached_results)
                query_execution.append(
                    {
                        "query_pack": query_pack,
                        "query": query,
                        "source_used": str(last_search_metadata["source_used"]),
                        "result_count": len(cached_results),
                        "suppressed_count": int(last_search_metadata["suppressed_count"]),
                        "error_code": None,
                    }
                )
            else:
                cache_hits += 1
                query_execution.append(
                    {
                        "query_pack": query_pack,
                        "query": query,
                        "source_used": cached_results[0]["source_provider"] if len(cached_results) > 0 else "none",
                        "result_count": len(cached_results),
                        "suppressed_count": 0,
                        "error_code": None,
                    }
                )
            if len(cached_results) > 0:
                source_success_count += 1
            for result_item in cached_results:
                raw_results.append(
                    {
                        "title": result_item["title"],
                        "snippet": result_item["snippet"],
                        "url": result_item["url"],
                        "source_provider": result_item["source_provider"],
                    }
                )
                signal = extract_signal(
                    result_item,
                    signal_type=query_definition["signal_type"],
                )
                rejection_reason = get_signal_rejection_reason(signal)
                if rejection_reason == "generic_company":
                    filtered_out_generic_company_count += 1
                    continue
                extracted_signals.append(signal)
                pack_signals.append(signal)
        return pack_signals

    try:
        primary_pack_signals = run_query_pack(primary_query_definitions, query_pack="primary")
        if len(primary_pack_signals) == 0:
            fallback_triggered = True
            query_pack_used = "fallback"
            _ = run_query_pack(fallback_query_definitions, query_pack="fallback")

        deduplicated_signals = deduplicate_signals(extracted_signals)
        if len(deduplicated_signals) == 0:
            raise ValueError("no_signals_detected")
        scored_signals = score_signals(deduplicated_signals)
        filtered_signals = filter_signals(scored_signals)
        retained_signals = filtered_signals["retained_signals"]
        if len(filtered_signals["primary_signals"]) == 0:
            if not fallback_triggered:
                fallback_triggered = True
                query_pack_used = "fallback"
                _ = run_query_pack(fallback_query_definitions, query_pack="fallback")
            raise ValueError("no_signals_detected")
        primary_signals = format_signals(filtered_signals["primary_signals"])
        csv_path = export_signals_to_csv(
            primary_signals,
            output_paths["output_directory"],
            output_paths["csv_filename"],
        )
        manifest = build_run_manifest(
            {
                "run_date": validated_runtime_config["run_date"],
                "query_count_executed": len(query_execution),
                "cache_hits": cache_hits,
                "cache_misses": cache_misses,
                "source_success_count": source_success_count,
                "suppressed_result_count": suppressed_result_count,
                "filtered_out_generic_company_count": filtered_out_generic_company_count,
                "fallback_triggered": fallback_triggered,
                "raw_results_count": len(raw_results),
                "extracted_signal_count": len(extracted_signals),
                "deduplicated_signal_count": len(deduplicated_signals),
                "retained_signal_count": len(retained_signals),
                "exported_signal_count": len(primary_signals),
                "csv_path": csv_path,
                "status": "success",
                "error_code": None,
            }
        )
        manifest_path = write_manifest(manifest, output_paths["output_directory"])
        debug_report = _build_debug_report(
            query_definitions=executed_query_definitions,
            query_execution=query_execution,
            query_pack_used=query_pack_used,
            fallback_triggered=fallback_triggered,
            suppressed_result_count=suppressed_result_count,
            filtered_out_generic_company_count=filtered_out_generic_company_count,
            raw_results=raw_results,
            extracted_signals=extracted_signals,
            deduplicated_signals=deduplicated_signals,
            retained_signals=retained_signals,
            primary_signals=primary_signals,
        )
        debug_path = write_debug_report(debug_report, output_paths["output_directory"])
        return {
            "ok": True,
            "runtime_config": validated_runtime_config,
            "manifest": manifest,
            "manifest_path": manifest_path,
            "debug_path": debug_path,
            "csv_path": csv_path,
            "primary_signals": primary_signals,
            "error_code": None,
        }
    except Exception as exc:
        error_code = str(exc)
        manifest = build_run_manifest(
            {
                "run_date": validated_runtime_config["run_date"],
                "query_count_executed": len(query_execution),
                "cache_hits": cache_hits,
                "cache_misses": cache_misses,
                "source_success_count": source_success_count,
                "suppressed_result_count": suppressed_result_count,
                "filtered_out_generic_company_count": filtered_out_generic_company_count,
                "fallback_triggered": fallback_triggered,
                "raw_results_count": len(raw_results),
                "extracted_signal_count": len(extracted_signals),
                "deduplicated_signal_count": len(deduplicated_signals),
                "retained_signal_count": len(retained_signals),
                "exported_signal_count": 0,
                "csv_path": output_paths["csv_path"],
                "status": "failed",
                "error_code": error_code,
            }
        )
        manifest_path = write_manifest(manifest, output_paths["output_directory"])
        debug_report = _build_debug_report(
            query_definitions=executed_query_definitions,
            query_execution=query_execution,
            query_pack_used=query_pack_used,
            fallback_triggered=fallback_triggered,
            suppressed_result_count=suppressed_result_count,
            filtered_out_generic_company_count=filtered_out_generic_company_count,
            raw_results=raw_results,
            extracted_signals=extracted_signals,
            deduplicated_signals=deduplicated_signals,
            retained_signals=retained_signals,
            primary_signals=primary_signals,
        )
        debug_path = write_debug_report(debug_report, output_paths["output_directory"])
        return {
            "ok": False,
            "runtime_config": validated_runtime_config,
            "manifest": manifest,
            "manifest_path": manifest_path,
            "debug_path": debug_path,
            "csv_path": output_paths["csv_path"],
            "primary_signals": [],
            "error_code": error_code,
        }