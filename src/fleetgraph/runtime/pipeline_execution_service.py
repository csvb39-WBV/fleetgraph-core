from __future__ import annotations

from fleetgraph.cache.result_cache import ResultCache
from fleetgraph.connectors.web_search_connector import WebSearchConnector
from fleetgraph.control.dedup_engine import deduplicate_signals
from fleetgraph.control.query_budget_controller import validate_query_budget
from fleetgraph.output.csv_exporter import export_signals_to_csv
from fleetgraph.output.signal_output_formatter import format_signals
from fleetgraph.runtime.output_persistence import resolve_output_paths, write_manifest
from fleetgraph.runtime.run_manifest import build_run_manifest
from fleetgraph.runtime.runtime_config import build_runtime_config
from fleetgraph.signals.query_library import get_ordered_query_definitions
from fleetgraph.signals.signal_extractor import extract_signal
from fleetgraph.signals.signal_filter_engine import filter_signals
from fleetgraph.signals.signal_scoring_engine import score_signals


def execute_signal_pipeline(
    runtime_config: dict,
    *,
    transport: object | None = None,
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
    )

    query_definitions = validate_query_budget(
        get_ordered_query_definitions(),
        max_queries_per_run=validated_runtime_config["max_queries_per_run"],
        max_results_per_query=validated_runtime_config["max_results_per_query"],
    )

    cache_hits = 0
    cache_misses = 0
    raw_signals: list[dict[str, object]] = []

    try:
        for query_definition in query_definitions:
            query = query_definition["query"]
            result_limit = query_definition["max_results"]
            cached_results = cache.get(query)
            if cached_results is None:
                cache_misses += 1
                cached_results = connector.search(query, result_limit=result_limit)
                cache.set(query, cached_results)
            else:
                cache_hits += 1
            for result_item in cached_results:
                raw_signals.append(
                    extract_signal(
                        result_item,
                        signal_type=query_definition["signal_type"],
                    )
                )

        deduplicated_signals = deduplicate_signals(raw_signals)
        scored_signals = score_signals(deduplicated_signals)
        filtered_signals = filter_signals(scored_signals)
        primary_signals = format_signals(filtered_signals["primary_signals"])
        csv_path = export_signals_to_csv(
            primary_signals,
            output_paths["output_directory"],
            output_paths["csv_filename"],
        )
        manifest = build_run_manifest(
            {
                "run_date": validated_runtime_config["run_date"],
                "query_count_executed": len(query_definitions),
                "cache_hits": cache_hits,
                "cache_misses": cache_misses,
                "retained_signal_count": len(filtered_signals["retained_signals"]),
                "exported_signal_count": len(primary_signals),
                "csv_path": csv_path,
                "status": "success",
                "error_code": None,
            }
        )
        manifest_path = write_manifest(manifest, output_paths["output_directory"])
        return {
            "ok": True,
            "runtime_config": validated_runtime_config,
            "manifest": manifest,
            "manifest_path": manifest_path,
            "csv_path": csv_path,
            "primary_signals": primary_signals,
            "error_code": None,
        }
    except Exception as exc:
        error_code = str(exc)
        manifest = build_run_manifest(
            {
                "run_date": validated_runtime_config["run_date"],
                "query_count_executed": len(query_definitions),
                "cache_hits": cache_hits,
                "cache_misses": cache_misses,
                "retained_signal_count": 0,
                "exported_signal_count": 0,
                "csv_path": output_paths["csv_path"],
                "status": "failed",
                "error_code": error_code,
            }
        )
        manifest_path = write_manifest(manifest, output_paths["output_directory"])
        return {
            "ok": False,
            "runtime_config": validated_runtime_config,
            "manifest": manifest,
            "manifest_path": manifest_path,
            "csv_path": output_paths["csv_path"],
            "primary_signals": [],
            "error_code": error_code,
        }
