from __future__ import annotations

from pathlib import Path

from fleetgraph.cache.result_cache import ResultCache
from fleetgraph.connectors.web_search_connector import WebSearchConnector, WebSearchConnectorError
from fleetgraph.runtime.pipeline_execution_service import execute_signal_pipeline
from fleetgraph.runtime.runtime_config import build_runtime_config
from fleetgraph.watchlist.artifact_writer import write_watchlist_artifact
from fleetgraph.watchlist.enrichment_coordinator import build_enrichment_record
from fleetgraph.watchlist.query_pack_generator import generate_company_query_pack


def _deduplicate_results(search_results: list[dict[str, str]]) -> list[dict[str, str]]:
    deduplicated_results: list[dict[str, str]] = []
    seen_keys: set[tuple[str, str, str, str]] = set()
    for result_item in search_results:
        result_key = (
            result_item["title"],
            result_item["snippet"],
            result_item["url"],
            result_item["source_provider"],
        )
        if result_key not in seen_keys:
            seen_keys.add(result_key)
            deduplicated_results.append(result_item)
    return deduplicated_results


def enrich_watchlist_company(
    watchlist_entity: dict[str, object],
    *,
    connector: WebSearchConnector,
    cache: ResultCache,
    run_date: str,
) -> dict[str, object]:
    query_pack = generate_company_query_pack(watchlist_entity)
    raw_results: list[dict[str, str]] = []
    query_execution: list[dict[str, object]] = []
    for query_definition in query_pack:
        query = query_definition["query"]
        cached_results = cache.get(query)
        if cached_results is None:
            try:
                cached_results = connector.search(query, result_limit=int(query_definition["max_results"]))
                cache.set(query, cached_results)
            except WebSearchConnectorError as exc:
                if str(exc) == "no_results_returned":
                    cached_results = []
                else:
                    cached_results = []
        query_execution.append(
            {
                "query_id": query_definition["query_id"],
                "query": query,
                "result_count": len(cached_results),
            }
        )
        raw_results.extend(cached_results)
    deduplicated_results = _deduplicate_results(raw_results)
    enrichment_record = build_enrichment_record(watchlist_entity, deduplicated_results, run_date=run_date)
    return {
        "company_id": watchlist_entity["company_id"],
        "query_pack": query_pack,
        "query_execution": query_execution,
        "raw_results": deduplicated_results,
        "enrichment_record": enrichment_record,
    }


def execute_watchlist_mode(
    runtime_config: dict,
    *,
    watchlist_records: list[dict[str, object]],
    transport: object | None = None,
    source_fetcher: object | None = None,
    current_time: int = 0,
) -> dict[str, object]:
    validated_runtime_config = build_runtime_config(runtime_config)
    if not isinstance(watchlist_records, list) or len(watchlist_records) == 0:
        raise ValueError("invalid_watchlist_records")
    connector = WebSearchConnector(
        timeout_seconds=validated_runtime_config["connector_timeout_seconds"],
        max_retries=validated_runtime_config["connector_max_retries"],
        transport=transport,
        source_fetcher=source_fetcher,
    )
    cache = ResultCache(validated_runtime_config["cache_path"], current_time=current_time)
    artifact_output_directory = Path(validated_runtime_config["output_directory"]) / "watchlist"

    enrichments: list[dict[str, object]] = []
    artifact_paths: list[str] = []
    for watchlist_entity in watchlist_records:
        company_result = enrich_watchlist_company(
            watchlist_entity,
            connector=connector,
            cache=cache,
            run_date=str(validated_runtime_config["run_date"]),
        )
        artifact_path = write_watchlist_artifact(
            company_result["enrichment_record"],
            artifact_output_directory,
            company_id=str(company_result["company_id"]),
        )
        enrichments.append(company_result)
        artifact_paths.append(artifact_path)

    return {
        "mode": "watchlist",
        "ok": True,
        "run_date": validated_runtime_config["run_date"],
        "companies_processed": len(enrichments),
        "artifact_paths": artifact_paths,
        "enrichments": enrichments,
        "error_code": None,
    }


def execute_platform_mode(
    *,
    mode: str,
    runtime_config: dict,
    watchlist_records: list[dict[str, object]] | None = None,
    transport: object | None = None,
    source_fetcher: object | None = None,
    current_time: int = 0,
) -> dict[str, object]:
    if mode == "discovery":
        discovery_result = execute_signal_pipeline(
            runtime_config,
            transport=transport,
            source_fetcher=source_fetcher,
            current_time=current_time,
        )
        discovery_result["mode"] = "discovery"
        return discovery_result
    if mode == "watchlist":
        return execute_watchlist_mode(
            runtime_config,
            watchlist_records=watchlist_records or [],
            transport=transport,
            source_fetcher=source_fetcher,
            current_time=current_time,
        )
    raise ValueError("invalid_mode")
