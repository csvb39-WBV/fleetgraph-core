from __future__ import annotations

from pathlib import Path

from fleetgraph.cache.result_cache import ResultCache
from fleetgraph.connectors.web_search_connector import WebSearchConnector, WebSearchConnectorError
from fleetgraph.runtime.pipeline_execution_service import execute_signal_pipeline
from fleetgraph.runtime.runtime_config import build_runtime_config
from fleetgraph.watchlist.artifact_writer import write_watchlist_artifact
from fleetgraph.watchlist.delta_engine import build_company_delta_summary
from fleetgraph.watchlist.enrichment_coordinator import build_enrichment_record
from fleetgraph.watchlist.intelligence_service import (
    list_changed_companies,
    list_needs_review_companies,
    list_top_target_companies,
    write_watchlist_delta_summary,
)
from fleetgraph.watchlist.priority_engine import score_watchlist_company
from fleetgraph.watchlist.query_pack_generator import generate_company_query_pack
from fleetgraph.watchlist.read_service import get_watchlist_company_record, list_watchlist_company_records
from fleetgraph.watchlist.watchlist_loader import load_seed_enriched, load_verified_subset


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


def _load_dataset_records(dataset: str) -> list[dict[str, object]]:
    if dataset == "verified_subset":
        return load_verified_subset()
    if dataset == "seed_enriched":
        return load_seed_enriched()
    raise ValueError("invalid_watchlist_dataset")


def _build_delta_summary_for_company(
    *,
    previous_company: dict[str, object] | None,
    current_company: dict[str, object] | None,
    runtime_config: dict,
) -> dict[str, object]:
    delta_summary = build_company_delta_summary(previous_company, current_company)
    if current_company is not None:
        priority = score_watchlist_company(
            current_company,
            delta_summary=delta_summary,
            reference_date=str(runtime_config["run_date"]),
        )
        delta_summary["priority_score"] = priority["priority_score"]
        delta_summary["priority_reason_codes"] = priority["priority_reason_codes"]
    return delta_summary


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
    delta_paths: list[str] = []
    for watchlist_entity in watchlist_records:
        previous_company_result = get_watchlist_company_record(
            str(watchlist_entity["company_id"]),
            runtime_config=validated_runtime_config,
            dataset="verified_subset",
        )
        previous_company = previous_company_result["company"] if previous_company_result["ok"] is True else None
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
        current_company_result = get_watchlist_company_record(
            str(watchlist_entity["company_id"]),
            runtime_config=validated_runtime_config,
            dataset="verified_subset",
        )
        current_company = current_company_result["company"] if current_company_result["ok"] is True else None
        delta_summary = _build_delta_summary_for_company(
            previous_company=previous_company,
            current_company=current_company,
            runtime_config=validated_runtime_config,
        )
        delta_path = write_watchlist_delta_summary(
            delta_summary,
            runtime_config=validated_runtime_config,
            company_id=str(company_result["company_id"]),
        )
        enrichments.append(company_result)
        artifact_paths.append(artifact_path)
        delta_paths.append(delta_path)

    return {
        "mode": "watchlist",
        "ok": True,
        "run_date": validated_runtime_config["run_date"],
        "companies_processed": len(enrichments),
        "artifact_paths": artifact_paths,
        "delta_paths": delta_paths,
        "enrichments": enrichments,
        "error_code": None,
    }


def refresh_watchlist_company(
    runtime_config: dict,
    *,
    company_id: str,
    dataset: str = "verified_subset",
    transport: object | None = None,
    source_fetcher: object | None = None,
    current_time: int = 0,
) -> dict[str, object]:
    if not isinstance(company_id, str) or company_id.strip() == "":
        raise ValueError("invalid_company_id")
    validated_runtime_config = build_runtime_config(runtime_config)
    dataset_records = _load_dataset_records(dataset)
    watchlist_entity = next((record for record in dataset_records if record["company_id"] == company_id), None)
    if watchlist_entity is None:
        return {
            "mode": "watchlist",
            "ok": False,
            "company": None,
            "artifact_path": None,
            "delta_path": None,
            "delta_summary": None,
            "error_code": "unknown_company_id",
        }
    previous_company_result = get_watchlist_company_record(
        company_id,
        runtime_config=validated_runtime_config,
        dataset=dataset,
    )
    previous_company = previous_company_result["company"] if previous_company_result["ok"] is True else None
    connector = WebSearchConnector(
        timeout_seconds=validated_runtime_config["connector_timeout_seconds"],
        max_retries=validated_runtime_config["connector_max_retries"],
        transport=transport,
        source_fetcher=source_fetcher,
    )
    cache = ResultCache(validated_runtime_config["cache_path"], current_time=current_time)
    company_result = enrich_watchlist_company(
        watchlist_entity,
        connector=connector,
        cache=cache,
        run_date=str(validated_runtime_config["run_date"]),
    )
    artifact_output_directory = Path(validated_runtime_config["output_directory"]) / "watchlist"
    artifact_path = write_watchlist_artifact(
        company_result["enrichment_record"],
        artifact_output_directory,
        company_id=company_id,
    )
    merged_company_result = get_watchlist_company_record(
        company_id,
        runtime_config=validated_runtime_config,
        dataset=dataset,
    )
    current_company = merged_company_result["company"] if merged_company_result["ok"] is True else None
    delta_summary = _build_delta_summary_for_company(
        previous_company=previous_company,
        current_company=current_company,
        runtime_config=validated_runtime_config,
    )
    delta_path = write_watchlist_delta_summary(
        delta_summary,
        runtime_config=validated_runtime_config,
        company_id=company_id,
    )
    return {
        "mode": "watchlist",
        "ok": True,
        "company": merged_company_result["company"],
        "artifact_path": artifact_path,
        "delta_path": delta_path,
        "delta_summary": delta_summary,
        "refresh_result": company_result,
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


def list_watchlist_mode_companies(
    runtime_config: dict,
    *,
    dataset: str = "verified_subset",
) -> dict[str, object]:
    validated_runtime_config = build_runtime_config(runtime_config)
    return {
        "mode": "watchlist",
        "ok": True,
        "companies": list_watchlist_company_records(runtime_config=validated_runtime_config, dataset=dataset),
        "changed_companies": list_changed_companies(validated_runtime_config, dataset=dataset)["changed_companies"],
        "top_targets": list_top_target_companies(validated_runtime_config, dataset=dataset)["top_targets"],
        "needs_review": list_needs_review_companies(validated_runtime_config, dataset=dataset)["needs_review"],
        "error_code": None,
    }
