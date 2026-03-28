from __future__ import annotations


_REQUIRED_MANIFEST_KEYS = {
    "run_date",
    "query_count_executed",
    "cache_hits",
    "cache_misses",
    "source_success_count",
    "suppressed_result_count",
    "filtered_out_generic_company_count",
    "fallback_triggered",
    "raw_results_count",
    "extracted_signal_count",
    "deduplicated_signal_count",
    "retained_signal_count",
    "exported_signal_count",
    "csv_path",
    "status",
    "error_code",
}
_VALID_STATUSES = {"success", "failed"}


def _is_valid_int(value: object) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def _is_valid_non_empty_string(value: object) -> bool:
    return isinstance(value, str) and value.strip() != ""


def build_run_manifest(manifest: dict) -> dict[str, object]:
    if not isinstance(manifest, dict):
        raise ValueError("invalid_manifest")
    if set(manifest.keys()) != _REQUIRED_MANIFEST_KEYS:
        raise ValueError("invalid_manifest")
    if not _is_valid_non_empty_string(manifest["run_date"]):
        raise ValueError("invalid_run_date")
    for field_name in (
        "query_count_executed",
        "cache_hits",
        "cache_misses",
        "source_success_count",
        "suppressed_result_count",
        "filtered_out_generic_company_count",
        "raw_results_count",
        "extracted_signal_count",
        "deduplicated_signal_count",
        "retained_signal_count",
        "exported_signal_count",
    ):
        if not _is_valid_int(manifest[field_name]) or manifest[field_name] < 0:
            raise ValueError(f"invalid_{field_name}")
    if not isinstance(manifest["fallback_triggered"], bool):
        raise ValueError("invalid_fallback_triggered")
    if not _is_valid_non_empty_string(manifest["csv_path"]):
        raise ValueError("invalid_csv_path")
    if manifest["status"] not in _VALID_STATUSES:
        raise ValueError("invalid_status")
    error_code = manifest["error_code"]
    if error_code is not None and not _is_valid_non_empty_string(error_code):
        raise ValueError("invalid_error_code")

    return {
        "run_date": manifest["run_date"].strip(),
        "query_count_executed": manifest["query_count_executed"],
        "cache_hits": manifest["cache_hits"],
        "cache_misses": manifest["cache_misses"],
        "source_success_count": manifest["source_success_count"],
        "suppressed_result_count": manifest["suppressed_result_count"],
        "filtered_out_generic_company_count": manifest["filtered_out_generic_company_count"],
        "fallback_triggered": manifest["fallback_triggered"],
        "raw_results_count": manifest["raw_results_count"],
        "extracted_signal_count": manifest["extracted_signal_count"],
        "deduplicated_signal_count": manifest["deduplicated_signal_count"],
        "retained_signal_count": manifest["retained_signal_count"],
        "exported_signal_count": manifest["exported_signal_count"],
        "csv_path": manifest["csv_path"].strip(),
        "status": manifest["status"],
        "error_code": error_code,
    }
