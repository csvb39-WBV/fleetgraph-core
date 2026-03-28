from __future__ import annotations

from pathlib import Path


_REQUIRED_CONFIG_KEYS = {
    "run_date",
    "output_directory",
    "cache_path",
    "max_queries_per_run",
    "max_results_per_query",
    "connector_timeout_seconds",
    "connector_max_retries",
}


def _is_valid_int(value: object) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def _is_valid_float(value: object) -> bool:
    return (isinstance(value, int) or isinstance(value, float)) and not isinstance(value, bool)


def _is_valid_non_empty_string(value: object) -> bool:
    return isinstance(value, str) and value.strip() != ""


def build_runtime_config(config: dict) -> dict[str, object]:
    if not isinstance(config, dict):
        raise ValueError("invalid_config")
    if set(config.keys()) != _REQUIRED_CONFIG_KEYS:
        raise ValueError("invalid_config")
    if not _is_valid_non_empty_string(config["run_date"]):
        raise ValueError("invalid_run_date")
    if not _is_valid_non_empty_string(config["output_directory"]):
        raise ValueError("invalid_output_directory")
    if not _is_valid_non_empty_string(config["cache_path"]):
        raise ValueError("invalid_cache_path")
    if not _is_valid_int(config["max_queries_per_run"]) or config["max_queries_per_run"] <= 0:
        raise ValueError("invalid_max_queries_per_run")
    if not _is_valid_int(config["max_results_per_query"]) or config["max_results_per_query"] <= 0:
        raise ValueError("invalid_max_results_per_query")
    if not _is_valid_float(config["connector_timeout_seconds"]) or float(config["connector_timeout_seconds"]) <= 0:
        raise ValueError("invalid_connector_timeout_seconds")
    if not _is_valid_int(config["connector_max_retries"]) or config["connector_max_retries"] < 0:
        raise ValueError("invalid_connector_max_retries")

    return {
        "run_date": config["run_date"].strip(),
        "output_directory": str(Path(config["output_directory"]).resolve()),
        "cache_path": str(Path(config["cache_path"]).resolve()),
        "max_queries_per_run": config["max_queries_per_run"],
        "max_results_per_query": config["max_results_per_query"],
        "connector_timeout_seconds": float(config["connector_timeout_seconds"]),
        "connector_max_retries": config["connector_max_retries"],
    }


def build_default_runtime_config(project_root: str | Path) -> dict[str, object]:
    project_root_path = Path(project_root).resolve()
    return build_runtime_config(
        {
            "run_date": "1970-01-01",
            "output_directory": str(project_root_path / "data" / "runs" / "current"),
            "cache_path": str(project_root_path / "data" / "cache" / "signal_cache.json"),
            "max_queries_per_run": 7,
            "max_results_per_query": 5,
            "connector_timeout_seconds": 5.0,
            "connector_max_retries": 2,
        }
    )
