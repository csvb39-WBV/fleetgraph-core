from __future__ import annotations

import json
import pathlib
import sys


PROJECT_ROOT = pathlib.Path(__file__).resolve().parents[2]
SRC_ROOT = PROJECT_ROOT / "src"

if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))


import fleetgraph.watchlist.watchlist_mode_service as watchlist_mode_service
from fleetgraph.watchlist.watchlist_loader import load_verified_subset
from fleetgraph.watchlist.watchlist_mode_service import execute_platform_mode, execute_watchlist_mode


class WatchlistTransport:
    def __call__(self, query: str, result_limit: int, timeout_seconds: float) -> list[dict[str, str]]:
        if "lawsuit filed" in query:
            return [
                {
                    "title": "Turner Construction lawsuit filed over project delay",
                    "snippet": "Public email jane.doe@turnerconstruction.com appears in the filing.",
                    "url": "https://example.com/turner-lawsuit",
                    "source_provider": "rss_news",
                }
            ]
        if "audit investigation" in query:
            return [
                {
                    "title": "Turner Construction investigation announced",
                    "snippet": "Investigation announced for Turner Construction on a major project.",
                    "url": "https://example.com/turner-investigation",
                    "source_provider": "duckduckgo_html",
                }
            ]
        return [
            {
                "title": "Turner Construction dispute notice",
                "snippet": "Project dispute filed against Turner Construction.",
                "url": "https://example.com/turner-dispute",
                "source_provider": "duckduckgo_api",
            }
        ]


class EmptyHitTransport:
    def __call__(self, query: str, result_limit: int, timeout_seconds: float) -> list[dict[str, str]]:
        return []


def _runtime_config(tmp_path: pathlib.Path) -> dict[str, object]:
    return {
        "run_date": "2026-03-28",
        "output_directory": str(tmp_path / "outputs"),
        "cache_path": str(tmp_path / "cache" / "watchlist_cache.json"),
        "max_queries_per_run": 14,
        "max_results_per_query": 5,
        "connector_timeout_seconds": 5.0,
        "connector_max_retries": 1,
    }


def test_watchlist_mode_is_explicit_and_enriches_one_company(tmp_path: pathlib.Path) -> None:
    company = load_verified_subset()[0]

    result = execute_watchlist_mode(
        _runtime_config(tmp_path),
        watchlist_records=[company],
        transport=WatchlistTransport(),
        current_time=100,
    )

    assert result["mode"] == "watchlist"
    assert result["ok"] is True
    assert result["companies_processed"] == 1
    assert pathlib.Path(result["artifact_paths"][0]).exists() is True
    artifact_payload = json.loads(pathlib.Path(result["artifact_paths"][0]).read_text(encoding="utf-8"))
    assert artifact_payload["company_name"] == "Turner Construction"
    assert artifact_payload["published_emails"] == [
        {
            "email": "jane.doe@turnerconstruction.com",
            "source_url": "https://example.com/turner-lawsuit",
            "confidence": "high",
        }
    ]


def test_empty_hits_return_deterministic_empty_artifact(tmp_path: pathlib.Path) -> None:
    company = load_verified_subset()[0]

    first = execute_watchlist_mode(
        _runtime_config(tmp_path / "first"),
        watchlist_records=[company],
        transport=EmptyHitTransport(),
        current_time=100,
    )
    second = execute_watchlist_mode(
        _runtime_config(tmp_path / "second"),
        watchlist_records=[company],
        transport=EmptyHitTransport(),
        current_time=100,
    )

    first_artifact = json.loads(pathlib.Path(first["artifact_paths"][0]).read_text(encoding="utf-8"))
    second_artifact = json.loads(pathlib.Path(second["artifact_paths"][0]).read_text(encoding="utf-8"))

    assert first_artifact == second_artifact
    assert first_artifact["published_emails"] == []
    assert first_artifact["recent_signals"] == []


def test_discovery_mode_remains_intact(monkeypatch) -> None:
    sentinel = {"ok": True, "mode": "discovery", "csv_path": "C:/tmp/daily_signals.csv"}
    monkeypatch.setattr(watchlist_mode_service, "execute_signal_pipeline", lambda *args, **kwargs: dict(sentinel))

    result = execute_platform_mode(mode="discovery", runtime_config=_runtime_config(pathlib.Path("C:/tmp")))

    assert result == sentinel
