from __future__ import annotations

from pathlib import Path

from fleetgraph.cache.result_cache import ResultCache


def _result(
    title: str = "Acme Construction sued",
    snippet: str = "2026-03-26 filing",
    url: str = "https://example.com/a",
    source_provider: str = "duckduckgo_api",
) -> dict[str, str]:
    return {
        "title": title,
        "snippet": snippet,
        "url": url,
        "source_provider": source_provider,
    }


def test_cache_hit_miss_and_expiry_behavior(tmp_path: Path) -> None:
    cache_path = tmp_path / "cache.json"
    cold_cache = ResultCache(cache_path, current_time=100)

    assert cold_cache.get("Acme lawsuit") is None

    cold_cache.set("Acme lawsuit", [_result()])
    warm_cache = ResultCache(cache_path, current_time=150)
    expired_cache = ResultCache(cache_path, current_time=86501)

    assert warm_cache.get("Acme lawsuit") == [_result()]
    assert expired_cache.get("Acme lawsuit") is None


def test_cache_query_key_normalization(tmp_path: Path) -> None:
    cache_path = tmp_path / "cache.json"
    cache = ResultCache(cache_path, current_time=100)
    cache.set("  Acme   Lawsuit ", [_result()])

    assert cache.get("acme lawsuit") == [_result()]


def test_malformed_cache_handling(tmp_path: Path) -> None:
    cache_path = tmp_path / "cache.json"
    cache_path.write_text("{not-json", encoding="utf-8")
    cache = ResultCache(cache_path, current_time=100)

    assert cache.get("acme lawsuit") is None
