from __future__ import annotations

import json
from pathlib import Path


class ResultCache:
    def __init__(
        self,
        cache_path: str | Path,
        *,
        ttl_seconds: int = 86400,
        current_time: int = 0,
    ) -> None:
        self._cache_path = Path(cache_path)
        if not isinstance(ttl_seconds, int) or isinstance(ttl_seconds, bool) or ttl_seconds <= 0:
            raise ValueError("invalid_ttl_seconds")
        if not isinstance(current_time, int) or isinstance(current_time, bool) or current_time < 0:
            raise ValueError("invalid_current_time")
        self._ttl_seconds = ttl_seconds
        self._current_time = current_time

    @staticmethod
    def normalize_query_key(query: str) -> str:
        if not isinstance(query, str) or query.strip() == "":
            raise ValueError("invalid_query")
        return " ".join(query.lower().split())

    def _load_cache(self) -> dict[str, object]:
        if not self._cache_path.exists():
            return {}
        try:
            payload = json.loads(self._cache_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return {}
        if not isinstance(payload, dict):
            return {}
        return payload

    def _write_cache(self, payload: dict[str, object]) -> None:
        self._cache_path.parent.mkdir(parents=True, exist_ok=True)
        normalized_payload = {
            key: payload[key]
            for key in sorted(payload.keys())
        }
        self._cache_path.write_text(
            json.dumps(normalized_payload, sort_keys=True, separators=(",", ":")),
            encoding="utf-8",
        )

    def get(self, query: str) -> list[dict[str, str]] | None:
        query_key = self.normalize_query_key(query)
        payload = self._load_cache()
        entry = payload.get(query_key)
        if not isinstance(entry, dict):
            return None
        cached_at = entry.get("cached_at")
        results = entry.get("results")
        if not isinstance(cached_at, int) or isinstance(cached_at, bool):
            return None
        if self._current_time - cached_at >= self._ttl_seconds:
            return None
        if not isinstance(results, list):
            return None
        normalized_results: list[dict[str, str]] = []
        for result in results:
            if not isinstance(result, dict):
                return None
            title = result.get("title")
            snippet = result.get("snippet")
            url = result.get("url")
            if not all(isinstance(value, str) and value.strip() != "" for value in (title, snippet, url)):
                return None
            normalized_results.append(
                {
                    "title": title,
                    "snippet": snippet,
                    "url": url,
                }
            )
        return normalized_results

    def set(self, query: str, results: list[dict[str, str]]) -> None:
        query_key = self.normalize_query_key(query)
        payload = self._load_cache()
        payload[query_key] = {
            "cached_at": self._current_time,
            "results": [
                {
                    "title": result["title"],
                    "snippet": result["snippet"],
                    "url": result["url"],
                }
                for result in results
            ],
        }
        self._write_cache(payload)
