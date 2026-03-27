from __future__ import annotations

import json
from urllib import error, parse, request


class WebSearchConnectorError(RuntimeError):
    pass


class WebSearchConnector:
    def __init__(
        self,
        *,
        timeout_seconds: float = 5.0,
        max_retries: int = 2,
        min_interval_seconds: float = 0.0,
        transport: object | None = None,
    ) -> None:
        if not isinstance(timeout_seconds, (int, float)) or timeout_seconds <= 0:
            raise ValueError("invalid_timeout_seconds")
        if not isinstance(max_retries, int) or isinstance(max_retries, bool) or max_retries < 0:
            raise ValueError("invalid_max_retries")
        if not isinstance(min_interval_seconds, (int, float)) or min_interval_seconds < 0:
            raise ValueError("invalid_min_interval_seconds")
        self._timeout_seconds = float(timeout_seconds)
        self._max_retries = max_retries
        self._min_interval_seconds = float(min_interval_seconds)
        self._transport = transport

    @staticmethod
    def _normalize_result_item(result_item: object) -> dict[str, str]:
        if not isinstance(result_item, dict):
            raise WebSearchConnectorError("invalid_result_item")
        title = result_item.get("title")
        snippet = result_item.get("snippet")
        url = result_item.get("url")
        if not all(isinstance(value, str) and value.strip() != "" for value in (title, snippet, url)):
            raise WebSearchConnectorError("invalid_result_item")
        return {
            "title": " ".join(title.split()),
            "snippet": " ".join(snippet.split()),
            "url": url.strip(),
        }

    @classmethod
    def normalize_results(cls, raw_results: object, *, result_limit: int) -> list[dict[str, str]]:
        if not isinstance(result_limit, int) or isinstance(result_limit, bool) or result_limit <= 0:
            raise WebSearchConnectorError("invalid_result_limit")
        if not isinstance(raw_results, list):
            raise WebSearchConnectorError("invalid_results_payload")
        normalized_results = [cls._normalize_result_item(result_item) for result_item in raw_results]
        return normalized_results[:result_limit]

    def _default_transport(self, query: str, result_limit: int) -> list[dict[str, str]]:
        endpoint = "https://duckduckgo.com/?q={query}&format=json&pretty=0".format(
            query=parse.quote(query)
        )
        http_request = request.Request(
            endpoint,
            headers={"User-Agent": "fleetgraph-core/0.1"},
            method="GET",
        )
        try:
            with request.urlopen(http_request, timeout=self._timeout_seconds) as response:
                payload = json.loads(response.read().decode("utf-8"))
        except (error.URLError, TimeoutError, json.JSONDecodeError) as exc:
            raise WebSearchConnectorError("connector_request_failed") from exc
        related_topics = payload.get("RelatedTopics", [])
        raw_results: list[dict[str, str]] = []
        for item in related_topics:
            if not isinstance(item, dict):
                continue
            text = item.get("Text")
            first_url = item.get("FirstURL")
            if not isinstance(text, str) or not isinstance(first_url, str):
                continue
            raw_results.append(
                {
                    "title": text.split(" - ", 1)[0],
                    "snippet": text,
                    "url": first_url,
                }
            )
        return self.normalize_results(raw_results, result_limit=result_limit)

    def search(self, query: str, *, result_limit: int) -> list[dict[str, str]]:
        if not isinstance(query, str) or query.strip() == "":
            raise WebSearchConnectorError("invalid_query")
        if not isinstance(result_limit, int) or isinstance(result_limit, bool) or result_limit <= 0:
            raise WebSearchConnectorError("invalid_result_limit")

        attempts = 0
        last_error: Exception | None = None
        while attempts <= self._max_retries:
            attempts += 1
            try:
                if self._transport is not None:
                    raw_results = self._transport(query, result_limit, self._timeout_seconds)
                    return self.normalize_results(raw_results, result_limit=result_limit)
                return self._default_transport(query, result_limit)
            except WebSearchConnectorError as exc:
                last_error = exc
        raise WebSearchConnectorError("connector_request_failed") from last_error
