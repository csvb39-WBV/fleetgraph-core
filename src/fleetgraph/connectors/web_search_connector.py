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
        normalized_results = normalized_results[:result_limit]
        if len(normalized_results) == 0:
            raise WebSearchConnectorError("no_results_returned")
        return normalized_results

    @classmethod
    def _extract_live_results(cls, payload: object) -> list[dict[str, str]]:
        if not isinstance(payload, dict):
            raise WebSearchConnectorError("unsupported_live_payload")

        raw_results: list[dict[str, str]] = []
        for item in payload.get("Results", []):
            if not isinstance(item, dict):
                continue
            text = item.get("Text") or item.get("AbstractText")
            item_url = item.get("FirstURL") or item.get("Url")
            title = item.get("Heading") or (text.split(" - ", 1)[0] if isinstance(text, str) else None)
            if isinstance(title, str) and isinstance(text, str) and isinstance(item_url, str):
                raw_results.append(
                    {
                        "title": title,
                        "snippet": text,
                        "url": item_url,
                    }
                )

        for item in payload.get("RelatedTopics", []):
            if not isinstance(item, dict):
                continue
            if "Topics" in item and isinstance(item["Topics"], list):
                for nested_item in item["Topics"]:
                    if not isinstance(nested_item, dict):
                        continue
                    text = nested_item.get("Text")
                    item_url = nested_item.get("FirstURL")
                    if isinstance(text, str) and isinstance(item_url, str):
                        raw_results.append(
                            {
                                "title": text.split(" - ", 1)[0],
                                "snippet": text,
                                "url": item_url,
                            }
                        )
                continue
            text = item.get("Text") or item.get("AbstractText")
            item_url = item.get("FirstURL") or item.get("Url")
            if isinstance(text, str) and isinstance(item_url, str):
                raw_results.append(
                    {
                        "title": text.split(" - ", 1)[0],
                        "snippet": text,
                        "url": item_url,
                    }
                )

        if len(raw_results) == 0:
            raise WebSearchConnectorError("no_results_returned")
        return raw_results

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
        raw_results = self._extract_live_results(payload)
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
                if str(exc) == "no_results_returned":
                    raise
                last_error = exc
        raise WebSearchConnectorError("connector_request_failed") from last_error
