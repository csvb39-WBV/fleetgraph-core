from __future__ import annotations

from fleetgraph.connectors.source_strategy import WebSearchConnectorError, retrieve_results


class WebSearchConnector:
    def __init__(
        self,
        *,
        timeout_seconds: float = 5.0,
        max_retries: int = 2,
        min_interval_seconds: float = 0.0,
        transport: object | None = None,
        source_fetcher: object | None = None,
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
        self._source_fetcher = source_fetcher

    @staticmethod
    def _normalize_result_item(result_item: object) -> dict[str, str]:
        if not isinstance(result_item, dict):
            raise WebSearchConnectorError("invalid_result_item")
        title = result_item.get("title")
        snippet = result_item.get("snippet")
        url = result_item.get("url")
        source_provider = result_item.get("source_provider")
        if not all(isinstance(value, str) and value.strip() != "" for value in (title, snippet, url, source_provider)):
            raise WebSearchConnectorError("invalid_result_item")
        return {
            "title": " ".join(title.split()),
            "snippet": " ".join(snippet.split()),
            "url": url.strip(),
            "source_provider": source_provider.strip(),
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

    def _default_transport(self, query: str, result_limit: int) -> list[dict[str, str]]:
        raw_results = retrieve_results(
            query,
            result_limit=result_limit,
            timeout_seconds=self._timeout_seconds,
            fetcher=self._source_fetcher,
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
                if str(exc) == "no_results_returned":
                    raise
                last_error = exc
        raise WebSearchConnectorError("connector_request_failed") from last_error
