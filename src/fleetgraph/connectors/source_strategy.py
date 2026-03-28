from __future__ import annotations

import json
import xml.etree.ElementTree as element_tree
from html.parser import HTMLParser
from urllib import error, parse, request


_PROVIDER_ORDER = (
    "duckduckgo_api",
    "duckduckgo_html",
    "rss_news",
)
_SUPPORTED_PROVIDERS = set(_PROVIDER_ORDER)


class WebSearchConnectorError(RuntimeError):
    pass


class _DuckDuckGoHtmlParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self._results: list[dict[str, str]] = []
        self._current_result: dict[str, str] | None = None
        self._capture_title = False
        self._capture_snippet = False

    @staticmethod
    def _has_class(attrs: dict[str, str], class_name: str) -> bool:
        classes = attrs.get("class", "")
        return class_name in classes.split()

    @staticmethod
    def _normalize_duckduckgo_html_url(url: str) -> str:
        stripped_url = url.strip()
        if stripped_url.startswith("/l/?"):
            parsed_url = parse.urlparse(stripped_url)
            query_params = parse.parse_qs(parsed_url.query)
            encoded_target = query_params.get("uddg", [""])[0]
            if encoded_target != "":
                return parse.unquote(encoded_target)
        return stripped_url

    def _flush_current_result(self) -> None:
        if self._current_result is None:
            return
        title = " ".join(self._current_result["title"].split())
        snippet = " ".join(self._current_result["snippet"].split())
        url = self._normalize_duckduckgo_html_url(self._current_result["url"])
        if title != "" and snippet != "" and url != "":
            self._results.append(
                {
                    "title": title,
                    "snippet": snippet,
                    "url": url,
                    "source_provider": "duckduckgo_html",
                }
            )
        self._current_result = None

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        attr_map = {key: value or "" for key, value in attrs}
        if tag == "a" and self._has_class(attr_map, "result__a"):
            self._flush_current_result()
            self._current_result = {
                "title": "",
                "snippet": "",
                "url": attr_map.get("href", "").strip(),
            }
            self._capture_title = True
            self._capture_snippet = False
            return
        if self._current_result is None:
            return
        if tag == "a" and self._has_class(attr_map, "result__snippet"):
            self._capture_snippet = True
            if self._current_result["url"] == "":
                self._current_result["url"] = attr_map.get("href", "").strip()
            return
        if tag == "div" and self._has_class(attr_map, "result__snippet"):
            self._capture_snippet = True

    def handle_endtag(self, tag: str) -> None:
        if tag == "a":
            self._capture_title = False
            self._capture_snippet = False
        if tag == "div":
            self._capture_snippet = False

    def handle_data(self, data: str) -> None:
        if self._current_result is None:
            return
        if self._capture_title:
            self._current_result["title"] += data
        elif self._capture_snippet:
            self._current_result["snippet"] += data

    def get_results(self) -> list[dict[str, str]]:
        self._flush_current_result()
        return list(self._results)


def _collapse_whitespace(value: str) -> str:
    return " ".join(value.split())


def _coerce_payload_text(payload: object) -> str:
    if isinstance(payload, bytes):
        return payload.decode("utf-8")
    if isinstance(payload, str):
        return payload
    raise WebSearchConnectorError("unsupported_live_payload")


def _normalize_result_item(*, title: str, snippet: str, url: str, source_provider: str) -> dict[str, str]:
    normalized_title = _collapse_whitespace(title)
    normalized_snippet = _collapse_whitespace(snippet)
    normalized_url = url.strip()
    if source_provider not in _SUPPORTED_PROVIDERS:
        raise WebSearchConnectorError("invalid_result_item")
    if not all(value != "" for value in (normalized_title, normalized_snippet, normalized_url)):
        raise WebSearchConnectorError("invalid_result_item")
    return {
        "title": normalized_title,
        "snippet": normalized_snippet,
        "url": normalized_url,
        "source_provider": source_provider,
    }


def _extend_duckduckgo_items(raw_results: list[dict[str, str]], payload_items: object) -> None:
    if not isinstance(payload_items, list):
        return
    for item in payload_items:
        if not isinstance(item, dict):
            continue
        if "Topics" in item:
            _extend_duckduckgo_items(raw_results, item.get("Topics"))
            continue
        text = item.get("Text") or item.get("AbstractText")
        item_url = item.get("FirstURL") or item.get("Url")
        title = item.get("Heading") or (text.split(" - ", 1)[0] if isinstance(text, str) else None)
        if isinstance(title, str) and isinstance(text, str) and isinstance(item_url, str):
            raw_results.append(
                _normalize_result_item(
                    title=title,
                    snippet=text,
                    url=item_url,
                    source_provider="duckduckgo_api",
                )
            )


def _parse_duckduckgo_api(payload: object, *, result_limit: int) -> list[dict[str, str]]:
    payload_text = _coerce_payload_text(payload)
    payload_data = json.loads(payload_text)
    if not isinstance(payload_data, dict):
        raise WebSearchConnectorError("unsupported_live_payload")
    raw_results: list[dict[str, str]] = []
    _extend_duckduckgo_items(raw_results, payload_data.get("Results", []))
    _extend_duckduckgo_items(raw_results, payload_data.get("RelatedTopics", []))
    return raw_results[:result_limit]


def _parse_duckduckgo_html(payload: object, *, result_limit: int) -> list[dict[str, str]]:
    payload_text = _coerce_payload_text(payload)
    parser = _DuckDuckGoHtmlParser()
    parser.feed(payload_text)
    return parser.get_results()[:result_limit]


def _parse_rss_news(payload: object, *, result_limit: int) -> list[dict[str, str]]:
    payload_text = _coerce_payload_text(payload)
    root = element_tree.fromstring(payload_text)
    raw_results: list[dict[str, str]] = []
    for item in root.findall(".//item"):
        title = item.findtext("title", default="")
        description = item.findtext("description", default="")
        link = item.findtext("link", default="")
        if title.strip() == "" or description.strip() == "" or link.strip() == "":
            continue
        raw_results.append(
            _normalize_result_item(
                title=title,
                snippet=description,
                url=link,
                source_provider="rss_news",
            )
        )
    return raw_results[:result_limit]


def _build_source_requests(query: str) -> tuple[dict[str, str], ...]:
    encoded_query = parse.quote(query)
    return (
        {
            "provider": "duckduckgo_api",
            "url": f"https://duckduckgo.com/?q={encoded_query}&format=json&pretty=0",
        },
        {
            "provider": "duckduckgo_html",
            "url": f"https://duckduckgo.com/html/?q={encoded_query}",
        },
        {
            "provider": "rss_news",
            "url": f"https://news.google.com/rss/search?q={encoded_query}",
        },
    )


def _fetch_live_payload(provider: str, url: str, timeout_seconds: float) -> bytes:
    request_headers = {
        "User-Agent": "fleetgraph-core/0.1",
    }
    http_request = request.Request(url, headers=request_headers, method="GET")
    try:
        with request.urlopen(http_request, timeout=timeout_seconds) as response:
            return response.read()
    except (error.URLError, TimeoutError) as exc:
        raise WebSearchConnectorError("connector_request_failed") from exc


def retrieve_results(
    query: str,
    *,
    result_limit: int,
    timeout_seconds: float,
    fetcher: object | None = None,
) -> list[dict[str, str]]:
    if not isinstance(query, str) or query.strip() == "":
        raise WebSearchConnectorError("invalid_query")
    if not isinstance(result_limit, int) or isinstance(result_limit, bool) or result_limit <= 0:
        raise WebSearchConnectorError("invalid_result_limit")
    if not isinstance(timeout_seconds, (int, float)) or timeout_seconds <= 0:
        raise WebSearchConnectorError("invalid_timeout_seconds")

    payload_fetcher = fetcher or _fetch_live_payload
    parsers = {
        "duckduckgo_api": _parse_duckduckgo_api,
        "duckduckgo_html": _parse_duckduckgo_html,
        "rss_news": _parse_rss_news,
    }

    for source_request in _build_source_requests(query):
        provider = source_request["provider"]
        try:
            payload = payload_fetcher(provider, source_request["url"], float(timeout_seconds))
            results = parsers[provider](payload, result_limit=result_limit)
        except (WebSearchConnectorError, json.JSONDecodeError, element_tree.ParseError, UnicodeDecodeError):
            continue
        if len(results) > 0:
            return results
    raise WebSearchConnectorError("no_results_returned")
