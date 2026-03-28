from __future__ import annotations

import pytest

from fleetgraph.connectors.source_strategy import WebSearchConnectorError, retrieve_results


def test_multi_source_primary_fails_html_fallback_succeeds() -> None:
    def fetcher(provider: str, url: str, timeout_seconds: float) -> str:
        if provider == "duckduckgo_api":
            return '{"Results": [], "RelatedTopics": []}'
        if provider == "duckduckgo_html":
            return """
            <html><body>
            <a class="result__a" href="/l/?uddg=https%3A%2F%2Fexample.com%2Fatlas">Atlas Build Group lawsuit filed</a>
            <div class="result__snippet">Atlas Build Group faces contract dispute.</div>
            </body></html>
            """
        return "<rss><channel></channel></rss>"

    results = retrieve_results(
        "construction lawsuit contractor",
        result_limit=3,
        timeout_seconds=5.0,
        fetcher=fetcher,
    )

    assert results == [
        {
            "title": "Atlas Build Group lawsuit filed",
            "snippet": "Atlas Build Group faces contract dispute.",
            "url": "https://example.com/atlas",
            "source_provider": "duckduckgo_html",
        }
    ]


def test_multi_source_rss_fallback_succeeds_when_others_fail() -> None:
    def fetcher(provider: str, url: str, timeout_seconds: float) -> str:
        if provider == "rss_news":
            return """
            <rss><channel>
                <item>
                    <title>Beacon Masonry Services audit review</title>
                    <description>Beacon Masonry Services audit review published.</description>
                    <link>https://example.com/beacon</link>
                </item>
            </channel></rss>
            """
        return ""

    results = retrieve_results(
        "audit construction company",
        result_limit=3,
        timeout_seconds=5.0,
        fetcher=fetcher,
    )

    assert results == [
        {
            "title": "Beacon Masonry Services audit review",
            "snippet": "Beacon Masonry Services audit review published.",
            "url": "https://example.com/beacon",
            "source_provider": "rss_news",
        }
    ]


def test_all_sources_fail_raise_no_results_returned() -> None:
    with pytest.raises(WebSearchConnectorError, match="no_results_returned"):
        retrieve_results(
            "construction lawsuit contractor",
            result_limit=3,
            timeout_seconds=5.0,
            fetcher=lambda provider, url, timeout_seconds: "",
        )


def test_source_strategy_deterministic_selection() -> None:
    def fetcher(provider: str, url: str, timeout_seconds: float) -> str:
        if provider == "duckduckgo_api":
            return '{"Results": [{"Heading": "Atlas Build Group suit", "AbstractText": "Atlas Build Group suit posted.", "FirstURL": "https://example.com/atlas"}], "RelatedTopics": []}'
        return ""

    first = retrieve_results(
        "construction lawsuit contractor",
        result_limit=3,
        timeout_seconds=5.0,
        fetcher=fetcher,
    )
    second = retrieve_results(
        "construction lawsuit contractor",
        result_limit=3,
        timeout_seconds=5.0,
        fetcher=fetcher,
    )

    assert first == second
