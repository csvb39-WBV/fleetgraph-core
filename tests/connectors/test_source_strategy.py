from __future__ import annotations

import pytest

from fleetgraph.connectors.source_strategy import (
    WebSearchConnectorError,
    filter_results,
    has_required_event_terms,
    is_educational_result,
    retrieve_results,
    retrieve_results_with_metadata,
)


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

    metadata = retrieve_results_with_metadata(
        "lawsuit filed against contractor company major project",
        result_limit=3,
        timeout_seconds=5.0,
        fetcher=fetcher,
    )

    assert metadata == {
        "ok": True,
        "results": [
            {
                "title": "Atlas Build Group lawsuit filed",
                "snippet": "Atlas Build Group faces contract dispute.",
                "url": "https://example.com/atlas",
                "source_provider": "duckduckgo_html",
            }
        ],
        "source_provider": "duckduckgo_html",
        "suppressed_count": 0,
        "error_code": None,
    }


def test_multi_source_rss_fallback_succeeds_when_others_fail() -> None:
    def fetcher(provider: str, url: str, timeout_seconds: float) -> str:
        if provider == "rss_news":
            return """
            <rss><channel>
                <item>
                    <title>Beacon Masonry Services audit investigation announced</title>
                    <description>Beacon Masonry Services audit investigation announced for a public project.</description>
                    <link>https://example.com/beacon</link>
                </item>
            </channel></rss>
            """
        return ""

    results = retrieve_results(
        "audit investigation company project firm holdings",
        result_limit=3,
        timeout_seconds=5.0,
        fetcher=fetcher,
    )

    assert results == [
        {
            "title": "Beacon Masonry Services audit investigation announced",
            "snippet": "Beacon Masonry Services audit investigation announced for a public project.",
            "url": "https://example.com/beacon",
            "source_provider": "rss_news",
        }
    ]


def test_all_sources_fail_raise_no_results_returned() -> None:
    with pytest.raises(WebSearchConnectorError, match="no_results_returned"):
        retrieve_results(
            "lawsuit filed against contractor company major project",
            result_limit=3,
            timeout_seconds=5.0,
            fetcher=lambda provider, url, timeout_seconds: "",
        )


def test_source_strategy_deterministic_selection() -> None:
    def fetcher(provider: str, url: str, timeout_seconds: float) -> str:
        if provider == "duckduckgo_api":
            return '{"Results": [{"Heading": "Atlas Build Group lawsuit", "AbstractText": "Atlas Build Group lawsuit filed.", "FirstURL": "https://example.com/atlas"}], "RelatedTopics": []}'
        return ""

    first = retrieve_results(
        "lawsuit filed against contractor company major project",
        result_limit=3,
        timeout_seconds=5.0,
        fetcher=fetcher,
    )
    second = retrieve_results(
        "lawsuit filed against contractor company major project",
        result_limit=3,
        timeout_seconds=5.0,
        fetcher=fetcher,
    )

    assert first == second


def test_educational_results_fully_removed() -> None:
    result_item = {
        "title": "Construction litigation guide explained",
        "snippet": "Lawsuit filed against Atlas Build Group.",
        "url": "https://example.com/guide",
        "source_provider": "duckduckgo_api",
    }

    assert is_educational_result(result_item) is True


def test_hard_block_educational_results_even_with_event_terms() -> None:
    results, suppressed_count = filter_results(
        [
            {
                "title": "FAQ: Atlas Build Group lawsuit filed after project delay",
                "snippet": "Complaint filed against Atlas Build Group.",
                "url": "https://example.com/faq",
                "source_provider": "duckduckgo_api",
            }
        ]
    )

    assert results == []
    assert suppressed_count == 1


def test_event_validation_works() -> None:
    assert has_required_event_terms(
        {
            "title": "Atlas Build Group lawsuit filed after project delay",
            "snippet": "Federal complaint filed against Atlas Build Group.",
            "url": "https://example.com/event",
            "source_provider": "duckduckgo_api",
        }
    ) is True
    assert has_required_event_terms(
        {
            "title": "Atlas Build Group quarterly revenue update",
            "snippet": "Financial results posted.",
            "url": "https://example.com/revenue",
            "source_provider": "duckduckgo_api",
        }
    ) is False


def test_mixed_batches_return_only_event_relevant_items() -> None:
    results, suppressed_count = filter_results(
        [
            {
                "title": "Construction litigation guide explained",
                "snippet": "Complete guide for contractors.",
                "url": "https://example.com/guide",
                "source_provider": "duckduckgo_api",
            },
            {
                "title": "Atlas Build Group quarterly revenue update",
                "snippet": "Financial results posted.",
                "url": "https://example.com/revenue",
                "source_provider": "duckduckgo_api",
            },
            {
                "title": "Atlas Build Group lawsuit filed after project delay",
                "snippet": "Federal complaint filed against Atlas Build Group.",
                "url": "https://example.com/event",
                "source_provider": "duckduckgo_api",
            },
        ]
    )

    assert suppressed_count == 1
    assert results == [
        {
            "title": "Atlas Build Group lawsuit filed after project delay",
            "snippet": "Federal complaint filed against Atlas Build Group.",
            "url": "https://example.com/event",
            "source_provider": "duckduckgo_api",
        }
    ]
