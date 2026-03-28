from __future__ import annotations

import pytest

from fleetgraph.connectors.web_search_connector import WebSearchConnector, WebSearchConnectorError


def test_connector_normalization_behavior() -> None:
    connector = WebSearchConnector(
        transport=lambda query, result_limit, timeout_seconds: [
            {
                "title": "  Acme Construction LLC sued  ",
                "snippet": " 2026-03-26 filing posted ",
                "url": " https://example.com/acme ",
            },
            {
                "title": "Beacon Builders audit",
                "snippet": "Audit released",
                "url": "https://example.com/beacon",
            },
        ]
    )

    assert connector.search("acme lawsuit", result_limit=1) == [
        {
            "title": "Acme Construction LLC sued",
            "snippet": "2026-03-26 filing posted",
            "url": "https://example.com/acme",
        }
    ]


def test_connector_timeout_retry_failure_behavior() -> None:
    attempts = {"count": 0}

    def failing_transport(query: str, result_limit: int, timeout_seconds: float) -> list[dict[str, str]]:
        attempts["count"] += 1
        raise WebSearchConnectorError("connector_request_failed")

    connector = WebSearchConnector(transport=failing_transport, max_retries=2)

    with pytest.raises(WebSearchConnectorError, match="connector_request_failed"):
        connector.search("acme lawsuit", result_limit=2)

    assert attempts["count"] == 3


def test_connector_empty_live_result_handling() -> None:
    connector = WebSearchConnector(transport=lambda query, result_limit, timeout_seconds: [])

    with pytest.raises(WebSearchConnectorError, match="no_results_returned"):
        connector.search("acme lawsuit", result_limit=2)
