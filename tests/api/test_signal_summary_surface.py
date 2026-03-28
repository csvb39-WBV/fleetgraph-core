from __future__ import annotations

import pathlib
import sys

import pytest


PROJECT_ROOT = pathlib.Path(__file__).resolve().parents[2]
SRC_ROOT = PROJECT_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))


from fleetgraph.api.signal_summary_surface import build_signal_summary


def _signal(
    company: str,
    signal_type: str,
    priority: str,
    confidence_score: int,
    source: str,
) -> dict[str, object]:
    return {
        "company": company,
        "signal_type": signal_type,
        "event_summary": f"{signal_type} event for {company}",
        "source": source,
        "date_detected": "2026-03-28",
        "confidence_score": confidence_score,
        "priority": priority,
        "raw_text": f"{signal_type} event for {company}",
        "recommended_action": "CALL NOW",
    }


def test_summary_correctness_includes_source_counts() -> None:
    result = build_signal_summary(
        [
            _signal("Atlas Build Co", "litigation", "HIGH", 5, "rss_news://court-feed"),
            _signal("Beacon Masonry", "audit", "HIGH", 4, "duckduckgo_html://search-result"),
            _signal("Civic Review LLC", "government", "MEDIUM", 3, "duckduckgo_api://instant-answer"),
            _signal("Delta Works", "project_distress", "HIGH", 4, "duckduckgo_html://search-result-2"),
            _signal("Atlas Build Co", "audit", "MEDIUM", 3, "rss_news://audit-feed"),
        ]
    )

    assert result == {
        "count_by_signal_type": {
            "audit": 2,
            "government": 1,
            "litigation": 1,
            "project_distress": 1,
        },
        "count_by_priority": {
            "HIGH": 3,
            "MEDIUM": 2,
        },
        "count_by_source": {
            "rss_news": 2,
            "duckduckgo_html": 2,
            "duckduckgo_api": 1,
        },
        "total_exported_count": 5,
        "top_companies": [
            "Atlas Build Co",
            "Beacon Masonry",
            "Civic Review LLC",
            "Delta Works",
        ],
    }


def test_top_companies_support_broader_entity_names() -> None:
    result = build_signal_summary(
        [
            _signal("Smith & Jones LLP", "litigation", "HIGH", 5, "rss_news://court-feed"),
            _signal("Beacon Holdings", "audit", "HIGH", 4, "duckduckgo_html://search-result"),
            _signal("Atlas Services Group", "government", "MEDIUM", 3, "duckduckgo_api://instant-answer"),
            _signal("Gray Counsel PLLC", "litigation", "HIGH", 5, "rss_news://legal-feed"),
        ]
    )

    assert result["top_companies"] == [
        "Atlas Services Group",
        "Beacon Holdings",
        "Gray Counsel PLLC",
        "Smith & Jones LLP",
    ]


def test_input_validation() -> None:
    with pytest.raises(ValueError):
        build_signal_summary([])
