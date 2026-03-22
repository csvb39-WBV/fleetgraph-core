from __future__ import annotations

import pathlib
import sys

import pytest


PROJECT_ROOT = pathlib.Path(__file__).resolve().parents[2]
SRC_ROOT = PROJECT_ROOT / "src"

if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))


from fleetgraph_core.intelligence.signal_source_registry import (
    get_signal_source_registry,
    list_signal_sources,
    resolve_signal_source,
)


def test_registry_covers_all_tiers_0_through_3() -> None:
    registry = get_signal_source_registry()
    discovered_tiers = {record["signal_tier"] for record in registry.values()}

    assert discovered_tiers == {0, 1, 2, 3}


def test_list_signal_sources_is_deterministic_and_sorted() -> None:
    first = list_signal_sources()
    second = list_signal_sources()

    assert first == second
    assert first == tuple(sorted(first))


def test_resolve_signal_source_returns_expected_mapping() -> None:
    result = resolve_signal_source(
        source_name=" sec_filings ",
        source_type=" filing ",
    )

    assert result == {
        "source_name": "sec_filings",
        "signal_tier": 0,
        "signal_category": "COMPLIANCE",
        "source_class": "filing",
        "valid": True,
    }


def test_resolve_signal_source_is_deterministic_for_equivalent_inputs() -> None:
    first = resolve_signal_source(
        source_name="NEWS_RSS_FEEDS",
        source_type="MEDIA",
    )
    second = resolve_signal_source(
        source_name=" news_rss_feeds ",
        source_type=" media ",
    )

    assert first == second


@pytest.mark.parametrize("source_name", ["", "   ", None, 1])
def test_resolve_signal_source_rejects_invalid_source_name(source_name: object) -> None:
    with pytest.raises(ValueError, match="source_name must be a non-empty string"):
        resolve_signal_source(source_name=source_name, source_type="filing")


@pytest.mark.parametrize("source_type", ["", "   ", None, 1])
def test_resolve_signal_source_rejects_invalid_source_type(source_type: object) -> None:
    with pytest.raises(ValueError, match="source_type must be a non-empty string"):
        resolve_signal_source(source_name="sec_filings", source_type=source_type)


def test_resolve_signal_source_rejects_unknown_source_name() -> None:
    with pytest.raises(ValueError, match="unknown source_name: unknown_source"):
        resolve_signal_source(source_name="unknown_source", source_type="web")


def test_resolve_signal_source_rejects_mismatched_source_type() -> None:
    with pytest.raises(
        ValueError,
        match=(
            "source_type does not match registry classification "
            "for sec_filings: expected filing"
        ),
    ):
        resolve_signal_source(source_name="sec_filings", source_type="web")


def test_every_defined_source_is_resolvable_with_expected_source_type() -> None:
    registry = get_signal_source_registry()

    for source_name, record in registry.items():
        result = resolve_signal_source(
            source_name=source_name,
            source_type=record["source_class"],
        )
        assert result["source_name"] == source_name
        assert result["signal_tier"] == record["signal_tier"]
        assert result["signal_category"] == record["signal_category"]
        assert result["source_class"] == record["source_class"]
        assert result["valid"] is True