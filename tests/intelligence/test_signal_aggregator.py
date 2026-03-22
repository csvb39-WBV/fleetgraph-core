from __future__ import annotations

import pathlib
import sys

import pytest


PROJECT_ROOT = pathlib.Path(__file__).resolve().parents[2]
SRC_ROOT = PROJECT_ROOT / "src"

if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))


from fleetgraph_core.intelligence.signal_aggregator import aggregate_signals


def test_aggregate_signals_normalizes_valid_signals() -> None:
    result = aggregate_signals(
        [
            {
                "source": " permits ",
                "data": {"b": 2, "a": 1},
            },
            {
                "source": "news",
                "data": "  raw payload  ",
            },
        ]
    )

    assert result == {
        "aggregated_signals": [
            {
                "source": "news",
                "normalized_data": {"raw": "raw payload"},
                "valid": True,
            },
            {
                "source": "permits",
                "normalized_data": {"a": 1, "b": 2},
                "valid": True,
            },
        ]
    }


def test_aggregate_signals_keeps_malformed_signals_with_error_metadata() -> None:
    result = aggregate_signals(
        [
            {"source": "permits", "data": {"permit_type": "building"}},
            {"source": "   ", "data": {"x": 1}},
            {"source": "alerts"},
            "not-a-mapping",
        ]
    )

    assert result["aggregated_signals"] == [
        {
            "source": "alerts",
            "normalized_data": {},
            "valid": False,
            "error": "data is required",
        },
        {
            "source": "permits",
            "normalized_data": {"permit_type": "building"},
            "valid": True,
        },
        {
            "source": "unknown",
            "normalized_data": {"raw": "not-a-mapping"},
            "valid": False,
            "error": "signal must be a mapping",
        },
        {
            "source": "unknown",
            "normalized_data": {"x": 1},
            "valid": False,
            "error": "source must be a non-empty string",
        },
    ]


def test_aggregate_signals_rejects_non_list_inputs() -> None:
    with pytest.raises(ValueError, match="signals must be a list"):
        aggregate_signals({"signals": []})


def test_aggregate_signals_is_deterministic() -> None:
    payload = [
        {"source": "zeta", "data": {"value": 1}},
        {"source": "alpha", "data": {"value": 2}},
        {"source": "alpha", "data": {"value": 3}},
    ]

    first = aggregate_signals(payload)
    second = aggregate_signals(payload)

    assert first == second


def test_aggregate_signals_preserves_signal_count() -> None:
    payload = [
        {"source": "permits", "data": {"id": 1}},
        {"source": " ", "data": {"id": 2}},
        {"source": "news", "data": "blob"},
        {"source": "alerts"},
    ]

    result = aggregate_signals(payload)

    assert len(result["aggregated_signals"]) == len(payload)