from __future__ import annotations

import sys
from pathlib import Path

import pytest


REPO_ROOT = Path(__file__).resolve().parents[2]
SRC_ROOT = REPO_ROOT / "src"

if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))


from fleetgraph_core.runtime.runtime_operation_router import route_runtime_operation


@pytest.mark.parametrize(
    "operation_type",
    ["ingest", "retrieve", "reprocess", "status"],
)
def test_route_runtime_operation_supports_all_valid_routes(operation_type: str) -> None:
    payload = {"operation_type": operation_type}

    result = route_runtime_operation(payload)

    assert result == {
        "route": operation_type,
        "reasons": [f"{operation_type}_route_selected"],
    }


def test_route_runtime_operation_enforces_exact_output_key_order() -> None:
    payload = {"operation_type": "ingest"}

    result = route_runtime_operation(payload)

    assert list(result.keys()) == ["route", "reasons"]


def test_route_runtime_operation_is_deterministic_repeated_calls() -> None:
    payload = {"operation_type": "retrieve"}

    first = route_runtime_operation(payload)
    second = route_runtime_operation(payload)

    assert first == second
    assert list(first.keys()) == list(second.keys())


def test_route_runtime_operation_does_not_mutate_input() -> None:
    payload = {"operation_type": "reprocess"}
    original = dict(payload)

    route_runtime_operation(payload)

    assert payload == original


@pytest.mark.parametrize(
    "payload",
    [
        {},
        {"operation_type": "ingest", "extra": True},
    ],
)
def test_route_runtime_operation_rejects_missing_or_extra_keys(
    payload: dict[str, object],
) -> None:
    with pytest.raises(ValueError, match="payload must include exactly"):
        route_runtime_operation(payload)


def test_route_runtime_operation_rejects_non_string_operation_type() -> None:
    payload = {"operation_type": 123}

    with pytest.raises(ValueError, match="operation_type must be a string"):
        route_runtime_operation(payload)


def test_route_runtime_operation_rejects_invalid_operation_type_value() -> None:
    payload = {"operation_type": "sync"}

    with pytest.raises(ValueError, match="operation_type must be one of"):
        route_runtime_operation(payload)
