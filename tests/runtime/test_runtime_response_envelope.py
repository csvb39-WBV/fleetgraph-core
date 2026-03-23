from __future__ import annotations

from copy import deepcopy

import pytest

from fleetgraph_core.runtime.runtime_response_envelope import (
    build_runtime_response_envelope,
)


def _valid_input(
    operation_type: str = "ingest",
    status: str = "completed",
) -> dict[str, object]:
    return {
        "request_id": "req-001",
        "client_id": "client-001",
        "operation_type": operation_type,
        "status": status,
        "result": {
            "z_field": 1,
            "a_field": {"nested_second": 2, "nested_first": 1},
        },
        "errors": [
            {"code": "none", "message": "ok"},
        ],
        "billing_enabled": True,
    }


@pytest.mark.parametrize("operation_type", ["ingest", "retrieve", "reprocess", "status"])
def test_build_runtime_response_envelope_accepts_all_operation_types(
    operation_type: str,
) -> None:
    envelope = build_runtime_response_envelope(_valid_input(operation_type=operation_type))

    assert envelope["operation_type"] == operation_type


@pytest.mark.parametrize("status", ["accepted", "completed", "failed"])
def test_build_runtime_response_envelope_accepts_all_statuses(status: str) -> None:
    envelope = build_runtime_response_envelope(_valid_input(status=status))

    assert envelope["status"] == status


def test_build_runtime_response_envelope_enforces_top_level_key_order() -> None:
    envelope = build_runtime_response_envelope(_valid_input())

    assert list(envelope.keys()) == [
        "request_id",
        "client_id",
        "operation_type",
        "status",
        "result",
        "errors",
        "billing_enabled",
    ]


def test_build_runtime_response_envelope_is_deterministic() -> None:
    request = _valid_input("retrieve", "accepted")

    first = build_runtime_response_envelope(request)
    second = build_runtime_response_envelope(request)

    assert first == second


def test_build_runtime_response_envelope_does_not_mutate_input() -> None:
    request = _valid_input()
    request_before = deepcopy(request)

    _ = build_runtime_response_envelope(request)

    assert request == request_before


def test_build_runtime_response_envelope_returns_new_object() -> None:
    request = _valid_input()

    result = build_runtime_response_envelope(request)

    assert result is not request
    assert result["result"] is not request["result"]
    assert result["errors"] is not request["errors"]


def test_build_runtime_response_envelope_preserves_nested_dict_ordering() -> None:
    request = _valid_input()

    result = build_runtime_response_envelope(request)

    assert list(result["result"].keys()) == ["z_field", "a_field"]
    assert list(result["result"]["a_field"].keys()) == [
        "nested_second",
        "nested_first",
    ]


def test_build_runtime_response_envelope_rejects_missing_keys() -> None:
    request = _valid_input()
    request.pop("client_id")

    with pytest.raises(ValueError, match="Missing required keys: client_id"):
        build_runtime_response_envelope(request)


def test_build_runtime_response_envelope_rejects_extra_keys() -> None:
    request = _valid_input()
    request["unexpected"] = "extra"

    with pytest.raises(ValueError, match="Unexpected keys: unexpected"):
        build_runtime_response_envelope(request)


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("request_id", 123, "request_id must be a string"),
        ("request_id", "", "request_id must be a non-empty string"),
        ("client_id", 123, "client_id must be a string"),
        ("client_id", "", "client_id must be a non-empty string"),
        (
            "operation_type",
            "delete",
            "operation_type must be one of: ingest, retrieve, reprocess, status",
        ),
        (
            "status",
            "running",
            "status must be one of: accepted, completed, failed",
        ),
        ("result", [], "result must be a dict"),
        ("errors", {}, "errors must be a list"),
        ("billing_enabled", "true", "billing_enabled must be a bool"),
    ],
)
def test_build_runtime_response_envelope_rejects_malformed_values(
    field: str,
    value: object,
    message: str,
) -> None:
    request = _valid_input()
    request[field] = value

    with pytest.raises(ValueError, match=message):
        build_runtime_response_envelope(request)
