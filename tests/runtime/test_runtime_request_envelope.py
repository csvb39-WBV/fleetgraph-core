from __future__ import annotations

from copy import deepcopy

import pytest

from fleetgraph_core.runtime.runtime_request_envelope import (
    build_runtime_request_envelope,
)


def _valid_input(operation_type: str = "ingest") -> dict[str, object]:
    return {
        "request_id": "req-001",
        "client_id": "client-001",
        "api_key": "secret-key",
        "operation_type": operation_type,
        "payload": {
            "z_field": 1,
            "a_field": {"nested_second": 2, "nested_first": 1},
        },
        "runtime_limits": {
            "max_retries": 3,
            "limits_nested": {"burst": 10, "steady": 5},
        },
        "billing_enabled": True,
    }


def test_build_runtime_request_envelope_valid_ingest() -> None:
    envelope = build_runtime_request_envelope(_valid_input("ingest"))

    assert envelope["operation_type"] == "ingest"


def test_build_runtime_request_envelope_valid_retrieve() -> None:
    envelope = build_runtime_request_envelope(_valid_input("retrieve"))

    assert envelope["operation_type"] == "retrieve"


def test_build_runtime_request_envelope_valid_reprocess() -> None:
    envelope = build_runtime_request_envelope(_valid_input("reprocess"))

    assert envelope["operation_type"] == "reprocess"


def test_build_runtime_request_envelope_valid_status() -> None:
    envelope = build_runtime_request_envelope(_valid_input("status"))

    assert envelope["operation_type"] == "status"


def test_build_runtime_request_envelope_enforces_top_level_key_order() -> None:
    envelope = build_runtime_request_envelope(_valid_input())

    assert list(envelope.keys()) == [
        "request_id",
        "client_id",
        "api_key",
        "operation_type",
        "payload",
        "runtime_limits",
        "billing_enabled",
    ]


def test_build_runtime_request_envelope_is_deterministic() -> None:
    request = _valid_input("retrieve")

    first = build_runtime_request_envelope(request)
    second = build_runtime_request_envelope(request)

    assert first == second


def test_build_runtime_request_envelope_does_not_mutate_input() -> None:
    request = _valid_input()
    request_before = deepcopy(request)

    _ = build_runtime_request_envelope(request)

    assert request == request_before


def test_build_runtime_request_envelope_returns_new_object() -> None:
    request = _valid_input()

    result = build_runtime_request_envelope(request)

    assert result is not request
    assert result["payload"] is not request["payload"]
    assert result["runtime_limits"] is not request["runtime_limits"]


def test_build_runtime_request_envelope_preserves_nested_dict_ordering() -> None:
    request = _valid_input()

    result = build_runtime_request_envelope(request)

    assert list(result["payload"].keys()) == ["z_field", "a_field"]
    assert list(result["payload"]["a_field"].keys()) == [
        "nested_second",
        "nested_first",
    ]
    assert list(result["runtime_limits"].keys()) == [
        "max_retries",
        "limits_nested",
    ]
    assert list(result["runtime_limits"]["limits_nested"].keys()) == [
        "burst",
        "steady",
    ]


def test_build_runtime_request_envelope_rejects_missing_keys() -> None:
    request = _valid_input()
    request.pop("client_id")

    with pytest.raises(ValueError, match="Missing required keys: client_id"):
        build_runtime_request_envelope(request)


def test_build_runtime_request_envelope_rejects_extra_keys() -> None:
    request = _valid_input()
    request["unexpected"] = "extra"

    with pytest.raises(ValueError, match="Unexpected keys: unexpected"):
        build_runtime_request_envelope(request)


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("request_id", 123, "request_id must be a string"),
        ("request_id", "", "request_id must be a non-empty string"),
        ("client_id", 123, "client_id must be a string"),
        ("client_id", "", "client_id must be a non-empty string"),
        ("api_key", 123, "api_key must be a string"),
        (
            "operation_type",
            "delete",
            "operation_type must be one of: ingest, retrieve, reprocess, status",
        ),
        ("payload", [], "payload must be a dict"),
        ("runtime_limits", [], "runtime_limits must be a dict"),
        ("billing_enabled", "true", "billing_enabled must be a bool"),
    ],
)
def test_build_runtime_request_envelope_rejects_malformed_values(
    field: str,
    value: object,
    message: str,
) -> None:
    request = _valid_input()
    request[field] = value

    with pytest.raises(ValueError, match=message):
        build_runtime_request_envelope(request)


def test_build_runtime_request_envelope_allows_explicit_empty_api_key() -> None:
    request = _valid_input()
    request["api_key"] = ""

    envelope = build_runtime_request_envelope(request)

    assert envelope["api_key"] == ""