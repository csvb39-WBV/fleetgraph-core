from __future__ import annotations

import sys
from pathlib import Path

import pytest


REPO_ROOT = Path(__file__).resolve().parents[2]
SRC_ROOT = REPO_ROOT / "src"

if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))


from fleetgraph_core.runtime.runtime_security_orchestrator import (
    orchestrate_runtime_security,
)


def _valid_payload() -> dict[str, object]:
    return {
        "auth_input": {
            "provided_api_key": "key-1",
            "authorized_api_keys": ["key-1", "key-2"],
        },
        "validation_input": {
            "request_id": "req-001",
            "endpoint": "/runtime/execute",
            "content_type": "application/json",
            "payload_size_bytes": 64,
            "max_payload_size_bytes": 1024,
            "has_body": True,
        },
        "rate_limit_input": {
            "client_id": "client-001",
            "request_count_in_window": 1,
            "max_requests_per_window": 10,
            "window_active": True,
        },
    }


def test_orchestrate_runtime_security_stops_on_auth_failure() -> None:
    payload = _valid_payload()
    payload["auth_input"] = {
        "provided_api_key": "bad-key",
        "authorized_api_keys": ["key-1", "key-2"],
    }

    result = orchestrate_runtime_security(payload)

    assert result == {
        "status": "stop",
        "stage": "auth",
        "reasons": ["api_key_not_authorized"],
    }


def test_orchestrate_runtime_security_stops_on_validation_failure() -> None:
    payload = _valid_payload()
    payload["validation_input"] = {
        "request_id": "",
        "endpoint": "/runtime/execute",
        "content_type": "application/json",
        "payload_size_bytes": 64,
        "max_payload_size_bytes": 1024,
        "has_body": True,
    }

    result = orchestrate_runtime_security(payload)

    assert result == {
        "status": "stop",
        "stage": "validation",
        "reasons": ["request_id_missing"],
    }


def test_orchestrate_runtime_security_stops_on_rate_limit_failure() -> None:
    payload = _valid_payload()
    payload["rate_limit_input"] = {
        "client_id": "client-001",
        "request_count_in_window": 10,
        "max_requests_per_window": 10,
        "window_active": True,
    }

    result = orchestrate_runtime_security(payload)

    assert result == {
        "status": "stop",
        "stage": "rate_limit",
        "reasons": ["rate_limit_exceeded"],
    }


def test_orchestrate_runtime_security_continues_when_all_pass() -> None:
    payload = _valid_payload()

    result = orchestrate_runtime_security(payload)

    assert result == {
        "status": "continue",
        "stage": "complete",
        "reasons": ["security_checks_passed"],
    }


def test_orchestrate_runtime_security_exact_output_key_order() -> None:
    payload = _valid_payload()

    result = orchestrate_runtime_security(payload)

    assert list(result.keys()) == ["status", "stage", "reasons"]


def test_orchestrate_runtime_security_deterministic_repeated_calls() -> None:
    payload = _valid_payload()

    first = orchestrate_runtime_security(payload)
    second = orchestrate_runtime_security(payload)

    assert first == second
    assert list(first.keys()) == list(second.keys())


def test_orchestrate_runtime_security_input_immutability() -> None:
    payload = _valid_payload()
    original = {
        "auth_input": {
            "provided_api_key": payload["auth_input"]["provided_api_key"],
            "authorized_api_keys": list(payload["auth_input"]["authorized_api_keys"]),
        },
        "validation_input": {
            "request_id": payload["validation_input"]["request_id"],
            "endpoint": payload["validation_input"]["endpoint"],
            "content_type": payload["validation_input"]["content_type"],
            "payload_size_bytes": payload["validation_input"]["payload_size_bytes"],
            "max_payload_size_bytes": payload["validation_input"]["max_payload_size_bytes"],
            "has_body": payload["validation_input"]["has_body"],
        },
        "rate_limit_input": {
            "client_id": payload["rate_limit_input"]["client_id"],
            "request_count_in_window": payload["rate_limit_input"]["request_count_in_window"],
            "max_requests_per_window": payload["rate_limit_input"]["max_requests_per_window"],
            "window_active": payload["rate_limit_input"]["window_active"],
        },
    }

    orchestrate_runtime_security(payload)

    assert payload == original


@pytest.mark.parametrize(
    "payload",
    [
        {
            "validation_input": {
                "request_id": "req-001",
                "endpoint": "/runtime/execute",
                "content_type": "application/json",
                "payload_size_bytes": 64,
                "max_payload_size_bytes": 1024,
                "has_body": True,
            },
            "rate_limit_input": {
                "client_id": "client-001",
                "request_count_in_window": 1,
                "max_requests_per_window": 10,
                "window_active": True,
            },
        },
        {
            "auth_input": {
                "provided_api_key": "key-1",
                "authorized_api_keys": ["key-1", "key-2"],
            },
            "validation_input": {
                "request_id": "req-001",
                "endpoint": "/runtime/execute",
                "content_type": "application/json",
                "payload_size_bytes": 64,
                "max_payload_size_bytes": 1024,
                "has_body": True,
            },
            "rate_limit_input": {
                "client_id": "client-001",
                "request_count_in_window": 1,
                "max_requests_per_window": 10,
                "window_active": True,
            },
            "extra": True,
        },
    ],
)
def test_orchestrate_runtime_security_rejects_missing_or_extra_top_level_keys(
    payload: dict[str, object],
) -> None:
    with pytest.raises(ValueError, match="payload must include exactly"):
        orchestrate_runtime_security(payload)


@pytest.mark.parametrize(
    "key,bad_value,message",
    [
        ("auth_input", "bad", "auth_input must be a dict"),
        ("validation_input", "bad", "validation_input must be a dict"),
        ("rate_limit_input", "bad", "rate_limit_input must be a dict"),
    ],
)
def test_orchestrate_runtime_security_rejects_non_dict_top_level_inputs(
    key: str,
    bad_value: object,
    message: str,
) -> None:
    payload = _valid_payload()
    payload[key] = bad_value

    with pytest.raises(ValueError, match=message):
        orchestrate_runtime_security(payload)


def test_orchestrate_runtime_security_stage_values_are_exact() -> None:
    auth_stop = orchestrate_runtime_security(
        {
            "auth_input": {
                "provided_api_key": "",
                "authorized_api_keys": ["key-1"],
            },
            "validation_input": {
                "request_id": "req-001",
                "endpoint": "/runtime/execute",
                "content_type": "application/json",
                "payload_size_bytes": 64,
                "max_payload_size_bytes": 1024,
                "has_body": True,
            },
            "rate_limit_input": {
                "client_id": "client-001",
                "request_count_in_window": 1,
                "max_requests_per_window": 10,
                "window_active": True,
            },
        }
    )

    validation_stop = orchestrate_runtime_security(
        {
            "auth_input": {
                "provided_api_key": "key-1",
                "authorized_api_keys": ["key-1"],
            },
            "validation_input": {
                "request_id": "",
                "endpoint": "/runtime/execute",
                "content_type": "application/json",
                "payload_size_bytes": 64,
                "max_payload_size_bytes": 1024,
                "has_body": True,
            },
            "rate_limit_input": {
                "client_id": "client-001",
                "request_count_in_window": 1,
                "max_requests_per_window": 10,
                "window_active": True,
            },
        }
    )

    rate_limit_stop = orchestrate_runtime_security(
        {
            "auth_input": {
                "provided_api_key": "key-1",
                "authorized_api_keys": ["key-1"],
            },
            "validation_input": {
                "request_id": "req-001",
                "endpoint": "/runtime/execute",
                "content_type": "application/json",
                "payload_size_bytes": 64,
                "max_payload_size_bytes": 1024,
                "has_body": True,
            },
            "rate_limit_input": {
                "client_id": "client-001",
                "request_count_in_window": 5,
                "max_requests_per_window": 5,
                "window_active": True,
            },
        }
    )

    complete = orchestrate_runtime_security(_valid_payload())

    assert auth_stop["stage"] == "auth"
    assert validation_stop["stage"] == "validation"
    assert rate_limit_stop["stage"] == "rate_limit"
    assert complete["stage"] == "complete"
