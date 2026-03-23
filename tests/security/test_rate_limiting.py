"""
Test suite for D15-MB3 rate limiting evaluator.
"""

from __future__ import annotations

from copy import deepcopy

import pytest

from fleetgraph_core.security.rate_limiting import evaluate_rate_limiting


def make_valid_input() -> dict:
    return {
        "client_id": "client_001",
        "request_count_in_window": 3,
        "max_requests_per_window": 10,
        "window_active": True,
    }


class TestAllowPaths:
    def test_allow_when_window_inactive(self) -> None:
        payload = make_valid_input()
        payload["window_active"] = False

        result = evaluate_rate_limiting(payload)

        assert result == {
            "status": "allow",
            "reasons": ["window_not_active"],
        }

    def test_allow_within_rate_limit(self) -> None:
        payload = make_valid_input()
        payload["window_active"] = True
        payload["request_count_in_window"] = 9
        payload["max_requests_per_window"] = 10

        result = evaluate_rate_limiting(payload)

        assert result == {
            "status": "allow",
            "reasons": ["within_rate_limit"],
        }


class TestRejectPaths:
    def test_reject_when_limit_reached(self) -> None:
        payload = make_valid_input()
        payload["window_active"] = True
        payload["request_count_in_window"] = 10
        payload["max_requests_per_window"] = 10

        result = evaluate_rate_limiting(payload)

        assert result == {
            "status": "reject",
            "reasons": ["rate_limit_exceeded"],
        }

    def test_reject_when_limit_exceeded(self) -> None:
        payload = make_valid_input()
        payload["window_active"] = True
        payload["request_count_in_window"] = 11
        payload["max_requests_per_window"] = 10

        result = evaluate_rate_limiting(payload)

        assert result == {
            "status": "reject",
            "reasons": ["rate_limit_exceeded"],
        }

    def test_reject_on_empty_client_id(self) -> None:
        payload = make_valid_input()
        payload["client_id"] = ""
        payload["window_active"] = False

        result = evaluate_rate_limiting(payload)

        assert result == {
            "status": "reject",
            "reasons": ["client_id_missing"],
        }


class TestOutputContract:
    def test_exact_output_key_order(self) -> None:
        result = evaluate_rate_limiting(make_valid_input())

        assert tuple(result.keys()) == ("status", "reasons")

    def test_reasons_is_list(self) -> None:
        result = evaluate_rate_limiting(make_valid_input())

        assert isinstance(result["reasons"], list)


class TestDeterminism:
    def test_deterministic_repeated_calls(self) -> None:
        payload = make_valid_input()

        first = evaluate_rate_limiting(payload)
        second = evaluate_rate_limiting(payload)

        assert first == second


class TestInputImmutability:
    def test_input_not_mutated(self) -> None:
        payload = make_valid_input()
        before = deepcopy(payload)

        evaluate_rate_limiting(payload)

        assert payload == before


class TestMalformedInputRejection:
    def test_missing_keys_rejected(self) -> None:
        payload = make_valid_input()
        del payload["window_active"]

        with pytest.raises(ValueError, match="missing required fields"):
            evaluate_rate_limiting(payload)

    def test_extra_keys_rejected(self) -> None:
        payload = make_valid_input()
        payload["unexpected"] = "x"

        with pytest.raises(ValueError, match="unexpected fields"):
            evaluate_rate_limiting(payload)

    def test_client_id_not_string_rejected(self) -> None:
        payload = make_valid_input()
        payload["client_id"] = 1

        with pytest.raises(TypeError, match="client_id.*str"):
            evaluate_rate_limiting(payload)

    def test_request_count_not_int_rejected(self) -> None:
        payload = make_valid_input()
        payload["request_count_in_window"] = "3"

        with pytest.raises(TypeError, match="request_count_in_window.*int"):
            evaluate_rate_limiting(payload)

    def test_max_requests_not_int_rejected(self) -> None:
        payload = make_valid_input()
        payload["max_requests_per_window"] = 10.0

        with pytest.raises(TypeError, match="max_requests_per_window.*int"):
            evaluate_rate_limiting(payload)

    def test_bool_request_count_rejected(self) -> None:
        payload = make_valid_input()
        payload["request_count_in_window"] = True

        with pytest.raises(TypeError, match="request_count_in_window.*int"):
            evaluate_rate_limiting(payload)

    def test_bool_max_requests_rejected(self) -> None:
        payload = make_valid_input()
        payload["max_requests_per_window"] = False

        with pytest.raises(TypeError, match="max_requests_per_window.*int"):
            evaluate_rate_limiting(payload)

    def test_negative_request_count_rejected(self) -> None:
        payload = make_valid_input()
        payload["request_count_in_window"] = -1

        with pytest.raises(ValueError, match="request_count_in_window.*not be negative"):
            evaluate_rate_limiting(payload)

    def test_negative_max_requests_rejected(self) -> None:
        payload = make_valid_input()
        payload["max_requests_per_window"] = -1

        with pytest.raises(ValueError, match="max_requests_per_window.*not be negative"):
            evaluate_rate_limiting(payload)

    def test_window_active_not_bool_rejected(self) -> None:
        payload = make_valid_input()
        payload["window_active"] = "true"

        with pytest.raises(TypeError, match="window_active.*bool"):
            evaluate_rate_limiting(payload)
