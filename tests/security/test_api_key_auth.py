"""
Test suite for D15-MB1 API key authentication evaluator.
"""

from __future__ import annotations

from copy import deepcopy

import pytest

from fleetgraph_core.security.api_key_auth import evaluate_api_key_auth


def make_valid_input() -> dict:
    return {
        "provided_api_key": "key-123",
        "authorized_api_keys": ["key-999", "key-123", "key-abc"],
    }


class TestAuthorizedPath:
    def test_authorized_path(self) -> None:
        result = evaluate_api_key_auth(make_valid_input())

        assert result == {
            "status": "authorized",
            "reasons": ["api_key_authorized"],
        }


class TestUnauthorizedPaths:
    def test_missing_api_key_path(self) -> None:
        payload = make_valid_input()
        payload["provided_api_key"] = ""

        result = evaluate_api_key_auth(payload)

        assert result == {
            "status": "unauthorized",
            "reasons": ["api_key_missing"],
        }

    def test_empty_authorized_set_path(self) -> None:
        payload = make_valid_input()
        payload["authorized_api_keys"] = []

        result = evaluate_api_key_auth(payload)

        assert result == {
            "status": "unauthorized",
            "reasons": ["authorized_key_set_empty"],
        }

    def test_non_matching_key_path(self) -> None:
        payload = make_valid_input()
        payload["provided_api_key"] = "key-nope"

        result = evaluate_api_key_auth(payload)

        assert result == {
            "status": "unauthorized",
            "reasons": ["api_key_not_authorized"],
        }


class TestOutputContract:
    def test_exact_output_key_order(self) -> None:
        result = evaluate_api_key_auth(make_valid_input())

        assert tuple(result.keys()) == ("status", "reasons")

    def test_reasons_is_list(self) -> None:
        result = evaluate_api_key_auth(make_valid_input())

        assert isinstance(result["reasons"], list)


class TestDeterminism:
    def test_deterministic_repeated_calls(self) -> None:
        payload = make_valid_input()

        first = evaluate_api_key_auth(payload)
        second = evaluate_api_key_auth(payload)

        assert first == second


class TestInputImmutability:
    def test_input_not_mutated(self) -> None:
        payload = make_valid_input()
        before = deepcopy(payload)

        evaluate_api_key_auth(payload)

        assert payload == before


class TestMalformedInputRejection:
    def test_missing_keys_rejected(self) -> None:
        payload = make_valid_input()
        del payload["authorized_api_keys"]

        with pytest.raises(ValueError, match="missing required fields"):
            evaluate_api_key_auth(payload)

    def test_extra_keys_rejected(self) -> None:
        payload = make_valid_input()
        payload["unexpected"] = "x"

        with pytest.raises(ValueError, match="unexpected fields"):
            evaluate_api_key_auth(payload)

    def test_provided_api_key_not_string_rejected(self) -> None:
        payload = make_valid_input()
        payload["provided_api_key"] = 123

        with pytest.raises(TypeError, match="provided_api_key.*str"):
            evaluate_api_key_auth(payload)

    def test_authorized_api_keys_not_list_rejected(self) -> None:
        payload = make_valid_input()
        payload["authorized_api_keys"] = "key-123"

        with pytest.raises(TypeError, match="authorized_api_keys.*list"):
            evaluate_api_key_auth(payload)

    def test_authorized_api_key_entry_not_string_rejected(self) -> None:
        payload = make_valid_input()
        payload["authorized_api_keys"] = ["key-123", 7]

        with pytest.raises(TypeError, match="entry at index 1.*str"):
            evaluate_api_key_auth(payload)

    def test_authorized_api_key_entry_empty_string_rejected(self) -> None:
        payload = make_valid_input()
        payload["authorized_api_keys"] = ["key-123", ""]

        with pytest.raises(ValueError, match="entry at index 1.*non-empty string"):
            evaluate_api_key_auth(payload)
