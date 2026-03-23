"""
Test suite for D13-MB7 Runtime Logging Cost Guardrails Evaluator.
"""

from __future__ import annotations

from copy import deepcopy

import pytest

from fleetgraph_core.runtime.runtime_logging_cost_guardrails import (
    evaluate_runtime_logging_cost_guardrails,
)


def make_valid_input() -> dict:
    return {
        "log_level": "INFO",
        "event_category": "request",
        "payload_size_bytes": 512,
        "max_payload_size_bytes": 1024,
        "production_mode": True,
    }


class TestAllowDecision:
    def test_exact_allow_decision(self) -> None:
        result = evaluate_runtime_logging_cost_guardrails(make_valid_input())

        assert result == {
            "status": "allow",
            "reasons": ["within_logging_cost_limits"],
        }

    def test_non_production_debug_allowed_within_payload_limit(self) -> None:
        payload = make_valid_input()
        payload["production_mode"] = False
        payload["log_level"] = "DEBUG"

        result = evaluate_runtime_logging_cost_guardrails(payload)

        assert result == {
            "status": "allow",
            "reasons": ["within_logging_cost_limits"],
        }


class TestRejectPaths:
    def test_reject_for_payload_size_breach(self) -> None:
        payload = make_valid_input()
        payload["payload_size_bytes"] = 2048

        result = evaluate_runtime_logging_cost_guardrails(payload)

        assert result == {
            "status": "reject",
            "reasons": ["payload_size_limit_exceeded"],
        }

    def test_reject_for_debug_in_production(self) -> None:
        payload = make_valid_input()
        payload["log_level"] = "DEBUG"

        result = evaluate_runtime_logging_cost_guardrails(payload)

        assert result == {
            "status": "reject",
            "reasons": ["debug_logging_blocked_in_production"],
        }

    def test_reject_for_diagnostic_in_production(self) -> None:
        payload = make_valid_input()
        payload["event_category"] = "diagnostic"

        result = evaluate_runtime_logging_cost_guardrails(payload)

        assert result == {
            "status": "reject",
            "reasons": ["diagnostic_logging_blocked_in_production"],
        }

    def test_reject_multiple_violations_have_deterministic_reason_order(self) -> None:
        payload = make_valid_input()
        payload["payload_size_bytes"] = 2048
        payload["log_level"] = "DEBUG"
        payload["event_category"] = "diagnostic"

        result = evaluate_runtime_logging_cost_guardrails(payload)

        assert result == {
            "status": "reject",
            "reasons": [
                "payload_size_limit_exceeded",
                "debug_logging_blocked_in_production",
                "diagnostic_logging_blocked_in_production",
            ],
        }


class TestOutputContract:
    def test_exact_output_key_order(self) -> None:
        result = evaluate_runtime_logging_cost_guardrails(make_valid_input())

        assert tuple(result.keys()) == ("status", "reasons")

    def test_reasons_is_list(self) -> None:
        result = evaluate_runtime_logging_cost_guardrails(make_valid_input())

        assert isinstance(result["reasons"], list)


class TestDeterminism:
    def test_deterministic_repeated_calls(self) -> None:
        payload = make_valid_input()

        first = evaluate_runtime_logging_cost_guardrails(payload)
        second = evaluate_runtime_logging_cost_guardrails(payload)

        assert first == second


class TestInputImmutability:
    def test_input_not_mutated(self) -> None:
        payload = make_valid_input()
        before = deepcopy(payload)

        evaluate_runtime_logging_cost_guardrails(payload)

        assert payload == before


class TestMalformedInputRejection:
    def test_missing_keys_rejected(self) -> None:
        payload = make_valid_input()
        del payload["production_mode"]

        with pytest.raises(ValueError, match="missing required fields"):
            evaluate_runtime_logging_cost_guardrails(payload)

    def test_extra_keys_rejected(self) -> None:
        payload = make_valid_input()
        payload["unexpected"] = "x"

        with pytest.raises(ValueError, match="unexpected fields"):
            evaluate_runtime_logging_cost_guardrails(payload)

    def test_invalid_log_level_rejected(self) -> None:
        payload = make_valid_input()
        payload["log_level"] = "TRACE"

        with pytest.raises(ValueError, match="log_level.*must be one of"):
            evaluate_runtime_logging_cost_guardrails(payload)

    def test_invalid_event_category_rejected(self) -> None:
        payload = make_valid_input()
        payload["event_category"] = "audit"

        with pytest.raises(ValueError, match="event_category.*must be one of"):
            evaluate_runtime_logging_cost_guardrails(payload)

    @pytest.mark.parametrize(
        "field,bad_value",
        [
            ("payload_size_bytes", "1024"),
            ("max_payload_size_bytes", 1.0),
        ],
    )
    def test_non_integer_payload_fields_rejected(self, field: str, bad_value: object) -> None:
        payload = make_valid_input()
        payload[field] = bad_value

        with pytest.raises(TypeError, match=f"field '{field}'.*int"):
            evaluate_runtime_logging_cost_guardrails(payload)

    @pytest.mark.parametrize(
        "field",
        [
            "payload_size_bytes",
            "max_payload_size_bytes",
        ],
    )
    def test_negative_integer_payload_fields_rejected(self, field: str) -> None:
        payload = make_valid_input()
        payload[field] = -1

        with pytest.raises(ValueError, match=f"field '{field}'.*not be negative"):
            evaluate_runtime_logging_cost_guardrails(payload)

    @pytest.mark.parametrize(
        "field,bad_bool",
        [
            ("payload_size_bytes", True),
            ("max_payload_size_bytes", False),
        ],
    )
    def test_bool_values_rejected_for_integer_payload_fields(
        self,
        field: str,
        bad_bool: bool,
    ) -> None:
        payload = make_valid_input()
        payload[field] = bad_bool

        with pytest.raises(TypeError, match=f"field '{field}'.*int"):
            evaluate_runtime_logging_cost_guardrails(payload)

    def test_production_mode_not_bool_rejected(self) -> None:
        payload = make_valid_input()
        payload["production_mode"] = "true"

        with pytest.raises(TypeError, match="production_mode.*bool"):
            evaluate_runtime_logging_cost_guardrails(payload)
