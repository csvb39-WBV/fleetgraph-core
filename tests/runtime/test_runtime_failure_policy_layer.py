"""
Test suite for D3-MB4 Runtime Failure Policy Layer.
"""

from __future__ import annotations

from copy import deepcopy

import pytest

from fleetgraph_core.runtime.runtime_failure_policy_layer import (
    CANONICAL_FAILURE_TYPES,
    build_failure_policy_response,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def make_input(
    failure_type: str = "VALIDATION_ERROR",
    attempt_count: int = 0,
    max_retries: int = 3,
) -> dict:
    return {
        "failure_type": failure_type,
        "attempt_count": attempt_count,
        "max_retries": max_retries,
    }


# ---------------------------------------------------------------------------
# Core retry rules
# ---------------------------------------------------------------------------


class TestRetryRules:
    def test_validation_error_never_retries(self) -> None:
        result = build_failure_policy_response(
            make_input("VALIDATION_ERROR", attempt_count=0, max_retries=5)
        )

        assert result["should_retry"] is False
        assert result["retry_decision"] == "do_not_retry"

    def test_validation_error_never_retries_regardless_of_attempts(self) -> None:
        result = build_failure_policy_response(
            make_input("VALIDATION_ERROR", attempt_count=0, max_retries=0)
        )

        assert result["should_retry"] is False
        assert result["retry_decision"] == "do_not_retry"

    def test_execution_error_never_retries(self) -> None:
        result = build_failure_policy_response(
            make_input("EXECUTION_ERROR", attempt_count=0, max_retries=5)
        )

        assert result["should_retry"] is False
        assert result["retry_decision"] == "do_not_retry"

    def test_execution_error_never_retries_regardless_of_attempts(self) -> None:
        result = build_failure_policy_response(
            make_input("EXECUTION_ERROR", attempt_count=0, max_retries=0)
        )

        assert result["should_retry"] is False
        assert result["retry_decision"] == "do_not_retry"

    def test_timeout_error_retries_when_below_max(self) -> None:
        result = build_failure_policy_response(
            make_input("TIMEOUT_ERROR", attempt_count=1, max_retries=3)
        )

        assert result["should_retry"] is True
        assert result["retry_decision"] == "retry"

    def test_timeout_error_does_not_retry_at_max(self) -> None:
        result = build_failure_policy_response(
            make_input("TIMEOUT_ERROR", attempt_count=3, max_retries=3)
        )

        assert result["should_retry"] is False
        assert result["retry_decision"] == "do_not_retry"

    def test_timeout_error_does_not_retry_above_max(self) -> None:
        result = build_failure_policy_response(
            make_input("TIMEOUT_ERROR", attempt_count=5, max_retries=3)
        )

        assert result["should_retry"] is False
        assert result["retry_decision"] == "do_not_retry"

    def test_timeout_error_retries_at_zero_attempts_with_max_above_zero(self) -> None:
        result = build_failure_policy_response(
            make_input("TIMEOUT_ERROR", attempt_count=0, max_retries=1)
        )

        assert result["should_retry"] is True

    def test_timeout_error_does_not_retry_when_max_retries_zero(self) -> None:
        result = build_failure_policy_response(
            make_input("TIMEOUT_ERROR", attempt_count=0, max_retries=0)
        )

        assert result["should_retry"] is False

    def test_dependency_error_retries_when_below_max(self) -> None:
        result = build_failure_policy_response(
            make_input("DEPENDENCY_ERROR", attempt_count=2, max_retries=5)
        )

        assert result["should_retry"] is True
        assert result["retry_decision"] == "retry"

    def test_dependency_error_does_not_retry_at_max(self) -> None:
        result = build_failure_policy_response(
            make_input("DEPENDENCY_ERROR", attempt_count=3, max_retries=3)
        )

        assert result["should_retry"] is False
        assert result["retry_decision"] == "do_not_retry"

    def test_dependency_error_does_not_retry_when_max_retries_zero(self) -> None:
        result = build_failure_policy_response(
            make_input("DEPENDENCY_ERROR", attempt_count=0, max_retries=0)
        )

        assert result["should_retry"] is False


# ---------------------------------------------------------------------------
# Exact output values
# ---------------------------------------------------------------------------


class TestExactOutput:
    def test_validation_error_exact_response(self) -> None:
        result = build_failure_policy_response(
            make_input("VALIDATION_ERROR", attempt_count=0, max_retries=3)
        )

        assert result == {
            "failure_type": "VALIDATION_ERROR",
            "should_retry": False,
            "retry_decision": "do_not_retry",
        }

    def test_timeout_error_retry_exact_response(self) -> None:
        result = build_failure_policy_response(
            make_input("TIMEOUT_ERROR", attempt_count=1, max_retries=3)
        )

        assert result == {
            "failure_type": "TIMEOUT_ERROR",
            "should_retry": True,
            "retry_decision": "retry",
        }

    def test_failure_type_preserved_in_output(self) -> None:
        for failure_type in sorted(CANONICAL_FAILURE_TYPES):
            result = build_failure_policy_response(
                make_input(failure_type, attempt_count=0, max_retries=3)
            )
            assert result["failure_type"] == failure_type


# ---------------------------------------------------------------------------
# Key ordering
# ---------------------------------------------------------------------------


class TestKeyOrdering:
    def test_top_level_key_order_fixed_for_no_retry(self) -> None:
        result = build_failure_policy_response(make_input("VALIDATION_ERROR"))

        assert tuple(result.keys()) == ("failure_type", "should_retry", "retry_decision")

    def test_top_level_key_order_fixed_for_retry(self) -> None:
        result = build_failure_policy_response(
            make_input("TIMEOUT_ERROR", attempt_count=0, max_retries=3)
        )

        assert tuple(result.keys()) == ("failure_type", "should_retry", "retry_decision")


# ---------------------------------------------------------------------------
# Determinism
# ---------------------------------------------------------------------------


class TestDeterminism:
    def test_same_input_produces_same_output_no_retry(self) -> None:
        inp = make_input("EXECUTION_ERROR", attempt_count=1, max_retries=5)

        first = build_failure_policy_response(inp)
        second = build_failure_policy_response(inp)

        assert first == second

    def test_same_input_produces_same_output_retry(self) -> None:
        inp = make_input("DEPENDENCY_ERROR", attempt_count=0, max_retries=3)

        first = build_failure_policy_response(inp)
        second = build_failure_policy_response(inp)

        assert first == second


# ---------------------------------------------------------------------------
# Input immutability
# ---------------------------------------------------------------------------


class TestInputImmutability:
    def test_input_not_mutated(self) -> None:
        inp = make_input("TIMEOUT_ERROR", attempt_count=2, max_retries=5)
        inp_before = deepcopy(inp)

        build_failure_policy_response(inp)

        assert inp == inp_before


# ---------------------------------------------------------------------------
# Input validation
# ---------------------------------------------------------------------------


class TestInputValidation:
    def test_non_dict_rejected(self) -> None:
        with pytest.raises(TypeError, match="policy_input must be a dict"):
            build_failure_policy_response("not a dict")  # type: ignore[arg-type]

    def test_missing_failure_type_rejected(self) -> None:
        with pytest.raises(ValueError, match="failure_type"):
            build_failure_policy_response({"attempt_count": 0, "max_retries": 3})

    def test_missing_attempt_count_rejected(self) -> None:
        with pytest.raises(ValueError, match="attempt_count"):
            build_failure_policy_response({"failure_type": "VALIDATION_ERROR", "max_retries": 3})

    def test_missing_max_retries_rejected(self) -> None:
        with pytest.raises(ValueError, match="max_retries"):
            build_failure_policy_response(
                {"failure_type": "VALIDATION_ERROR", "attempt_count": 0}
            )

    def test_extra_fields_rejected(self) -> None:
        inp = make_input()
        inp["extra"] = "unexpected"

        with pytest.raises(ValueError, match="unexpected fields"):
            build_failure_policy_response(inp)

    def test_invalid_failure_type_rejected(self) -> None:
        with pytest.raises(ValueError, match="failure_type"):
            build_failure_policy_response(make_input("UNKNOWN_ERROR"))

    def test_non_string_failure_type_rejected(self) -> None:
        inp = make_input()
        inp["failure_type"] = 123  # type: ignore[assignment]

        with pytest.raises(TypeError, match="failure_type"):
            build_failure_policy_response(inp)

    def test_non_integer_attempt_count_rejected(self) -> None:
        inp = make_input()
        inp["attempt_count"] = "1"  # type: ignore[assignment]

        with pytest.raises(TypeError, match="attempt_count"):
            build_failure_policy_response(inp)

    def test_bool_attempt_count_rejected(self) -> None:
        inp = make_input()
        inp["attempt_count"] = True  # type: ignore[assignment]

        with pytest.raises(TypeError, match="attempt_count"):
            build_failure_policy_response(inp)

    def test_non_integer_max_retries_rejected(self) -> None:
        inp = make_input()
        inp["max_retries"] = 3.0  # type: ignore[assignment]

        with pytest.raises(TypeError, match="max_retries"):
            build_failure_policy_response(inp)

    def test_bool_max_retries_rejected(self) -> None:
        inp = make_input()
        inp["max_retries"] = False  # type: ignore[assignment]

        with pytest.raises(TypeError, match="max_retries"):
            build_failure_policy_response(inp)

    def test_negative_attempt_count_rejected(self) -> None:
        with pytest.raises(ValueError, match="attempt_count"):
            build_failure_policy_response(make_input(attempt_count=-1))

    def test_negative_max_retries_rejected(self) -> None:
        with pytest.raises(ValueError, match="max_retries"):
            build_failure_policy_response(make_input(max_retries=-1))

    def test_all_canonical_failure_types_accepted(self) -> None:
        for failure_type in CANONICAL_FAILURE_TYPES:
            result = build_failure_policy_response(make_input(failure_type))
            assert result["failure_type"] == failure_type
