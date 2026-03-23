"""
Test suite for D12-MB3 CI Determinism Harness.
"""

from __future__ import annotations

import sys
from copy import deepcopy
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
CI_SCRIPTS_ROOT = REPO_ROOT / "scripts" / "ci"

if str(CI_SCRIPTS_ROOT) not in sys.path:
    sys.path.insert(0, str(CI_SCRIPTS_ROOT))

from determinism_harness import run_determinism_check  # noqa: E402


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _echo(payload):
    """Deterministic callable — always returns the payload unchanged."""
    return payload


def _constant(payload):
    """Deterministic callable — always returns the same constant."""
    return {"status": "ok", "value": 42}


_call_counter: dict[str, int] = {}


def _non_deterministic(payload):
    """Returns a different value on each call."""
    key = str(payload)
    _call_counter[key] = _call_counter.get(key, 0) + 1
    return {"count": _call_counter[key]}


def _always_raises(payload):
    raise RuntimeError("deliberate failure")


# ---------------------------------------------------------------------------
# Pass path
# ---------------------------------------------------------------------------


class TestPassPath:
    def test_deterministic_echo_callable_passes(self) -> None:
        result = run_determinism_check(_echo, {"key": "value"}, repeat_count=5)

        assert result["status"] == "pass"
        assert result["errors"] == []

    def test_deterministic_constant_callable_passes(self) -> None:
        result = run_determinism_check(_constant, {"any": "input"}, repeat_count=3)

        assert result["status"] == "pass"
        assert result["errors"] == []

    def test_repeat_count_two_minimum_passes(self) -> None:
        result = run_determinism_check(_echo, "simple_input", repeat_count=2)

        assert result["status"] == "pass"

    def test_deterministic_lambda_passes(self) -> None:
        result = run_determinism_check(lambda p: {"x": 1}, {}, repeat_count=4)

        assert result["status"] == "pass"


# ---------------------------------------------------------------------------
# Non-deterministic callable
# ---------------------------------------------------------------------------


class TestNonDeterministicCallable:
    def setup_method(self) -> None:
        _call_counter.clear()

    def test_non_deterministic_callable_fails(self) -> None:
        result = run_determinism_check(_non_deterministic, "probe", repeat_count=3)

        assert result["status"] == "fail"
        assert len(result["errors"]) >= 1

    def test_non_deterministic_error_message_identifies_run(self) -> None:
        result = run_determinism_check(_non_deterministic, "probe", repeat_count=3)

        assert any("run 2" in e for e in result["errors"])

    def test_diverging_runs_all_reported(self) -> None:
        result = run_determinism_check(_non_deterministic, "probe", repeat_count=4)

        diverging = [e for e in result["errors"] if "differs from run 1" in e]
        assert len(diverging) >= 2


# ---------------------------------------------------------------------------
# Callable raises
# ---------------------------------------------------------------------------


class TestCallableRaises:
    def test_raising_callable_fails(self) -> None:
        result = run_determinism_check(_always_raises, {}, repeat_count=3)

        assert result["status"] == "fail"

    def test_raising_callable_error_message_contains_exception_info(self) -> None:
        result = run_determinism_check(_always_raises, {}, repeat_count=3)

        assert any("raised an exception" in e for e in result["errors"])
        assert any("RuntimeError" in e for e in result["errors"])

    def test_raising_callable_error_identifies_run_number(self) -> None:
        result = run_determinism_check(_always_raises, {}, repeat_count=3)

        assert any("run 1" in e for e in result["errors"])

    def test_raises_on_first_call_stops_cleanly(self) -> None:
        result = run_determinism_check(_always_raises, {}, repeat_count=5)

        # Only the first call raises; error list should be stable (exactly 1 entry)
        assert len(result["errors"]) == 1


# ---------------------------------------------------------------------------
# repeat_count validation
# ---------------------------------------------------------------------------


class TestRepeatCountValidation:
    def test_repeat_count_one_rejected(self) -> None:
        with pytest.raises(ValueError, match="repeat_count must be >= 2"):
            run_determinism_check(_echo, {}, repeat_count=1)

    def test_repeat_count_zero_rejected(self) -> None:
        with pytest.raises(ValueError, match="repeat_count must be >= 2"):
            run_determinism_check(_echo, {}, repeat_count=0)

    def test_negative_repeat_count_rejected(self) -> None:
        with pytest.raises(ValueError, match="repeat_count must be >= 2"):
            run_determinism_check(_echo, {}, repeat_count=-1)

    def test_non_int_repeat_count_rejected(self) -> None:
        with pytest.raises(TypeError, match="repeat_count must be an int"):
            run_determinism_check(_echo, {}, repeat_count="3")  # type: ignore[arg-type]

    def test_float_repeat_count_rejected(self) -> None:
        with pytest.raises(TypeError, match="repeat_count must be an int"):
            run_determinism_check(_echo, {}, repeat_count=3.0)  # type: ignore[arg-type]

    def test_bool_repeat_count_rejected(self) -> None:
        with pytest.raises(TypeError, match="repeat_count must be an int"):
            run_determinism_check(_echo, {}, repeat_count=True)  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# Non-callable input
# ---------------------------------------------------------------------------


class TestNonCallableInput:
    def test_string_rejected(self) -> None:
        with pytest.raises(TypeError, match="callable_under_test must be callable"):
            run_determinism_check("not_callable", {}, repeat_count=3)  # type: ignore[arg-type]

    def test_integer_rejected(self) -> None:
        with pytest.raises(TypeError, match="callable_under_test must be callable"):
            run_determinism_check(42, {}, repeat_count=3)  # type: ignore[arg-type]

    def test_none_rejected(self) -> None:
        with pytest.raises(TypeError, match="callable_under_test must be callable"):
            run_determinism_check(None, {}, repeat_count=3)  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# Output contract
# ---------------------------------------------------------------------------


class TestOutputContract:
    def test_fixed_key_order_on_pass(self) -> None:
        result = run_determinism_check(_echo, {}, repeat_count=2)

        assert tuple(result.keys()) == ("status", "errors")

    def test_fixed_key_order_on_fail(self) -> None:
        _call_counter.clear()
        result = run_determinism_check(_non_deterministic, "x", repeat_count=3)

        assert tuple(result.keys()) == ("status", "errors")

    def test_errors_is_list_on_pass(self) -> None:
        result = run_determinism_check(_echo, {}, repeat_count=2)

        assert isinstance(result["errors"], list)

    def test_errors_empty_on_pass(self) -> None:
        result = run_determinism_check(_constant, {}, repeat_count=4)

        assert result["errors"] == []

    def test_status_bounded_to_pass_or_fail(self) -> None:
        pass_result = run_determinism_check(_echo, {}, repeat_count=2)
        _call_counter.clear()
        fail_result = run_determinism_check(_non_deterministic, "z", repeat_count=3)

        assert pass_result["status"] == "pass"
        assert fail_result["status"] == "fail"


# ---------------------------------------------------------------------------
# Harness-level determinism
# ---------------------------------------------------------------------------


class TestHarnessDeterminism:
    def test_same_deterministic_call_produces_same_result_twice(self) -> None:
        first = run_determinism_check(_constant, {"in": 1}, repeat_count=3)
        second = run_determinism_check(_constant, {"in": 1}, repeat_count=3)

        assert first == second

    def test_harness_raises_do_not_vary_between_runs(self) -> None:
        first = run_determinism_check(_always_raises, {}, repeat_count=3)
        second = run_determinism_check(_always_raises, {}, repeat_count=3)

        assert first == second


# ---------------------------------------------------------------------------
# Input immutability
# ---------------------------------------------------------------------------


class TestInputImmutability:
    def test_input_payload_not_mutated_on_pass(self) -> None:
        payload = {"a": 1, "b": [2, 3]}
        payload_before = deepcopy(payload)

        run_determinism_check(_echo, payload, repeat_count=3)

        assert payload == payload_before

    def test_input_payload_not_mutated_on_fail(self) -> None:
        _call_counter.clear()
        payload = "probe_key"

        run_determinism_check(_non_deterministic, payload, repeat_count=3)

        assert payload == "probe_key"

    def test_input_payload_not_mutated_when_callable_raises(self) -> None:
        payload = {"x": 99}
        payload_before = deepcopy(payload)

        run_determinism_check(_always_raises, payload, repeat_count=2)

        assert payload == payload_before
