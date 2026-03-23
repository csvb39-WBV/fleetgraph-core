"""
Test suite for D12-MB4 CI Auto-Rejection Orchestrator.
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

from auto_rejection_orchestrator import run_auto_rejection  # noqa: E402


# ---------------------------------------------------------------------------
# Fixtures / helpers
# ---------------------------------------------------------------------------

_PASS_COMPONENT = {"status": "pass", "errors": []}
_FAIL_FILE_FORMAT = {"status": "fail", "errors": ["invalid path: bad/path.py"]}
_FAIL_SCHEMA = {"status": "fail", "errors": ["runtime_health: missing key 'status'"]}
_FAIL_DETERMINISM = {"status": "fail", "errors": ["run 2 output differs from run 1"]}


def _all_pass() -> dict:
    return {
        "file_format": {"status": "pass", "errors": []},
        "schema": {"status": "pass", "errors": []},
        "determinism": {"status": "pass", "errors": []},
    }


# ---------------------------------------------------------------------------
# All-pass path
# ---------------------------------------------------------------------------


class TestAllPass:
    def test_all_pass_returns_pass(self) -> None:
        result = run_auto_rejection(_all_pass())

        assert result["status"] == "pass"

    def test_all_pass_errors_empty(self) -> None:
        result = run_auto_rejection(_all_pass())

        assert result["errors"] == []

    def test_all_pass_fixed_key_order(self) -> None:
        result = run_auto_rejection(_all_pass())

        assert tuple(result.keys()) == ("status", "errors")


# ---------------------------------------------------------------------------
# Individual component failures
# ---------------------------------------------------------------------------


class TestIndividualComponentFailure:
    def test_file_format_fail_returns_fail(self) -> None:
        inp = _all_pass()
        inp["file_format"] = _FAIL_FILE_FORMAT

        result = run_auto_rejection(inp)

        assert result["status"] == "fail"

    def test_file_format_fail_errors_propagated(self) -> None:
        inp = _all_pass()
        inp["file_format"] = _FAIL_FILE_FORMAT

        result = run_auto_rejection(inp)

        assert _FAIL_FILE_FORMAT["errors"][0] in result["errors"]

    def test_schema_fail_returns_fail(self) -> None:
        inp = _all_pass()
        inp["schema"] = _FAIL_SCHEMA

        result = run_auto_rejection(inp)

        assert result["status"] == "fail"

    def test_schema_fail_errors_propagated(self) -> None:
        inp = _all_pass()
        inp["schema"] = _FAIL_SCHEMA

        result = run_auto_rejection(inp)

        assert _FAIL_SCHEMA["errors"][0] in result["errors"]

    def test_determinism_fail_returns_fail(self) -> None:
        inp = _all_pass()
        inp["determinism"] = _FAIL_DETERMINISM

        result = run_auto_rejection(inp)

        assert result["status"] == "fail"

    def test_determinism_fail_errors_propagated(self) -> None:
        inp = _all_pass()
        inp["determinism"] = _FAIL_DETERMINISM

        result = run_auto_rejection(inp)

        assert _FAIL_DETERMINISM["errors"][0] in result["errors"]


# ---------------------------------------------------------------------------
# Multiple failing components — deterministic error order
# ---------------------------------------------------------------------------


class TestMultipleFailures:
    def test_all_fail_returns_fail(self) -> None:
        inp = {
            "file_format": _FAIL_FILE_FORMAT,
            "schema": _FAIL_SCHEMA,
            "determinism": _FAIL_DETERMINISM,
        }

        result = run_auto_rejection(inp)

        assert result["status"] == "fail"

    def test_all_fail_errors_aggregated_in_order(self) -> None:
        inp = {
            "file_format": _FAIL_FILE_FORMAT,
            "schema": _FAIL_SCHEMA,
            "determinism": _FAIL_DETERMINISM,
        }

        result = run_auto_rejection(inp)

        errors = result["errors"]
        ff_idx = errors.index(_FAIL_FILE_FORMAT["errors"][0])
        sc_idx = errors.index(_FAIL_SCHEMA["errors"][0])
        dt_idx = errors.index(_FAIL_DETERMINISM["errors"][0])
        assert ff_idx < sc_idx < dt_idx

    def test_file_format_and_schema_fail_determinism_pass(self) -> None:
        inp = {
            "file_format": _FAIL_FILE_FORMAT,
            "schema": _FAIL_SCHEMA,
            "determinism": {"status": "pass", "errors": []},
        }

        result = run_auto_rejection(inp)

        assert result["status"] == "fail"
        errors = result["errors"]
        assert _FAIL_FILE_FORMAT["errors"][0] in errors
        assert _FAIL_SCHEMA["errors"][0] in errors

    def test_schema_and_determinism_fail_file_format_pass(self) -> None:
        inp = {
            "file_format": {"status": "pass", "errors": []},
            "schema": _FAIL_SCHEMA,
            "determinism": _FAIL_DETERMINISM,
        }

        result = run_auto_rejection(inp)

        errors = result["errors"]
        sc_idx = errors.index(_FAIL_SCHEMA["errors"][0])
        dt_idx = errors.index(_FAIL_DETERMINISM["errors"][0])
        assert sc_idx < dt_idx

    def test_multiple_errors_per_component_aggregated(self) -> None:
        inp = {
            "file_format": {"status": "fail", "errors": ["err_ff_1", "err_ff_2"]},
            "schema": {"status": "fail", "errors": ["err_sc_1"]},
            "determinism": {"status": "pass", "errors": []},
        }

        result = run_auto_rejection(inp)

        assert result["errors"] == ["err_ff_1", "err_ff_2", "err_sc_1"]


# ---------------------------------------------------------------------------
# Output key order
# ---------------------------------------------------------------------------


class TestOutputKeyOrder:
    def test_fixed_key_order_on_fail(self) -> None:
        inp = _all_pass()
        inp["file_format"] = _FAIL_FILE_FORMAT

        result = run_auto_rejection(inp)

        assert tuple(result.keys()) == ("status", "errors")

    def test_errors_is_list_type(self) -> None:
        result = run_auto_rejection(_all_pass())

        assert isinstance(result["errors"], list)


# ---------------------------------------------------------------------------
# Deterministic repeated calls
# ---------------------------------------------------------------------------


class TestDeterministicCalls:
    def test_same_input_same_output_pass(self) -> None:
        first = run_auto_rejection(_all_pass())
        second = run_auto_rejection(_all_pass())

        assert first == second

    def test_same_input_same_output_fail(self) -> None:
        inp = {
            "file_format": _FAIL_FILE_FORMAT,
            "schema": _FAIL_SCHEMA,
            "determinism": _FAIL_DETERMINISM,
        }

        first = run_auto_rejection(inp)
        second = run_auto_rejection(inp)

        assert first == second

    def test_error_order_stable_across_calls(self) -> None:
        inp = {
            "file_format": _FAIL_FILE_FORMAT,
            "schema": _FAIL_SCHEMA,
            "determinism": _FAIL_DETERMINISM,
        }

        results = [run_auto_rejection(inp) for _ in range(5)]
        error_lists = [r["errors"] for r in results]
        assert all(e == error_lists[0] for e in error_lists)


# ---------------------------------------------------------------------------
# Input immutability
# ---------------------------------------------------------------------------


class TestInputImmutability:
    def test_input_not_mutated_on_pass(self) -> None:
        inp = _all_pass()
        before = deepcopy(inp)

        run_auto_rejection(inp)

        assert inp == before

    def test_input_not_mutated_on_fail(self) -> None:
        inp = {
            "file_format": _FAIL_FILE_FORMAT,
            "schema": _FAIL_SCHEMA,
            "determinism": _FAIL_DETERMINISM,
        }
        before = deepcopy(inp)

        run_auto_rejection(inp)

        assert inp == before


# ---------------------------------------------------------------------------
# Malformed input rejection
# ---------------------------------------------------------------------------


class TestMalformedInputRejection:
    def test_non_dict_input_raises_type_error(self) -> None:
        with pytest.raises(TypeError, match="check_results must be a dict"):
            run_auto_rejection("not a dict")  # type: ignore[arg-type]

    def test_list_input_raises_type_error(self) -> None:
        with pytest.raises(TypeError, match="check_results must be a dict"):
            run_auto_rejection([])  # type: ignore[arg-type]

    def test_none_input_raises_type_error(self) -> None:
        with pytest.raises(TypeError, match="check_results must be a dict"):
            run_auto_rejection(None)  # type: ignore[arg-type]

    def test_missing_component_key_raises_value_error(self) -> None:
        inp = {
            "file_format": _PASS_COMPONENT,
            "schema": _PASS_COMPONENT,
            # "determinism" missing
        }

        with pytest.raises(ValueError, match="missing component keys"):
            run_auto_rejection(inp)

    def test_extra_component_key_raises_value_error(self) -> None:
        inp = _all_pass()
        inp["extra"] = _PASS_COMPONENT  # type: ignore[assignment]

        with pytest.raises(ValueError, match="unexpected component keys"):
            run_auto_rejection(inp)

    def test_component_value_not_dict_raises_type_error(self) -> None:
        inp = _all_pass()
        inp["schema"] = "not a dict"  # type: ignore[assignment]

        with pytest.raises(TypeError, match="component 'schema' must be a dict"):
            run_auto_rejection(inp)

    def test_component_missing_status_raises_value_error(self) -> None:
        inp = _all_pass()
        inp["determinism"] = {"errors": []}  # no "status"

        with pytest.raises(ValueError, match="missing required fields"):
            run_auto_rejection(inp)

    def test_component_missing_errors_raises_value_error(self) -> None:
        inp = _all_pass()
        inp["file_format"] = {"status": "pass"}  # no "errors"

        with pytest.raises(ValueError, match="missing required fields"):
            run_auto_rejection(inp)

    def test_invalid_status_value_raises_value_error(self) -> None:
        inp = _all_pass()
        inp["schema"] = {"status": "unknown", "errors": []}

        with pytest.raises(ValueError, match="invalid status"):
            run_auto_rejection(inp)

    def test_errors_not_list_raises_value_error(self) -> None:
        inp = _all_pass()
        inp["determinism"] = {"status": "pass", "errors": "oops"}

        with pytest.raises(ValueError, match="errors must be a list"):
            run_auto_rejection(inp)

    def test_all_three_components_missing_raises_value_error(self) -> None:
        with pytest.raises(ValueError, match="missing component keys"):
            run_auto_rejection({})
