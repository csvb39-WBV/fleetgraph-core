"""
Test suite for D3-MB3 Runtime Readiness Layer.
"""

from __future__ import annotations

from copy import deepcopy

import pytest

from fleetgraph_core.runtime.runtime_readiness_layer import build_runtime_readiness_response


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def make_ready_state() -> dict:
    return {
        "config_loaded": True,
        "bootstrap_complete": True,
    }


def make_not_ready_config_state() -> dict:
    return {
        "config_loaded": False,
        "bootstrap_complete": True,
    }


def make_not_ready_bootstrap_state() -> dict:
    return {
        "config_loaded": True,
        "bootstrap_complete": False,
    }


# ---------------------------------------------------------------------------
# Core behavior
# ---------------------------------------------------------------------------


class TestCoreBehavior:
    def test_ready_when_all_checks_true(self) -> None:
        result = build_runtime_readiness_response(make_ready_state())

        assert result["status"] == "ready"
        assert result["checks"]["config_loaded"] is True
        assert result["checks"]["bootstrap_complete"] is True

    def test_not_ready_when_config_loaded_false(self) -> None:
        result = build_runtime_readiness_response(make_not_ready_config_state())

        assert result["status"] == "not_ready"
        assert result["checks"]["config_loaded"] is False
        assert result["checks"]["bootstrap_complete"] is True

    def test_not_ready_when_bootstrap_complete_false(self) -> None:
        result = build_runtime_readiness_response(make_not_ready_bootstrap_state())

        assert result["status"] == "not_ready"
        assert result["checks"]["config_loaded"] is True
        assert result["checks"]["bootstrap_complete"] is False

    def test_not_ready_when_all_checks_false(self) -> None:
        result = build_runtime_readiness_response(
            {"config_loaded": False, "bootstrap_complete": False}
        )

        assert result["status"] == "not_ready"

    def test_ready_produces_exact_response(self) -> None:
        result = build_runtime_readiness_response(make_ready_state())

        assert result == {
            "status": "ready",
            "checks": {
                "config_loaded": True,
                "bootstrap_complete": True,
            },
        }

    def test_not_ready_produces_exact_response(self) -> None:
        result = build_runtime_readiness_response(make_not_ready_config_state())

        assert result == {
            "status": "not_ready",
            "checks": {
                "config_loaded": False,
                "bootstrap_complete": True,
            },
        }


# ---------------------------------------------------------------------------
# Key ordering
# ---------------------------------------------------------------------------


class TestKeyOrdering:
    def test_top_level_key_order_is_fixed(self) -> None:
        result = build_runtime_readiness_response(make_ready_state())

        assert tuple(result.keys()) == ("status", "checks")

    def test_checks_key_order_is_fixed(self) -> None:
        result = build_runtime_readiness_response(make_ready_state())

        assert tuple(result["checks"].keys()) == ("config_loaded", "bootstrap_complete")

    def test_key_order_preserved_for_not_ready(self) -> None:
        result = build_runtime_readiness_response(make_not_ready_bootstrap_state())

        assert tuple(result.keys()) == ("status", "checks")
        assert tuple(result["checks"].keys()) == ("config_loaded", "bootstrap_complete")


# ---------------------------------------------------------------------------
# Determinism
# ---------------------------------------------------------------------------


class TestDeterminism:
    def test_same_input_produces_same_output_ready(self) -> None:
        first = build_runtime_readiness_response(make_ready_state())
        second = build_runtime_readiness_response(make_ready_state())

        assert first == second

    def test_same_input_produces_same_output_not_ready(self) -> None:
        first = build_runtime_readiness_response(make_not_ready_config_state())
        second = build_runtime_readiness_response(make_not_ready_config_state())

        assert first == second


# ---------------------------------------------------------------------------
# Input validation
# ---------------------------------------------------------------------------


class TestInputValidation:
    def test_non_dict_rejected(self) -> None:
        with pytest.raises(TypeError, match="readiness_state must be a dict"):
            build_runtime_readiness_response("not a dict")  # type: ignore[arg-type]

    def test_missing_config_loaded_rejected(self) -> None:
        with pytest.raises(ValueError, match="config_loaded"):
            build_runtime_readiness_response({"bootstrap_complete": True})

    def test_missing_bootstrap_complete_rejected(self) -> None:
        with pytest.raises(ValueError, match="bootstrap_complete"):
            build_runtime_readiness_response({"config_loaded": True})

    def test_extra_fields_rejected(self) -> None:
        state = make_ready_state()
        state["extra"] = "unexpected"

        with pytest.raises(ValueError, match="unexpected fields"):
            build_runtime_readiness_response(state)

    def test_non_bool_config_loaded_rejected(self) -> None:
        state = make_ready_state()
        state["config_loaded"] = 1  # type: ignore[assignment]

        with pytest.raises(TypeError, match="config_loaded"):
            build_runtime_readiness_response(state)

    def test_non_bool_bootstrap_complete_rejected(self) -> None:
        state = make_ready_state()
        state["bootstrap_complete"] = "true"  # type: ignore[assignment]

        with pytest.raises(TypeError, match="bootstrap_complete"):
            build_runtime_readiness_response(state)


# ---------------------------------------------------------------------------
# Safety
# ---------------------------------------------------------------------------


class TestSafety:
    def test_input_not_mutated(self) -> None:
        state = make_ready_state()
        state_before = deepcopy(state)

        build_runtime_readiness_response(state)

        assert state == state_before
