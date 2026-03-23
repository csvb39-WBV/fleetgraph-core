"""
Test suite for D12-MB2 CI Schema Validator.
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

from schema_validator import SUPPORTED_ENDPOINTS, validate_endpoint_schema  # noqa: E402


# ---------------------------------------------------------------------------
# Canonical valid payloads
# ---------------------------------------------------------------------------

_RUNTIME_BLOCK = {
    "environment": "staging",
    "api_host": "0.0.0.0",
    "api_port": 8000,
    "debug": False,
    "log_level": "INFO",
    "logger_name": "fleetgraph.runtime.staging",
    "logger_level": "INFO",
}

VALID_PAYLOADS: dict[str, dict] = {
    "runtime_summary": dict(_RUNTIME_BLOCK),
    "runtime_external": {
        "response_type": "runtime_external_api_response",
        "response_schema_version": "1.0",
        "runtime": dict(_RUNTIME_BLOCK),
    },
    "runtime_health": {
        "response_type": "runtime_health_response",
        "response_schema_version": "1.0",
        "status": "healthy",
        "checks": {"config_valid": True, "logger_ready": True},
        "runtime": dict(_RUNTIME_BLOCK),
    },
    "runtime_metrics": {
        "response_type": "runtime_metrics_response",
        "response_schema_version": "1.0",
        "runtime_metrics": {"startup_success": True, "runtime_status": "running"},
        "request_metrics": {
            "request_count_total": 0,
            "request_success_count": 0,
            "request_failure_count": 0,
        },
        "error_metrics": {"exception_count": 0, "failure_event_count": 0},
        "health_alignment": {
            "health_endpoint_status": "healthy",
            "health_is_healthy": True,
        },
    },
    "runtime_readiness": {
        "status": "ready",
        "checks": {"config_loaded": True, "bootstrap_complete": True},
    },
}


def make_input(endpoint: str) -> dict:
    return {"endpoint": endpoint, "payload": deepcopy(VALID_PAYLOADS[endpoint])}


# ---------------------------------------------------------------------------
# Pass path — all endpoints
# ---------------------------------------------------------------------------


class TestPassPath:
    @pytest.mark.parametrize("endpoint", sorted(SUPPORTED_ENDPOINTS))
    def test_valid_payload_passes(self, endpoint: str) -> None:
        result = validate_endpoint_schema(make_input(endpoint))

        assert result["status"] == "pass"
        assert result["errors"] == []


# ---------------------------------------------------------------------------
# Missing required keys
# ---------------------------------------------------------------------------


class TestMissingKeys:
    def test_missing_top_level_key_fails(self) -> None:
        inp = make_input("runtime_summary")
        del inp["payload"]["environment"]

        result = validate_endpoint_schema(inp)

        assert result["status"] == "fail"
        assert any("missing required key 'environment'" in e for e in result["errors"])

    def test_missing_nested_key_fails(self) -> None:
        inp = make_input("runtime_health")
        del inp["payload"]["checks"]["config_valid"]

        result = validate_endpoint_schema(inp)

        assert result["status"] == "fail"
        assert any("missing required key 'config_valid'" in e for e in result["errors"])

    def test_missing_readiness_status_key_fails(self) -> None:
        inp = make_input("runtime_readiness")
        del inp["payload"]["status"]

        result = validate_endpoint_schema(inp)

        assert result["status"] == "fail"
        assert any("missing required key 'status'" in e for e in result["errors"])

    def test_missing_metrics_runtime_metrics_key_fails(self) -> None:
        inp = make_input("runtime_metrics")
        del inp["payload"]["runtime_metrics"]["startup_success"]

        result = validate_endpoint_schema(inp)

        assert result["status"] == "fail"
        assert any("missing required key 'startup_success'" in e for e in result["errors"])


# ---------------------------------------------------------------------------
# Extra / unexpected keys
# ---------------------------------------------------------------------------


class TestExtraKeys:
    def test_extra_top_level_key_fails(self) -> None:
        inp = make_input("runtime_summary")
        inp["payload"]["injected"] = "value"

        result = validate_endpoint_schema(inp)

        assert result["status"] == "fail"
        assert any("unexpected key 'injected'" in e for e in result["errors"])

    def test_extra_nested_key_fails(self) -> None:
        inp = make_input("runtime_health")
        inp["payload"]["checks"]["extra_check"] = True

        result = validate_endpoint_schema(inp)

        assert result["status"] == "fail"
        assert any("unexpected key 'extra_check'" in e for e in result["errors"])

    def test_extra_readiness_checks_key_fails(self) -> None:
        inp = make_input("runtime_readiness")
        inp["payload"]["checks"]["db_ready"] = True

        result = validate_endpoint_schema(inp)

        assert result["status"] == "fail"
        assert any("unexpected key 'db_ready'" in e for e in result["errors"])


# ---------------------------------------------------------------------------
# Wrong value types
# ---------------------------------------------------------------------------


class TestWrongTypes:
    def test_api_port_as_string_fails(self) -> None:
        inp = make_input("runtime_summary")
        inp["payload"]["api_port"] = "8000"

        result = validate_endpoint_schema(inp)

        assert result["status"] == "fail"
        assert any("api_port" in e and "wrong type" in e for e in result["errors"])

    def test_debug_as_string_fails(self) -> None:
        inp = make_input("runtime_summary")
        inp["payload"]["debug"] = "false"

        result = validate_endpoint_schema(inp)

        assert result["status"] == "fail"
        assert any("debug" in e and "wrong type" in e for e in result["errors"])

    def test_bool_rejected_where_int_expected(self) -> None:
        inp = make_input("runtime_metrics")
        inp["payload"]["request_metrics"]["request_count_total"] = True

        result = validate_endpoint_schema(inp)

        assert result["status"] == "fail"
        assert any(
            "request_count_total" in e and "wrong type" in e for e in result["errors"]
        )

    def test_nested_bool_check_wrong_type_fails(self) -> None:
        inp = make_input("runtime_health")
        inp["payload"]["checks"]["config_valid"] = "yes"

        result = validate_endpoint_schema(inp)

        assert result["status"] == "fail"
        assert any(
            "config_valid" in e and "wrong type" in e for e in result["errors"]
        )

    def test_readiness_checks_bool_wrong_type_fails(self) -> None:
        inp = make_input("runtime_readiness")
        inp["payload"]["checks"]["config_loaded"] = 1

        result = validate_endpoint_schema(inp)

        assert result["status"] == "fail"
        assert any("config_loaded" in e and "wrong type" in e for e in result["errors"])


# ---------------------------------------------------------------------------
# Wrong key order
# ---------------------------------------------------------------------------


class TestWrongKeyOrder:
    def test_wrong_top_level_order_fails(self) -> None:
        correct = deepcopy(VALID_PAYLOADS["runtime_summary"])
        # rebuild with keys in wrong order
        shuffled = {
            "logger_level": correct["logger_level"],
            "environment": correct["environment"],
            "api_host": correct["api_host"],
            "api_port": correct["api_port"],
            "debug": correct["debug"],
            "log_level": correct["log_level"],
            "logger_name": correct["logger_name"],
        }

        result = validate_endpoint_schema({"endpoint": "runtime_summary", "payload": shuffled})

        assert result["status"] == "fail"
        assert any("wrong key order" in e for e in result["errors"])

    def test_wrong_nested_order_fails(self) -> None:
        inp = make_input("runtime_health")
        correct_checks = inp["payload"]["checks"]
        inp["payload"]["checks"] = {
            "logger_ready": correct_checks["logger_ready"],
            "config_valid": correct_checks["config_valid"],
        }

        result = validate_endpoint_schema(inp)

        assert result["status"] == "fail"
        assert any("wrong key order" in e and "checks" in e for e in result["errors"])

    def test_wrong_readiness_field_order_fails(self) -> None:
        payload = {
            "checks": {"config_loaded": True, "bootstrap_complete": True},
            "status": "ready",
        }

        result = validate_endpoint_schema({"endpoint": "runtime_readiness", "payload": payload})

        assert result["status"] == "fail"
        assert any("wrong key order" in e for e in result["errors"])


# ---------------------------------------------------------------------------
# Output contract
# ---------------------------------------------------------------------------


class TestOutputContract:
    def test_fixed_key_order_on_pass(self) -> None:
        result = validate_endpoint_schema(make_input("runtime_summary"))

        assert tuple(result.keys()) == ("status", "errors")

    def test_fixed_key_order_on_fail(self) -> None:
        inp = make_input("runtime_summary")
        del inp["payload"]["environment"]

        result = validate_endpoint_schema(inp)

        assert tuple(result.keys()) == ("status", "errors")

    def test_errors_is_list(self) -> None:
        result = validate_endpoint_schema(make_input("runtime_summary"))

        assert isinstance(result["errors"], list)

    def test_empty_errors_on_pass(self) -> None:
        result = validate_endpoint_schema(make_input("runtime_metrics"))

        assert result["errors"] == []


# ---------------------------------------------------------------------------
# Multiple error aggregation
# ---------------------------------------------------------------------------


class TestMultipleErrorAggregation:
    def test_multiple_missing_keys_all_reported(self) -> None:
        inp = make_input("runtime_summary")
        del inp["payload"]["environment"]
        del inp["payload"]["api_host"]
        del inp["payload"]["debug"]

        result = validate_endpoint_schema(inp)

        missing_errors = [e for e in result["errors"] if "missing required key" in e]
        assert len(missing_errors) == 3

    def test_multiple_extra_keys_all_reported(self) -> None:
        inp = make_input("runtime_summary")
        inp["payload"]["extra_one"] = "x"
        inp["payload"]["extra_two"] = "y"

        result = validate_endpoint_schema(inp)

        extra_errors = [e for e in result["errors"] if "unexpected key" in e]
        assert len(extra_errors) == 2

    def test_missing_and_type_errors_both_reported(self) -> None:
        inp = make_input("runtime_summary")
        del inp["payload"]["environment"]
        inp["payload"]["api_port"] = "not_an_int"

        result = validate_endpoint_schema(inp)

        assert any("missing required key 'environment'" in e for e in result["errors"])
        assert any("api_port" in e and "wrong type" in e for e in result["errors"])


# ---------------------------------------------------------------------------
# Determinism
# ---------------------------------------------------------------------------


class TestDeterminism:
    @pytest.mark.parametrize("endpoint", sorted(SUPPORTED_ENDPOINTS))
    def test_same_valid_input_always_passes(self, endpoint: str) -> None:
        first = validate_endpoint_schema(make_input(endpoint))
        second = validate_endpoint_schema(make_input(endpoint))

        assert first == second

    def test_same_invalid_input_produces_same_errors(self) -> None:
        inp = make_input("runtime_summary")
        del inp["payload"]["environment"]

        first = validate_endpoint_schema(inp)
        second = validate_endpoint_schema(deepcopy(inp))

        assert first == second


# ---------------------------------------------------------------------------
# Input immutability
# ---------------------------------------------------------------------------


class TestInputImmutability:
    def test_payload_not_mutated_on_pass(self) -> None:
        inp = make_input("runtime_readiness")
        payload_before = deepcopy(inp["payload"])

        validate_endpoint_schema(inp)

        assert inp["payload"] == payload_before

    def test_payload_not_mutated_on_fail(self) -> None:
        inp = make_input("runtime_summary")
        del inp["payload"]["environment"]
        payload_before = deepcopy(inp["payload"])

        validate_endpoint_schema(inp)

        assert inp["payload"] == payload_before


# ---------------------------------------------------------------------------
# Input type validation
# ---------------------------------------------------------------------------


class TestInputTypeValidation:
    def test_non_dict_input_raises(self) -> None:
        with pytest.raises(TypeError, match="validation_input must be a dict"):
            validate_endpoint_schema("not a dict")  # type: ignore[arg-type]

    def test_missing_endpoint_key_raises(self) -> None:
        with pytest.raises(ValueError, match="missing required key 'endpoint'"):
            validate_endpoint_schema({"payload": {}})

    def test_missing_payload_key_raises(self) -> None:
        with pytest.raises(ValueError, match="missing required key 'payload'"):
            validate_endpoint_schema({"endpoint": "runtime_summary"})

    def test_non_string_endpoint_raises(self) -> None:
        with pytest.raises(TypeError, match="'endpoint' must be a str"):
            validate_endpoint_schema({"endpoint": 123, "payload": {}})

    def test_non_dict_payload_raises(self) -> None:
        with pytest.raises(TypeError, match="'payload' must be a dict"):
            validate_endpoint_schema({"endpoint": "runtime_summary", "payload": "x"})

    def test_unknown_endpoint_raises(self) -> None:
        with pytest.raises(ValueError, match="unknown endpoint"):
            validate_endpoint_schema({"endpoint": "runtime_unknown", "payload": {}})
