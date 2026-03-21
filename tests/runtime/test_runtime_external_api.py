"""
Test suite for MB10 External API Layer.
"""

from copy import deepcopy
from unittest.mock import patch

import pytest

from fleetgraph_core.runtime.execution_registry import ExecutionRegistry
from fleetgraph_core.runtime.runtime_external_api import handle_runtime_request

_PATCH_ADAPTER = "fleetgraph_core.runtime.runtime_external_api.apply_runtime_api_request"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def make_registry() -> ExecutionRegistry:
    return ExecutionRegistry()


def make_api_request() -> dict:
    return {
        "request_id": "req_001",
        "customer_id": "Sortimo",
        "customer_type": "upfitter",
        "runtime_template": {
            "template_id": "pb_001",
            "template_scope": "fleet_outbound",
            "default_schedule_id": "pb_001",
            "default_schedule_scope": "fleet_outbound",
        },
        "scheduled_batches": [[{"canonical_organization_key": "acme.com", "source_id": "a1", "opportunity_rank": 1}]],
    }


def make_request_envelope() -> dict:
    return {"request": make_api_request()}


def make_adapter_response() -> dict:
    return {
        "request_id": "req_001",
        "api_state": "completed",
        "boundary_result": {
            "boundary_state": "completed",
            "failure_category": None,
            "failure_message": None,
            "schedule_result": {"schedule_id": "pb_001"},
            "audit_report": {"audit_report_id": "audit:abc"},
        },
    }


# ---------------------------------------------------------------------------
# Envelope validation
# ---------------------------------------------------------------------------


class TestEnvelopeValidation:
    def test_non_dict_rejected(self):
        with pytest.raises(TypeError, match="request_envelope must be a dict"):
            handle_runtime_request("not a dict", make_registry())

    def test_missing_request_rejected(self):
        with pytest.raises(ValueError, match="request"):
            handle_runtime_request({}, make_registry())

    def test_extra_fields_rejected(self):
        envelope = make_request_envelope()
        envelope["extra"] = "x"

        with pytest.raises(ValueError, match="unexpected fields"):
            handle_runtime_request(envelope, make_registry())

    def test_request_not_dict_rejected(self):
        envelope = {"request": "not a dict"}

        with pytest.raises(TypeError, match="request"):
            handle_runtime_request(envelope, make_registry())


# ---------------------------------------------------------------------------
# Core behavior
# ---------------------------------------------------------------------------


class TestCoreBehavior:
    def test_valid_envelope_returns_wrapped_response(self):
        envelope = make_request_envelope()
        registry = make_registry()
        adapter_response = make_adapter_response()

        with patch(_PATCH_ADAPTER, return_value=adapter_response):
            result = handle_runtime_request(envelope, registry)

        assert result == {"response": adapter_response}

    def test_output_has_exact_fields_only(self):
        envelope = make_request_envelope()
        registry = make_registry()
        adapter_response = make_adapter_response()

        with patch(_PATCH_ADAPTER, return_value=adapter_response):
            result = handle_runtime_request(envelope, registry)

        assert set(result.keys()) == {"response"}

    def test_response_embedded_exactly(self):
        envelope = make_request_envelope()
        registry = make_registry()
        adapter_response = make_adapter_response()

        with patch(_PATCH_ADAPTER, return_value=adapter_response):
            result = handle_runtime_request(envelope, registry)

        assert result["response"] is adapter_response


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------


class TestOrchestration:
    def test_apply_runtime_api_request_called_once(self):
        envelope = make_request_envelope()
        registry = make_registry()
        adapter_response = make_adapter_response()

        with patch(_PATCH_ADAPTER, return_value=adapter_response) as mock_adapter:
            handle_runtime_request(envelope, registry)

        mock_adapter.assert_called_once()

    def test_apply_runtime_api_request_called_with_correct_inputs(self):
        envelope = make_request_envelope()
        registry = make_registry()
        adapter_response = make_adapter_response()

        with patch(_PATCH_ADAPTER, return_value=adapter_response) as mock_adapter:
            handle_runtime_request(envelope, registry)

        mock_adapter.assert_called_once_with(
            api_request=envelope["request"],
            execution_registry=registry,
        )


# ---------------------------------------------------------------------------
# Failure behavior
# ---------------------------------------------------------------------------


class TestFailureBehavior:
    def test_api_adapter_exception_propagates_unchanged(self):
        envelope = make_request_envelope()

        with patch(_PATCH_ADAPTER, side_effect=ValueError("adapter failed")):
            with pytest.raises(ValueError, match="adapter failed"):
                handle_runtime_request(envelope, make_registry())

    def test_no_exception_wrapping(self):
        envelope = make_request_envelope()

        with patch(_PATCH_ADAPTER, side_effect=RuntimeError("boom")):
            with pytest.raises(RuntimeError, match="boom"):
                handle_runtime_request(envelope, make_registry())


# ---------------------------------------------------------------------------
# Safety
# ---------------------------------------------------------------------------


class TestSafety:
    def test_input_envelope_not_mutated(self):
        envelope = make_request_envelope()
        envelope_before = deepcopy(envelope)
        adapter_response = make_adapter_response()

        with patch(_PATCH_ADAPTER, return_value=adapter_response):
            handle_runtime_request(envelope, make_registry())

        assert envelope == envelope_before
