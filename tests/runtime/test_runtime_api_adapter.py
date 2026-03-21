"""
Test suite for MB7-A Internal API Adapter Layer.

Validates:
- Closed-schema validation of api_request
- Customer eligibility enforcement
- Core behavior: correct field mapping, exact output schema
- Orchestration: correct call order and arguments
- Failure behavior: exceptions propagate correctly
- Safety: api_request not mutated
"""

from copy import deepcopy
from unittest.mock import patch

import pytest

from fleetgraph_core.runtime.execution_registry import ExecutionRegistry
from fleetgraph_core.runtime.runtime_api_adapter import apply_runtime_api_request

_PATCH_BUILD = "fleetgraph_core.runtime.runtime_api_adapter.build_schedule_request_from_template"
_PATCH_BOUNDARY = "fleetgraph_core.runtime.runtime_api_adapter.apply_runtime_failure_boundary"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def make_registry() -> ExecutionRegistry:
    return ExecutionRegistry()


def make_template(
    template_id: str = "tmpl_001",
    template_scope: str = "test_scope",
    default_schedule_id: str = "sched_001",
    default_schedule_scope: str = "sched_scope",
) -> dict:
    return {
        "template_id": template_id,
        "template_scope": template_scope,
        "default_schedule_id": default_schedule_id,
        "default_schedule_scope": default_schedule_scope,
    }


def make_record() -> dict:
    return {"canonical_organization_key": "org_a", "source_id": "src_1", "opportunity_rank": 1}


def make_api_request(
    request_id: str = "req_001",
    customer_id: str = "Sortimo",
    customer_type: str = "upfitter",
    runtime_template: dict | None = None,
    scheduled_batches: list | None = None,
) -> dict:
    if runtime_template is None:
        runtime_template = make_template()
    if scheduled_batches is None:
        scheduled_batches = [[make_record()]]
    return {
        "request_id": request_id,
        "customer_id": customer_id,
        "customer_type": customer_type,
        "runtime_template": runtime_template,
        "scheduled_batches": scheduled_batches,
    }


def make_completed_boundary_result(schedule_result: dict | None = None) -> dict:
    return {
        "boundary_state": "completed",
        "failure_category": None,
        "failure_message": None,
        "schedule_result": schedule_result or {"schedule_id": "sched_001"},
        "audit_report": {"audit_report_id": "audit:abc123"},
    }


def make_failed_boundary_result() -> dict:
    return {
        "boundary_state": "failed",
        "failure_category": "runtime_failure",
        "failure_message": "something went wrong",
        "schedule_result": None,
        "audit_report": None,
    }


def make_schedule_request() -> dict:
    return {
        "schedule_id": "sched_001",
        "schedule_scope": "sched_scope",
        "scheduled_batches": [[make_record()]],
    }


# ---------------------------------------------------------------------------
# API request validation
# ---------------------------------------------------------------------------


class TestAPIRequestValidation:
    """Closed-schema validation of api_request."""

    def test_non_dict_request_rejected(self):
        with pytest.raises(TypeError, match="api_request must be a dict"):
            apply_runtime_api_request("not a dict", make_registry())

    def test_non_dict_list_rejected(self):
        with pytest.raises(TypeError, match="api_request must be a dict"):
            apply_runtime_api_request(["req_001"], make_registry())

    def test_missing_request_id_rejected(self):
        req = make_api_request()
        del req["request_id"]

        with pytest.raises(ValueError, match="request_id"):
            apply_runtime_api_request(req, make_registry())

    def test_missing_runtime_template_rejected(self):
        req = make_api_request()
        del req["runtime_template"]

        with pytest.raises(ValueError, match="runtime_template"):
            apply_runtime_api_request(req, make_registry())

    def test_missing_scheduled_batches_rejected(self):
        req = make_api_request()
        del req["scheduled_batches"]

        with pytest.raises(ValueError, match="scheduled_batches"):
            apply_runtime_api_request(req, make_registry())

    def test_missing_customer_id_rejected(self):
        req = make_api_request()
        del req["customer_id"]

        with pytest.raises(ValueError, match="customer_id"):
            apply_runtime_api_request(req, make_registry())

    def test_missing_customer_type_rejected(self):
        req = make_api_request()
        del req["customer_type"]

        with pytest.raises(ValueError, match="customer_type"):
            apply_runtime_api_request(req, make_registry())

    def test_extra_field_rejected(self):
        req = make_api_request()
        req["extra_field"] = "not_allowed"

        with pytest.raises(ValueError, match="unexpected fields"):
            apply_runtime_api_request(req, make_registry())

    def test_wrong_type_request_id_rejected(self):
        req = make_api_request()
        req["request_id"] = 12345

        with pytest.raises(TypeError, match="request_id"):
            apply_runtime_api_request(req, make_registry())

    def test_empty_request_id_rejected(self):
        req = make_api_request(request_id="")

        with pytest.raises(ValueError, match="request_id"):
            apply_runtime_api_request(req, make_registry())

    def test_whitespace_only_request_id_rejected(self):
        req = make_api_request(request_id="   ")

        with pytest.raises(ValueError, match="request_id"):
            apply_runtime_api_request(req, make_registry())

    def test_wrong_type_runtime_template_rejected(self):
        req = make_api_request()
        req["runtime_template"] = "not_a_dict"

        with pytest.raises(TypeError, match="runtime_template"):
            apply_runtime_api_request(req, make_registry())

    def test_wrong_type_scheduled_batches_rejected(self):
        req = make_api_request()
        req["scheduled_batches"] = "not_a_list"

        with pytest.raises(TypeError, match="scheduled_batches"):
            apply_runtime_api_request(req, make_registry())

    def test_wrong_type_customer_id_rejected(self):
        req = make_api_request()
        req["customer_id"] = 123

        with pytest.raises(TypeError, match="customer_id"):
            apply_runtime_api_request(req, make_registry())

    def test_wrong_type_customer_type_rejected(self):
        req = make_api_request()
        req["customer_type"] = 123

        with pytest.raises(TypeError, match="customer_type"):
            apply_runtime_api_request(req, make_registry())

    def test_empty_customer_id_rejected(self):
        req = make_api_request(customer_id="")

        with pytest.raises(ValueError, match="customer_id"):
            apply_runtime_api_request(req, make_registry())

    def test_whitespace_only_customer_id_rejected(self):
        req = make_api_request(customer_id="  ")

        with pytest.raises(ValueError, match="customer_id"):
            apply_runtime_api_request(req, make_registry())

    def test_empty_customer_type_rejected(self):
        req = make_api_request(customer_type="")

        with pytest.raises(ValueError, match="customer_type"):
            apply_runtime_api_request(req, make_registry())

    def test_whitespace_only_customer_type_rejected(self):
        req = make_api_request(customer_type="\t")

        with pytest.raises(ValueError, match="customer_type"):
            apply_runtime_api_request(req, make_registry())


class TestEligibility:
    """Customer eligibility enforcement."""

    def test_non_upfitter_allowed(self):
        req = make_api_request(customer_id="AnyCustomer", customer_type="dealer")
        registry = make_registry()
        sched_req = make_schedule_request()
        boundary = make_completed_boundary_result()

        with patch(_PATCH_BUILD, return_value=sched_req) as mock_build, \
             patch(_PATCH_BOUNDARY, return_value=boundary) as mock_boundary:
            result = apply_runtime_api_request(req, registry)

        assert result["api_state"] == "completed"
        mock_build.assert_called_once()
        mock_boundary.assert_called_once()

    def test_sortimo_upfitter_allowed(self):
        req = make_api_request(customer_id="Sortimo", customer_type="upfitter")
        registry = make_registry()
        sched_req = make_schedule_request()
        boundary = make_completed_boundary_result()

        with patch(_PATCH_BUILD, return_value=sched_req) as mock_build, \
             patch(_PATCH_BOUNDARY, return_value=boundary) as mock_boundary:
            result = apply_runtime_api_request(req, registry)

        assert result["api_state"] == "completed"
        mock_build.assert_called_once()
        mock_boundary.assert_called_once()

    def test_non_sortimo_upfitter_rejected(self):
        req = make_api_request(customer_id="OtherBrand", customer_type="upfitter")

        with patch(_PATCH_BUILD) as mock_build, patch(_PATCH_BOUNDARY) as mock_boundary:
            with pytest.raises(ValueError, match="upfitter customers are restricted"):
                apply_runtime_api_request(req, make_registry())

        mock_build.assert_not_called()
        mock_boundary.assert_not_called()


# ---------------------------------------------------------------------------
# Core behavior
# ---------------------------------------------------------------------------


class TestCoreBehavior:
    """Nominal API response construction."""

    def test_valid_request_returns_dict(self):
        req = make_api_request()
        registry = make_registry()
        sched_req = make_schedule_request()
        boundary = make_completed_boundary_result()

        with patch(_PATCH_BUILD, return_value=sched_req), \
             patch(_PATCH_BOUNDARY, return_value=boundary):
            result = apply_runtime_api_request(req, registry)

        assert isinstance(result, dict)

    def test_output_has_exact_required_fields_only(self):
        req = make_api_request()
        registry = make_registry()
        sched_req = make_schedule_request()
        boundary = make_completed_boundary_result()

        with patch(_PATCH_BUILD, return_value=sched_req), \
             patch(_PATCH_BOUNDARY, return_value=boundary):
            result = apply_runtime_api_request(req, registry)

        assert set(result.keys()) == {"request_id", "api_state", "boundary_result"}

    def test_request_id_echoed_exactly(self):
        req = make_api_request(request_id="unique-req-xyz")
        registry = make_registry()
        sched_req = make_schedule_request()
        boundary = make_completed_boundary_result()

        with patch(_PATCH_BUILD, return_value=sched_req), \
             patch(_PATCH_BOUNDARY, return_value=boundary):
            result = apply_runtime_api_request(req, registry)

        assert result["request_id"] == "unique-req-xyz"

    def test_api_state_is_completed_on_completed_boundary(self):
        req = make_api_request()
        registry = make_registry()
        sched_req = make_schedule_request()
        boundary = make_completed_boundary_result()

        with patch(_PATCH_BUILD, return_value=sched_req), \
             patch(_PATCH_BOUNDARY, return_value=boundary):
            result = apply_runtime_api_request(req, registry)

        assert result["api_state"] == "completed"

    def test_api_state_is_failed_on_failed_boundary(self):
        req = make_api_request()
        registry = make_registry()
        sched_req = make_schedule_request()
        boundary = make_failed_boundary_result()

        with patch(_PATCH_BUILD, return_value=sched_req), \
             patch(_PATCH_BOUNDARY, return_value=boundary):
            result = apply_runtime_api_request(req, registry)

        assert result["api_state"] == "failed"

    def test_boundary_result_embedded_exactly(self):
        req = make_api_request()
        registry = make_registry()
        sched_req = make_schedule_request()
        boundary = make_completed_boundary_result()

        with patch(_PATCH_BUILD, return_value=sched_req), \
             patch(_PATCH_BOUNDARY, return_value=boundary):
            result = apply_runtime_api_request(req, registry)

        assert result["boundary_result"] is boundary


# ---------------------------------------------------------------------------
# Orchestration correctness
# ---------------------------------------------------------------------------


class TestOrchestrationCorrectness:
    """Call order and argument forwarding."""

    def test_build_schedule_request_called_once_with_exact_inputs(self):
        req = make_api_request()
        registry = make_registry()
        sched_req = make_schedule_request()
        boundary = make_completed_boundary_result()

        with patch(_PATCH_BUILD, return_value=sched_req) as mock_build, \
             patch(_PATCH_BOUNDARY, return_value=boundary):
            apply_runtime_api_request(req, registry)

        mock_build.assert_called_once_with(
            runtime_template=req["runtime_template"],
            scheduled_batches=req["scheduled_batches"],
        )

    def test_apply_runtime_failure_boundary_called_once_with_built_schedule_request_and_registry(self):
        req = make_api_request()
        registry = make_registry()
        sched_req = make_schedule_request()
        boundary = make_completed_boundary_result()

        with patch(_PATCH_BUILD, return_value=sched_req), \
             patch(_PATCH_BOUNDARY, return_value=boundary) as mock_boundary:
            apply_runtime_api_request(req, registry)

        mock_boundary.assert_called_once_with(
            schedule_request=sched_req,
            execution_registry=registry,
        )

    def test_execution_order_build_first_boundary_second(self):
        req = make_api_request()
        registry = make_registry()
        sched_req = make_schedule_request()
        boundary = make_completed_boundary_result()
        call_order = []

        def build_side_effect(**kwargs):
            call_order.append("build")
            return sched_req

        def boundary_side_effect(**kwargs):
            call_order.append("boundary")
            return boundary

        with patch(_PATCH_BUILD, side_effect=build_side_effect), \
             patch(_PATCH_BOUNDARY, side_effect=boundary_side_effect):
            apply_runtime_api_request(req, registry)

        assert call_order == ["build", "boundary"]


# ---------------------------------------------------------------------------
# Failure behavior
# ---------------------------------------------------------------------------


class TestFailureBehavior:
    """Exception propagation rules."""

    def test_template_build_exception_propagates_unchanged(self):
        req = make_api_request()
        registry = make_registry()

        with patch(_PATCH_BUILD, side_effect=ValueError("bad template")), \
             patch(_PATCH_BOUNDARY) as mock_boundary:
            with pytest.raises(ValueError, match="bad template"):
                apply_runtime_api_request(req, registry)

        mock_boundary.assert_not_called()

    def test_template_build_type_error_propagates_unchanged(self):
        req = make_api_request()
        registry = make_registry()

        with patch(_PATCH_BUILD, side_effect=TypeError("wrong type")), \
             patch(_PATCH_BOUNDARY) as mock_boundary:
            with pytest.raises(TypeError, match="wrong type"):
                apply_runtime_api_request(req, registry)

        mock_boundary.assert_not_called()

    def test_api_validation_error_raises_directly(self):
        req = make_api_request(request_id="")

        with patch(_PATCH_BUILD) as mock_build, \
             patch(_PATCH_BOUNDARY) as mock_boundary:
            with pytest.raises(ValueError, match="request_id"):
                apply_runtime_api_request(req, make_registry())

        mock_build.assert_not_called()
        mock_boundary.assert_not_called()

    def test_no_downstream_call_if_api_validation_fails(self):
        req = make_api_request()
        req["extra_field"] = "bad"

        with patch(_PATCH_BUILD) as mock_build, \
             patch(_PATCH_BOUNDARY) as mock_boundary:
            with pytest.raises(ValueError):
                apply_runtime_api_request(req, make_registry())

        mock_build.assert_not_called()
        mock_boundary.assert_not_called()

    def test_no_boundary_call_if_template_build_fails(self):
        req = make_api_request()
        registry = make_registry()

        with patch(_PATCH_BUILD, side_effect=ValueError("build failed")), \
             patch(_PATCH_BOUNDARY) as mock_boundary:
            with pytest.raises(ValueError):
                apply_runtime_api_request(req, registry)

        mock_boundary.assert_not_called()

    def test_eligibility_failure_raises_immediately(self):
        req = make_api_request(customer_id="NotSortimo", customer_type="upfitter")

        with patch(_PATCH_BUILD) as mock_build, patch(_PATCH_BOUNDARY) as mock_boundary:
            with pytest.raises(ValueError, match="upfitter customers are restricted"):
                apply_runtime_api_request(req, make_registry())

        mock_build.assert_not_called()
        mock_boundary.assert_not_called()

    def test_boundary_failure_mapped_to_failed_api_state(self):
        req = make_api_request(customer_id="Sortimo", customer_type="upfitter")
        registry = make_registry()
        sched_req = make_schedule_request()
        failed_boundary = make_failed_boundary_result()

        with patch(_PATCH_BUILD, return_value=sched_req), \
             patch(_PATCH_BOUNDARY, return_value=failed_boundary):
            result = apply_runtime_api_request(req, registry)

        assert result["api_state"] == "failed"
        assert result["boundary_result"] is failed_boundary


# ---------------------------------------------------------------------------
# Safety
# ---------------------------------------------------------------------------


class TestSafety:
    """Input mutation safety."""

    def test_api_request_not_mutated(self):
        req = make_api_request()
        original_copy = deepcopy(req)
        registry = make_registry()
        sched_req = make_schedule_request()
        boundary = make_completed_boundary_result()

        with patch(_PATCH_BUILD, return_value=sched_req), \
             patch(_PATCH_BOUNDARY, return_value=boundary):
            apply_runtime_api_request(req, registry)

        assert req == original_copy
