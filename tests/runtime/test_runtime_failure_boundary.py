"""
Test suite for MB5 Failure Boundary Layer.

Validates:
- Success path: both downstream functions called in order; exact output contract
- Failure classification: validation_error, duplicate_execution, runtime_failure,
  audit_failure
- Failure output contract: exact fields, None schedule_result / audit_report
- failure_message preserves original exception text
- audit not called when scheduler fails
- schedule_request not mutated

Patch boundary:
    fleetgraph_core.runtime.runtime_failure_boundary.apply_runtime_schedule
    fleetgraph_core.runtime.runtime_failure_boundary.build_runtime_audit_report
"""

from copy import deepcopy
from unittest.mock import MagicMock, call, patch

import pytest

from fleetgraph_core.runtime.execution_registry import ExecutionRegistry
from fleetgraph_core.runtime.runtime_failure_boundary import apply_runtime_failure_boundary

_SCHED_PATH = "fleetgraph_core.runtime.runtime_failure_boundary.apply_runtime_schedule"
_AUDIT_PATH = "fleetgraph_core.runtime.runtime_failure_boundary.build_runtime_audit_report"


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def execution_registry():
    return ExecutionRegistry()


@pytest.fixture
def valid_schedule_request():
    return {
        "schedule_id": "sched_001",
        "schedule_scope": "test_scope",
        "scheduled_batches": [],
    }


def make_schedule_result() -> dict:
    return {
        "schedule_id": "sched_001",
        "schedule_scope": "test_scope",
        "schedule_state": "completed",
        "scheduled_batch_count": 0,
        "completed_batch_count": 0,
        "runtime_results": [],
    }


def make_audit_report() -> dict:
    return {
        "audit_report_id": "audit:abc123",
        "schedule_id": "sched_001",
        "schedule_scope": "test_scope",
        "schedule_state": "completed",
        "scheduled_batch_count": 0,
        "completed_batch_count": 0,
        "runtime_result_count": 0,
        "total_input_record_count": 0,
        "total_output_record_count": 0,
        "audited_run_ids": [],
        "runtime_audit_entries": [],
    }


# ---------------------------------------------------------------------------
# Success path
# ---------------------------------------------------------------------------


class TestSuccessPath:
    """Nominal success: both downstream functions succeed."""

    @patch(_AUDIT_PATH)
    @patch(_SCHED_PATH)
    def test_successful_execution_returns_completed_boundary_result(
        self,
        mock_schedule,
        mock_audit,
        valid_schedule_request,
        execution_registry,
    ):
        mock_schedule.return_value = make_schedule_result()
        mock_audit.return_value = make_audit_report()

        result = apply_runtime_failure_boundary(valid_schedule_request, execution_registry)

        assert result["boundary_state"] == "completed"

    @patch(_AUDIT_PATH)
    @patch(_SCHED_PATH)
    def test_success_result_has_exact_required_fields_only(
        self,
        mock_schedule,
        mock_audit,
        valid_schedule_request,
        execution_registry,
    ):
        mock_schedule.return_value = make_schedule_result()
        mock_audit.return_value = make_audit_report()

        result = apply_runtime_failure_boundary(valid_schedule_request, execution_registry)

        assert set(result.keys()) == {
            "boundary_state",
            "failure_category",
            "failure_message",
            "schedule_result",
            "audit_report",
        }

    @patch(_AUDIT_PATH)
    @patch(_SCHED_PATH)
    def test_success_result_failure_fields_are_none(
        self,
        mock_schedule,
        mock_audit,
        valid_schedule_request,
        execution_registry,
    ):
        mock_schedule.return_value = make_schedule_result()
        mock_audit.return_value = make_audit_report()

        result = apply_runtime_failure_boundary(valid_schedule_request, execution_registry)

        assert result["failure_category"] is None
        assert result["failure_message"] is None

    @patch(_AUDIT_PATH)
    @patch(_SCHED_PATH)
    def test_schedule_result_embedded_exactly(
        self,
        mock_schedule,
        mock_audit,
        valid_schedule_request,
        execution_registry,
    ):
        sched = make_schedule_result()
        mock_schedule.return_value = sched
        mock_audit.return_value = make_audit_report()

        result = apply_runtime_failure_boundary(valid_schedule_request, execution_registry)

        assert result["schedule_result"] == sched

    @patch(_AUDIT_PATH)
    @patch(_SCHED_PATH)
    def test_audit_report_embedded_exactly(
        self,
        mock_schedule,
        mock_audit,
        valid_schedule_request,
        execution_registry,
    ):
        mock_schedule.return_value = make_schedule_result()
        audit = make_audit_report()
        mock_audit.return_value = audit

        result = apply_runtime_failure_boundary(valid_schedule_request, execution_registry)

        assert result["audit_report"] == audit

    @patch(_AUDIT_PATH)
    @patch(_SCHED_PATH)
    def test_both_downstream_functions_called_exactly_once_in_order(
        self,
        mock_schedule,
        mock_audit,
        valid_schedule_request,
        execution_registry,
    ):
        sched = make_schedule_result()
        mock_schedule.return_value = sched
        mock_audit.return_value = make_audit_report()

        apply_runtime_failure_boundary(valid_schedule_request, execution_registry)

        mock_schedule.assert_called_once_with(valid_schedule_request, execution_registry)
        mock_audit.assert_called_once_with(sched)


# ---------------------------------------------------------------------------
# Failure classification
# ---------------------------------------------------------------------------


class TestFailureClassification:
    """Deterministic failure category mapping."""

    @patch(_AUDIT_PATH)
    @patch(_SCHED_PATH)
    def test_scheduler_type_error_becomes_validation_error(
        self,
        mock_schedule,
        mock_audit,
        valid_schedule_request,
        execution_registry,
    ):
        mock_schedule.side_effect = TypeError("schedule_request must be a dict")

        result = apply_runtime_failure_boundary(valid_schedule_request, execution_registry)

        assert result["boundary_state"] == "failed"
        assert result["failure_category"] == "validation_error"

    @patch(_AUDIT_PATH)
    @patch(_SCHED_PATH)
    def test_scheduler_generic_value_error_becomes_validation_error(
        self,
        mock_schedule,
        mock_audit,
        valid_schedule_request,
        execution_registry,
    ):
        mock_schedule.side_effect = ValueError("schedule_id must not be empty")

        result = apply_runtime_failure_boundary(valid_schedule_request, execution_registry)

        assert result["boundary_state"] == "failed"
        assert result["failure_category"] == "validation_error"

    @patch(_AUDIT_PATH)
    @patch(_SCHED_PATH)
    def test_scheduler_duplicate_detected_value_error_becomes_duplicate_execution(
        self,
        mock_schedule,
        mock_audit,
        valid_schedule_request,
        execution_registry,
    ):
        mock_schedule.side_effect = ValueError(
            "Duplicate execution detected. Run run_001 has already been executed."
        )

        result = apply_runtime_failure_boundary(valid_schedule_request, execution_registry)

        assert result["boundary_state"] == "failed"
        assert result["failure_category"] == "duplicate_execution"

    @patch(_AUDIT_PATH)
    @patch(_SCHED_PATH)
    def test_scheduler_duplicate_run_id_detected_message_becomes_duplicate_execution(
        self,
        mock_schedule,
        mock_audit,
        valid_schedule_request,
        execution_registry,
    ):
        mock_schedule.side_effect = ValueError("duplicate run_id detected for run_xyz")

        result = apply_runtime_failure_boundary(valid_schedule_request, execution_registry)

        assert result["boundary_state"] == "failed"
        assert result["failure_category"] == "duplicate_execution"

    @patch(_AUDIT_PATH)
    @patch(_SCHED_PATH)
    def test_scheduler_runtime_error_becomes_runtime_failure(
        self,
        mock_schedule,
        mock_audit,
        valid_schedule_request,
        execution_registry,
    ):
        mock_schedule.side_effect = RuntimeError("Downstream pipeline failure")

        result = apply_runtime_failure_boundary(valid_schedule_request, execution_registry)

        assert result["boundary_state"] == "failed"
        assert result["failure_category"] == "runtime_failure"

    @patch(_AUDIT_PATH)
    @patch(_SCHED_PATH)
    def test_scheduler_custom_exception_becomes_runtime_failure(
        self,
        mock_schedule,
        mock_audit,
        valid_schedule_request,
        execution_registry,
    ):
        class CustomPipelineError(Exception):
            pass

        mock_schedule.side_effect = CustomPipelineError("Something went wrong")

        result = apply_runtime_failure_boundary(valid_schedule_request, execution_registry)

        assert result["boundary_state"] == "failed"
        assert result["failure_category"] == "runtime_failure"

    @patch(_AUDIT_PATH)
    @patch(_SCHED_PATH)
    def test_audit_failure_becomes_audit_failure_category(
        self,
        mock_schedule,
        mock_audit,
        valid_schedule_request,
        execution_registry,
    ):
        mock_schedule.return_value = make_schedule_result()
        mock_audit.side_effect = ValueError("schedule_result is missing required fields")

        result = apply_runtime_failure_boundary(valid_schedule_request, execution_registry)

        assert result["boundary_state"] == "failed"
        assert result["failure_category"] == "audit_failure"

    @patch(_AUDIT_PATH)
    @patch(_SCHED_PATH)
    def test_audit_runtime_error_becomes_audit_failure(
        self,
        mock_schedule,
        mock_audit,
        valid_schedule_request,
        execution_registry,
    ):
        mock_schedule.return_value = make_schedule_result()
        mock_audit.side_effect = RuntimeError("Unexpected audit crash")

        result = apply_runtime_failure_boundary(valid_schedule_request, execution_registry)

        assert result["boundary_state"] == "failed"
        assert result["failure_category"] == "audit_failure"


# ---------------------------------------------------------------------------
# Failure output contract
# ---------------------------------------------------------------------------


class TestFailureOutputContract:
    """Closed-schema enforcement on the failure result."""

    @patch(_AUDIT_PATH)
    @patch(_SCHED_PATH)
    def test_failed_result_has_exact_required_fields_only(
        self,
        mock_schedule,
        mock_audit,
        valid_schedule_request,
        execution_registry,
    ):
        mock_schedule.side_effect = RuntimeError("Pipeline failure")

        result = apply_runtime_failure_boundary(valid_schedule_request, execution_registry)

        assert set(result.keys()) == {
            "boundary_state",
            "failure_category",
            "failure_message",
            "schedule_result",
            "audit_report",
        }

    @patch(_AUDIT_PATH)
    @patch(_SCHED_PATH)
    def test_failure_result_sets_schedule_result_to_none(
        self,
        mock_schedule,
        mock_audit,
        valid_schedule_request,
        execution_registry,
    ):
        mock_schedule.side_effect = RuntimeError("Pipeline failure")

        result = apply_runtime_failure_boundary(valid_schedule_request, execution_registry)

        assert result["schedule_result"] is None

    @patch(_AUDIT_PATH)
    @patch(_SCHED_PATH)
    def test_failure_result_sets_audit_report_to_none(
        self,
        mock_schedule,
        mock_audit,
        valid_schedule_request,
        execution_registry,
    ):
        mock_schedule.side_effect = RuntimeError("Pipeline failure")

        result = apply_runtime_failure_boundary(valid_schedule_request, execution_registry)

        assert result["audit_report"] is None

    @patch(_AUDIT_PATH)
    @patch(_SCHED_PATH)
    def test_failure_message_preserves_original_exception_text(
        self,
        mock_schedule,
        mock_audit,
        valid_schedule_request,
        execution_registry,
    ):
        mock_schedule.side_effect = RuntimeError("Exact failure message from pipeline")

        result = apply_runtime_failure_boundary(valid_schedule_request, execution_registry)

        assert result["failure_message"] == "Exact failure message from pipeline"

    @patch(_AUDIT_PATH)
    @patch(_SCHED_PATH)
    def test_audit_failure_message_preserves_original_exception_text(
        self,
        mock_schedule,
        mock_audit,
        valid_schedule_request,
        execution_registry,
    ):
        mock_schedule.return_value = make_schedule_result()
        mock_audit.side_effect = ValueError("audit schema mismatch")

        result = apply_runtime_failure_boundary(valid_schedule_request, execution_registry)

        assert result["failure_message"] == "audit schema mismatch"

    @patch(_AUDIT_PATH)
    @patch(_SCHED_PATH)
    def test_duplicate_failure_message_preserved(
        self,
        mock_schedule,
        mock_audit,
        valid_schedule_request,
        execution_registry,
    ):
        original_msg = "Duplicate execution detected. Run run_001 has already been executed."
        mock_schedule.side_effect = ValueError(original_msg)

        result = apply_runtime_failure_boundary(valid_schedule_request, execution_registry)

        assert result["failure_message"] == original_msg


# ---------------------------------------------------------------------------
# Safety
# ---------------------------------------------------------------------------


class TestSafety:
    """Ordering and input safety guarantees."""

    @patch(_AUDIT_PATH)
    @patch(_SCHED_PATH)
    def test_audit_not_called_when_scheduler_fails(
        self,
        mock_schedule,
        mock_audit,
        valid_schedule_request,
        execution_registry,
    ):
        mock_schedule.side_effect = RuntimeError("Scheduler crashed")

        apply_runtime_failure_boundary(valid_schedule_request, execution_registry)

        mock_audit.assert_not_called()

    @patch(_AUDIT_PATH)
    @patch(_SCHED_PATH)
    def test_scheduler_called_before_audit_on_success_path(
        self,
        mock_schedule,
        mock_audit,
        valid_schedule_request,
        execution_registry,
    ):
        call_order = []
        sched_result = make_schedule_result()

        mock_schedule.side_effect = lambda *a, **kw: call_order.append("schedule") or sched_result
        mock_audit.side_effect = lambda *a, **kw: call_order.append("audit") or make_audit_report()

        apply_runtime_failure_boundary(valid_schedule_request, execution_registry)

        assert call_order == ["schedule", "audit"]

    @patch(_AUDIT_PATH)
    @patch(_SCHED_PATH)
    def test_schedule_request_not_mutated(
        self,
        mock_schedule,
        mock_audit,
        valid_schedule_request,
        execution_registry,
    ):
        mock_schedule.return_value = make_schedule_result()
        mock_audit.return_value = make_audit_report()
        original_copy = deepcopy(valid_schedule_request)

        apply_runtime_failure_boundary(valid_schedule_request, execution_registry)

        assert valid_schedule_request == original_copy

    @patch(_AUDIT_PATH)
    @patch(_SCHED_PATH)
    def test_schedule_request_not_mutated_on_failure(
        self,
        mock_schedule,
        mock_audit,
        valid_schedule_request,
        execution_registry,
    ):
        mock_schedule.side_effect = RuntimeError("crashed")
        original_copy = deepcopy(valid_schedule_request)

        apply_runtime_failure_boundary(valid_schedule_request, execution_registry)

        assert valid_schedule_request == original_copy
