"""
Test suite for MB3 Runtime Scheduler / Orchestration Layer.

Validates:
- Closed-schema input validation (missing fields, extra fields, wrong types,
  empty / whitespace-only strings, non-list batch entries)
- Core execution: single batch, multiple batches, empty schedule
- Output contract: all required fields, correct counts, embedded results
- Registry integration: explicit registration after success, duplicate
  propagation, call-count enforcement, no premature registration
- Input safety: schedule_request not mutated, exceptions propagated unchanged

Mocking boundary: apply_runtime_execution patched in runtime_scheduler module.
"""

from copy import deepcopy
from unittest.mock import call, patch

import pytest

from fleetgraph_core.runtime.execution_registry import ExecutionRegistry
from fleetgraph_core.runtime.runtime_scheduler import apply_runtime_schedule


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def execution_registry():
    """Fresh execution registry for each test."""
    return ExecutionRegistry()


@pytest.fixture
def valid_schedule_request():
    """Valid schedule request with two distinct batches."""
    batch1 = [
        {
            "canonical_organization_key": "org_a",
            "source_id": "src_1",
            "opportunity_rank": 1,
        }
    ]
    batch2 = [
        {
            "canonical_organization_key": "org_b",
            "source_id": "src_2",
            "opportunity_rank": 2,
        }
    ]
    return {
        "schedule_id": "sched_001",
        "schedule_scope": "test_scope",
        "scheduled_batches": [batch1, batch2],
    }


@pytest.fixture
def single_batch_schedule_request():
    """Valid schedule request with exactly one batch."""
    return {
        "schedule_id": "sched_single",
        "schedule_scope": "single_scope",
        "scheduled_batches": [
            [
                {
                    "canonical_organization_key": "org_x",
                    "source_id": "src_x",
                    "opportunity_rank": 1,
                }
            ]
        ],
    }


@pytest.fixture
def empty_schedule_request():
    """Valid schedule request with no batches."""
    return {
        "schedule_id": "sched_empty",
        "schedule_scope": "empty_scope",
        "scheduled_batches": [],
    }


# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------


def make_runtime_result(run_id: str, batch_size: int = 1) -> dict:
    """Return a minimal but structurally correct runtime result envelope."""
    return {
        "run_id": run_id,
        "runtime_state": "completed",
        "workflow_result": {
            "workflow_run_id": f"wf_{run_id}",
            "final_results": [{"id": str(i)} for i in range(batch_size)],
        },
        "input_record_count": batch_size,
        "output_record_count": batch_size,
    }


# ---------------------------------------------------------------------------
# Validation tests
# ---------------------------------------------------------------------------


class TestScheduleRequestValidation:
    """Closed-schema validation of schedule_request."""

    def test_non_dict_schedule_request_rejected(self, execution_registry):
        with pytest.raises(TypeError, match="schedule_request must be a dict"):
            apply_runtime_schedule("not a dict", execution_registry)

        with pytest.raises(TypeError, match="schedule_request must be a dict"):
            apply_runtime_schedule(["sched_1", "scope", []], execution_registry)

    def test_missing_schedule_id_rejected(self, execution_registry):
        request = {"schedule_scope": "scope", "scheduled_batches": []}

        with pytest.raises(ValueError, match="schedule_id"):
            apply_runtime_schedule(request, execution_registry)

    def test_missing_schedule_scope_rejected(self, execution_registry):
        request = {"schedule_id": "sched_1", "scheduled_batches": []}

        with pytest.raises(ValueError, match="schedule_scope"):
            apply_runtime_schedule(request, execution_registry)

    def test_missing_scheduled_batches_rejected(self, execution_registry):
        request = {"schedule_id": "sched_1", "schedule_scope": "scope"}

        with pytest.raises(ValueError, match="scheduled_batches"):
            apply_runtime_schedule(request, execution_registry)

    def test_extra_field_rejected(self, execution_registry):
        request = {
            "schedule_id": "sched_1",
            "schedule_scope": "scope",
            "scheduled_batches": [],
            "extra_field": "not_allowed",
        }

        with pytest.raises(ValueError, match="unexpected fields"):
            apply_runtime_schedule(request, execution_registry)

    def test_empty_schedule_id_rejected(self, execution_registry):
        request = {"schedule_id": "", "schedule_scope": "scope", "scheduled_batches": []}

        with pytest.raises(ValueError, match="schedule_id"):
            apply_runtime_schedule(request, execution_registry)

    def test_whitespace_only_schedule_id_rejected(self, execution_registry):
        request = {"schedule_id": "   ", "schedule_scope": "scope", "scheduled_batches": []}

        with pytest.raises(ValueError, match="schedule_id"):
            apply_runtime_schedule(request, execution_registry)

    def test_empty_schedule_scope_rejected(self, execution_registry):
        request = {"schedule_id": "sched_1", "schedule_scope": "", "scheduled_batches": []}

        with pytest.raises(ValueError, match="schedule_scope"):
            apply_runtime_schedule(request, execution_registry)

    def test_whitespace_only_schedule_scope_rejected(self, execution_registry):
        request = {"schedule_id": "sched_1", "schedule_scope": "\t", "scheduled_batches": []}

        with pytest.raises(ValueError, match="schedule_scope"):
            apply_runtime_schedule(request, execution_registry)

    def test_non_list_scheduled_batches_rejected(self, execution_registry):
        request = {
            "schedule_id": "sched_1",
            "schedule_scope": "scope",
            "scheduled_batches": {},
        }

        with pytest.raises(TypeError, match="scheduled_batches must be a list"):
            apply_runtime_schedule(request, execution_registry)

    def test_non_list_batch_entry_rejected(self, execution_registry):
        request = {
            "schedule_id": "sched_1",
            "schedule_scope": "scope",
            "scheduled_batches": [{"not": "a list"}],
        }

        with pytest.raises(TypeError, match=r"scheduled_batches\[0\] must be a list"):
            apply_runtime_schedule(request, execution_registry)


# ---------------------------------------------------------------------------
# Core execution tests
# ---------------------------------------------------------------------------


class TestCoreExecution:
    """Nominal schedule execution behaviour."""

    @patch("fleetgraph_core.runtime.runtime_scheduler.apply_runtime_execution")
    def test_single_batch_executes_successfully(
        self,
        mock_execute,
        single_batch_schedule_request,
        execution_registry,
    ):
        mock_execute.return_value = make_runtime_result("run_001")

        result = apply_runtime_schedule(single_batch_schedule_request, execution_registry)

        assert isinstance(result, dict)
        assert result["schedule_state"] == "completed"
        mock_execute.assert_called_once()

    @patch("fleetgraph_core.runtime.runtime_scheduler.apply_runtime_execution")
    def test_multiple_batches_execute_in_exact_input_order(
        self,
        mock_execute,
        valid_schedule_request,
        execution_registry,
    ):
        mock_execute.side_effect = [
            make_runtime_result("run_001"),
            make_runtime_result("run_002"),
        ]
        batch1 = valid_schedule_request["scheduled_batches"][0]
        batch2 = valid_schedule_request["scheduled_batches"][1]

        apply_runtime_schedule(valid_schedule_request, execution_registry)

        assert mock_execute.call_count == 2
        assert mock_execute.call_args_list[0] == call(
            records=batch1, execution_registry=execution_registry
        )
        assert mock_execute.call_args_list[1] == call(
            records=batch2, execution_registry=execution_registry
        )

    @patch("fleetgraph_core.runtime.runtime_scheduler.apply_runtime_execution")
    def test_empty_schedule_returns_valid_completed_summary(
        self,
        mock_execute,
        empty_schedule_request,
        execution_registry,
    ):
        result = apply_runtime_schedule(empty_schedule_request, execution_registry)

        assert result["schedule_state"] == "completed"
        assert result["scheduled_batch_count"] == 0
        assert result["completed_batch_count"] == 0
        assert result["runtime_results"] == []
        mock_execute.assert_not_called()

    @patch("fleetgraph_core.runtime.runtime_scheduler.apply_runtime_execution")
    def test_scheduled_batch_count_correct(
        self,
        mock_execute,
        valid_schedule_request,
        execution_registry,
    ):
        mock_execute.side_effect = [
            make_runtime_result("run_001"),
            make_runtime_result("run_002"),
        ]

        result = apply_runtime_schedule(valid_schedule_request, execution_registry)

        assert result["scheduled_batch_count"] == 2

    @patch("fleetgraph_core.runtime.runtime_scheduler.apply_runtime_execution")
    def test_completed_batch_count_correct(
        self,
        mock_execute,
        valid_schedule_request,
        execution_registry,
    ):
        mock_execute.side_effect = [
            make_runtime_result("run_001"),
            make_runtime_result("run_002"),
        ]

        result = apply_runtime_schedule(valid_schedule_request, execution_registry)

        assert result["completed_batch_count"] == 2

    @patch("fleetgraph_core.runtime.runtime_scheduler.apply_runtime_execution")
    def test_runtime_results_embedded_exactly(
        self,
        mock_execute,
        valid_schedule_request,
        execution_registry,
    ):
        result1 = make_runtime_result("run_001")
        result2 = make_runtime_result("run_002")
        mock_execute.side_effect = [result1, result2]

        result = apply_runtime_schedule(valid_schedule_request, execution_registry)

        assert result["runtime_results"] == [result1, result2]

    @patch("fleetgraph_core.runtime.runtime_scheduler.apply_runtime_execution")
    def test_result_has_all_required_fields_only(
        self,
        mock_execute,
        valid_schedule_request,
        execution_registry,
    ):
        mock_execute.side_effect = [
            make_runtime_result("run_001"),
            make_runtime_result("run_002"),
        ]

        result = apply_runtime_schedule(valid_schedule_request, execution_registry)

        assert set(result.keys()) == {
            "schedule_id",
            "schedule_scope",
            "schedule_state",
            "scheduled_batch_count",
            "completed_batch_count",
            "runtime_results",
        }

    @patch("fleetgraph_core.runtime.runtime_scheduler.apply_runtime_execution")
    def test_schedule_id_and_scope_echoed_in_result(
        self,
        mock_execute,
        single_batch_schedule_request,
        execution_registry,
    ):
        mock_execute.return_value = make_runtime_result("run_001")

        result = apply_runtime_schedule(single_batch_schedule_request, execution_registry)

        assert result["schedule_id"] == "sched_single"
        assert result["schedule_scope"] == "single_scope"


# ---------------------------------------------------------------------------
# Registry integration tests
# ---------------------------------------------------------------------------


class TestRegistryIntegration:
    """Execution registry interaction within the scheduler."""

    @patch("fleetgraph_core.runtime.runtime_scheduler.apply_runtime_execution")
    def test_scheduler_registers_each_successful_run_id(
        self,
        mock_execute,
        valid_schedule_request,
        execution_registry,
    ):
        mock_execute.side_effect = [
            make_runtime_result("run_001"),
            make_runtime_result("run_002"),
        ]

        apply_runtime_schedule(valid_schedule_request, execution_registry)

        assert execution_registry.has_run("run_001")
        assert execution_registry.has_run("run_002")
        assert len(execution_registry) == 2

    @patch("fleetgraph_core.runtime.runtime_scheduler.apply_runtime_execution")
    def test_duplicate_on_later_batch_propagates_and_stops_schedule(
        self,
        mock_execute,
        valid_schedule_request,
        execution_registry,
    ):
        mock_execute.side_effect = [
            make_runtime_result("run_001"),
            ValueError(
                "Duplicate execution detected. Run run_001 has already been executed."
            ),
        ]

        with pytest.raises(ValueError, match="Duplicate execution detected"):
            apply_runtime_schedule(valid_schedule_request, execution_registry)

    @patch("fleetgraph_core.runtime.runtime_scheduler.apply_runtime_execution")
    def test_runtime_execution_called_once_per_attempted_batch(
        self,
        mock_execute,
        valid_schedule_request,
        execution_registry,
    ):
        mock_execute.side_effect = [
            make_runtime_result("run_001"),
            ValueError("Duplicate execution detected."),
        ]

        with pytest.raises(ValueError):
            apply_runtime_schedule(valid_schedule_request, execution_registry)

        assert mock_execute.call_count == 2

    @patch("fleetgraph_core.runtime.runtime_scheduler.apply_runtime_execution")
    def test_no_auto_registration_before_successful_result(
        self,
        mock_execute,
        single_batch_schedule_request,
        execution_registry,
    ):
        mock_execute.side_effect = RuntimeError("Workflow failed before result")

        with pytest.raises(RuntimeError):
            apply_runtime_schedule(single_batch_schedule_request, execution_registry)

        assert len(execution_registry) == 0

    @patch("fleetgraph_core.runtime.runtime_scheduler.apply_runtime_execution")
    def test_only_completed_batches_registered_on_partial_failure(
        self,
        mock_execute,
        valid_schedule_request,
        execution_registry,
    ):
        """First batch succeeds and is registered; second batch fails."""
        mock_execute.side_effect = [
            make_runtime_result("run_001"),
            ValueError("Duplicate execution detected."),
        ]

        with pytest.raises(ValueError):
            apply_runtime_schedule(valid_schedule_request, execution_registry)

        assert execution_registry.has_run("run_001")
        assert len(execution_registry) == 1


# ---------------------------------------------------------------------------
# Safety tests
# ---------------------------------------------------------------------------


class TestSafety:
    """Input safety and exception propagation."""

    @patch("fleetgraph_core.runtime.runtime_scheduler.apply_runtime_execution")
    def test_schedule_request_not_mutated(
        self,
        mock_execute,
        valid_schedule_request,
        execution_registry,
    ):
        mock_execute.side_effect = [
            make_runtime_result("run_001"),
            make_runtime_result("run_002"),
        ]
        original_copy = deepcopy(valid_schedule_request)

        apply_runtime_schedule(valid_schedule_request, execution_registry)

        assert valid_schedule_request == original_copy

    @patch("fleetgraph_core.runtime.runtime_scheduler.apply_runtime_execution")
    def test_propagated_runtime_exception_unchanged(
        self,
        mock_execute,
        single_batch_schedule_request,
        execution_registry,
    ):
        mock_execute.side_effect = RuntimeError("Downstream pipeline failure")

        with pytest.raises(RuntimeError, match="Downstream pipeline failure"):
            apply_runtime_schedule(single_batch_schedule_request, execution_registry)

    @patch("fleetgraph_core.runtime.runtime_scheduler.apply_runtime_execution")
    def test_propagated_value_error_unchanged(
        self,
        mock_execute,
        single_batch_schedule_request,
        execution_registry,
    ):
        mock_execute.side_effect = ValueError(
            "Duplicate execution detected. Run xyz has already been executed."
        )

        with pytest.raises(ValueError, match="Duplicate execution detected"):
            apply_runtime_schedule(single_batch_schedule_request, execution_registry)
