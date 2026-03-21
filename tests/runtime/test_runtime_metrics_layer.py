"""
Test suite for MB9 Runtime Metrics / Analytics Layer.
"""

from copy import deepcopy

import pytest

from fleetgraph_core.runtime.runtime_metrics_layer import build_runtime_metrics_report


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def make_schedule_result() -> dict:
    return {
        "schedule_id": "sched_001",
        "schedule_scope": "fleet_outbound",
        "schedule_state": "completed",
        "scheduled_batch_count": 3,
        "completed_batch_count": 3,
        "runtime_results": [
            {"run_id": "r1"},
            {"run_id": "r2"},
            {"run_id": "r3"},
        ],
    }


def make_audit_report() -> dict:
    return {
        "audit_report_id": "audit:abc123",
        "schedule_id": "sched_001",
        "schedule_scope": "fleet_outbound",
        "schedule_state": "completed",
        "scheduled_batch_count": 3,
        "completed_batch_count": 3,
        "runtime_result_count": 3,
        "total_input_record_count": 9,
        "total_output_record_count": 9,
        "audited_run_ids": ["r1", "r2", "r3"],
        "runtime_audit_entries": [{"run_id": "r1"}, {"run_id": "r2"}, {"run_id": "r3"}],
    }


def make_completed_boundary_result() -> dict:
    return {
        "boundary_state": "completed",
        "failure_category": None,
        "failure_message": None,
        "schedule_result": make_schedule_result(),
        "audit_report": make_audit_report(),
    }


def make_failed_boundary_result() -> dict:
    return {
        "boundary_state": "failed",
        "failure_category": "runtime_failure",
        "failure_message": "execution failed",
        "schedule_result": None,
        "audit_report": None,
    }


# ---------------------------------------------------------------------------
# Boundary validation
# ---------------------------------------------------------------------------


class TestBoundaryValidation:
    def test_non_dict_rejected(self):
        with pytest.raises(TypeError, match="boundary_result must be a dict"):
            build_runtime_metrics_report("not a dict")

    def test_missing_fields_rejected(self):
        for field in [
            "boundary_state",
            "failure_category",
            "failure_message",
            "schedule_result",
            "audit_report",
        ]:
            boundary = make_completed_boundary_result()
            del boundary[field]
            with pytest.raises(ValueError, match=field):
                build_runtime_metrics_report(boundary)

    def test_extra_fields_rejected(self):
        boundary = make_completed_boundary_result()
        boundary["extra"] = "x"

        with pytest.raises(ValueError, match="unexpected fields"):
            build_runtime_metrics_report(boundary)

    def test_invalid_boundary_state_rejected(self):
        boundary = make_completed_boundary_result()
        boundary["boundary_state"] = "unknown"

        with pytest.raises(ValueError, match="Invalid boundary_state"):
            build_runtime_metrics_report(boundary)

    def test_wrong_types_rejected(self):
        boundary = make_completed_boundary_result()
        boundary["boundary_state"] = 123
        with pytest.raises(TypeError, match="boundary_state"):
            build_runtime_metrics_report(boundary)

        boundary = make_completed_boundary_result()
        boundary["failure_category"] = 123
        with pytest.raises(TypeError, match="failure_category"):
            build_runtime_metrics_report(boundary)

        boundary = make_completed_boundary_result()
        boundary["failure_message"] = 123
        with pytest.raises(TypeError, match="failure_message"):
            build_runtime_metrics_report(boundary)

        boundary = make_completed_boundary_result()
        boundary["schedule_result"] = "not_dict"
        with pytest.raises(TypeError, match="schedule_result"):
            build_runtime_metrics_report(boundary)

        boundary = make_completed_boundary_result()
        boundary["audit_report"] = "not_dict"
        with pytest.raises(TypeError, match="audit_report"):
            build_runtime_metrics_report(boundary)


# ---------------------------------------------------------------------------
# State combination validation
# ---------------------------------------------------------------------------


class TestStateCombinationValidation:
    def test_completed_with_none_schedule_result_rejected(self):
        boundary = make_completed_boundary_result()
        boundary["schedule_result"] = None

        with pytest.raises(ValueError, match="requires schedule_result dict"):
            build_runtime_metrics_report(boundary)

    def test_completed_with_none_audit_report_rejected(self):
        boundary = make_completed_boundary_result()
        boundary["audit_report"] = None

        with pytest.raises(ValueError, match="requires audit_report dict"):
            build_runtime_metrics_report(boundary)

    def test_completed_with_non_none_failure_category_rejected(self):
        boundary = make_completed_boundary_result()
        boundary["failure_category"] = "should_be_none"

        with pytest.raises(ValueError, match="failure_category"):
            build_runtime_metrics_report(boundary)

    def test_failed_with_dict_schedule_result_rejected(self):
        boundary = make_failed_boundary_result()
        boundary["schedule_result"] = make_schedule_result()

        with pytest.raises(ValueError, match="schedule_result"):
            build_runtime_metrics_report(boundary)

    def test_failed_with_dict_audit_report_rejected(self):
        boundary = make_failed_boundary_result()
        boundary["audit_report"] = make_audit_report()

        with pytest.raises(ValueError, match="audit_report"):
            build_runtime_metrics_report(boundary)

    def test_failed_with_none_failure_category_rejected(self):
        boundary = make_failed_boundary_result()
        boundary["failure_category"] = None

        with pytest.raises(ValueError, match="failure_category"):
            build_runtime_metrics_report(boundary)


# ---------------------------------------------------------------------------
# Nested validation
# ---------------------------------------------------------------------------


class TestNestedValidation:
    def test_missing_schedule_result_fields_rejected(self):
        for field in [
            "schedule_id",
            "schedule_scope",
            "schedule_state",
            "scheduled_batch_count",
            "completed_batch_count",
            "runtime_results",
        ]:
            boundary = make_completed_boundary_result()
            del boundary["schedule_result"][field]
            with pytest.raises(ValueError, match=field):
                build_runtime_metrics_report(boundary)

    def test_extra_schedule_result_fields_rejected(self):
        boundary = make_completed_boundary_result()
        boundary["schedule_result"]["extra"] = "x"

        with pytest.raises(ValueError, match="unexpected fields"):
            build_runtime_metrics_report(boundary)

    def test_missing_audit_report_fields_rejected(self):
        for field in [
            "audit_report_id",
            "schedule_id",
            "schedule_scope",
            "schedule_state",
            "scheduled_batch_count",
            "completed_batch_count",
            "runtime_result_count",
            "total_input_record_count",
            "total_output_record_count",
            "audited_run_ids",
            "runtime_audit_entries",
        ]:
            boundary = make_completed_boundary_result()
            del boundary["audit_report"][field]
            with pytest.raises(ValueError, match=field):
                build_runtime_metrics_report(boundary)

    def test_extra_audit_report_fields_rejected(self):
        boundary = make_completed_boundary_result()
        boundary["audit_report"]["extra"] = "x"

        with pytest.raises(ValueError, match="unexpected fields"):
            build_runtime_metrics_report(boundary)

    def test_wrong_nested_types_rejected(self):
        boundary = make_completed_boundary_result()
        boundary["schedule_result"]["schedule_id"] = 123
        with pytest.raises(TypeError, match="schedule_id"):
            build_runtime_metrics_report(boundary)

        boundary = make_completed_boundary_result()
        boundary["schedule_result"]["scheduled_batch_count"] = "3"
        with pytest.raises(TypeError, match="scheduled_batch_count"):
            build_runtime_metrics_report(boundary)

        boundary = make_completed_boundary_result()
        boundary["audit_report"]["runtime_result_count"] = "3"
        with pytest.raises(TypeError, match="runtime_result_count"):
            build_runtime_metrics_report(boundary)

        boundary = make_completed_boundary_result()
        boundary["audit_report"]["audited_run_ids"] = "not_a_list"
        with pytest.raises(TypeError, match="audited_run_ids"):
            build_runtime_metrics_report(boundary)


# ---------------------------------------------------------------------------
# Consistency validation
# ---------------------------------------------------------------------------


class TestConsistencyValidation:
    def test_mismatched_schedule_id_rejected(self):
        boundary = make_completed_boundary_result()
        boundary["audit_report"]["schedule_id"] = "sched_other"

        with pytest.raises(ValueError, match="schedule_id mismatch"):
            build_runtime_metrics_report(boundary)

    def test_mismatched_schedule_scope_rejected(self):
        boundary = make_completed_boundary_result()
        boundary["audit_report"]["schedule_scope"] = "other_scope"

        with pytest.raises(ValueError, match="schedule_scope mismatch"):
            build_runtime_metrics_report(boundary)

    def test_mismatched_scheduled_batch_count_rejected(self):
        boundary = make_completed_boundary_result()
        boundary["audit_report"]["scheduled_batch_count"] = 99

        with pytest.raises(ValueError, match="scheduled_batch_count mismatch"):
            build_runtime_metrics_report(boundary)

    def test_mismatched_completed_batch_count_rejected(self):
        boundary = make_completed_boundary_result()
        boundary["audit_report"]["completed_batch_count"] = 99

        with pytest.raises(ValueError, match="completed_batch_count mismatch"):
            build_runtime_metrics_report(boundary)


# ---------------------------------------------------------------------------
# Core behavior
# ---------------------------------------------------------------------------


class TestCoreBehavior:
    def test_completed_boundary_produces_exact_metrics_report(self):
        boundary = make_completed_boundary_result()

        result = build_runtime_metrics_report(boundary)

        assert result == {
            "metrics_state": "completed",
            "boundary_state": "completed",
            "schedule_id": "sched_001",
            "schedule_scope": "fleet_outbound",
            "runtime_result_count": 3,
            "completed_batch_count": 3,
            "scheduled_batch_count": 3,
            "total_input_record_count": 9,
            "total_output_record_count": 9,
            "audited_run_id_count": 3,
            "failure_category": None,
            "failure_message": None,
        }

    def test_failed_boundary_produces_zeroed_metrics_report(self):
        boundary = make_failed_boundary_result()

        result = build_runtime_metrics_report(boundary)

        assert result == {
            "metrics_state": "failed",
            "boundary_state": "failed",
            "schedule_id": None,
            "schedule_scope": None,
            "runtime_result_count": 0,
            "completed_batch_count": 0,
            "scheduled_batch_count": 0,
            "total_input_record_count": 0,
            "total_output_record_count": 0,
            "audited_run_id_count": 0,
            "failure_category": "runtime_failure",
            "failure_message": "execution failed",
        }

    def test_output_has_exact_fields_only(self):
        result = build_runtime_metrics_report(make_completed_boundary_result())

        assert set(result.keys()) == {
            "metrics_state",
            "boundary_state",
            "schedule_id",
            "schedule_scope",
            "runtime_result_count",
            "completed_batch_count",
            "scheduled_batch_count",
            "total_input_record_count",
            "total_output_record_count",
            "audited_run_id_count",
            "failure_category",
            "failure_message",
        }

    def test_audited_run_id_count_derived_from_audited_run_ids(self):
        boundary = make_completed_boundary_result()
        boundary["audit_report"]["audited_run_ids"] = ["r1", "r2", "r3", "r4", "r5"]

        result = build_runtime_metrics_report(boundary)

        assert result["audited_run_id_count"] == 5

    def test_failure_fields_preserved_exactly(self):
        boundary = make_failed_boundary_result()
        boundary["failure_category"] = "audit_failure"
        boundary["failure_message"] = "audit crashed"

        result = build_runtime_metrics_report(boundary)

        assert result["failure_category"] == "audit_failure"
        assert result["failure_message"] == "audit crashed"


# ---------------------------------------------------------------------------
# Safety
# ---------------------------------------------------------------------------


class TestSafety:
    def test_boundary_result_not_mutated(self):
        boundary = make_completed_boundary_result()
        boundary_before = deepcopy(boundary)

        build_runtime_metrics_report(boundary)

        assert boundary == boundary_before
