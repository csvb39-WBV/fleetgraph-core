"""
Test suite for MB4 Execution Audit Layer.

Validates:
- Closed-schema validation of schedule_result input (missing/extra/wrong-type
  fields at the schedule level and per runtime_results item)
- workflow_run_state extraction and mapping to workflow_state
- Core audit report construction: counts, aggregates, ordering, entry shape
- Deterministic and prefix-qualified audit_report_id
- Empty schedule behavior
- Input safety: no mutation of schedule_result or nested dicts
"""

from copy import deepcopy

import pytest

from fleetgraph_core.runtime.runtime_audit_layer import build_runtime_audit_report


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def make_workflow_result(
    workflow_run_state: str = "completed",
    output_count: int = 2,
) -> dict:
    return {
        "workflow_run_id": "wf_001",
        "workflow_run_state": workflow_run_state,
        "final_results": [{"id": str(i)} for i in range(output_count)],
    }


def make_runtime_result(
    run_id: str = "run_001",
    runtime_state: str = "completed",
    input_record_count: int = 2,
    output_record_count: int = 2,
    workflow_run_state: str = "completed",
) -> dict:
    return {
        "run_id": run_id,
        "runtime_state": runtime_state,
        "workflow_result": make_workflow_result(workflow_run_state, output_record_count),
        "input_record_count": input_record_count,
        "output_record_count": output_record_count,
    }


def make_schedule_result(
    schedule_id: str = "sched_001",
    schedule_scope: str = "test_scope",
    schedule_state: str = "completed",
    scheduled_batch_count: int = 1,
    completed_batch_count: int = 1,
    runtime_results: list | None = None,
) -> dict:
    if runtime_results is None:
        runtime_results = [make_runtime_result()]
    return {
        "schedule_id": schedule_id,
        "schedule_scope": schedule_scope,
        "schedule_state": schedule_state,
        "scheduled_batch_count": scheduled_batch_count,
        "completed_batch_count": completed_batch_count,
        "runtime_results": runtime_results,
    }


# ---------------------------------------------------------------------------
# Validation — schedule level
# ---------------------------------------------------------------------------


class TestScheduleResultValidation:
    """Closed-schema validation of the top-level schedule_result."""

    def test_non_dict_rejected(self):
        with pytest.raises(TypeError, match="schedule_result must be a dict"):
            build_runtime_audit_report("not a dict")

        with pytest.raises(TypeError, match="schedule_result must be a dict"):
            build_runtime_audit_report(["list", "not", "dict"])

    def test_missing_schedule_id_rejected(self):
        result = make_schedule_result()
        del result["schedule_id"]

        with pytest.raises(ValueError, match="schedule_id"):
            build_runtime_audit_report(result)

    def test_missing_schedule_scope_rejected(self):
        result = make_schedule_result()
        del result["schedule_scope"]

        with pytest.raises(ValueError, match="schedule_scope"):
            build_runtime_audit_report(result)

    def test_missing_schedule_state_rejected(self):
        result = make_schedule_result()
        del result["schedule_state"]

        with pytest.raises(ValueError, match="schedule_state"):
            build_runtime_audit_report(result)

    def test_missing_scheduled_batch_count_rejected(self):
        result = make_schedule_result()
        del result["scheduled_batch_count"]

        with pytest.raises(ValueError, match="scheduled_batch_count"):
            build_runtime_audit_report(result)

    def test_missing_completed_batch_count_rejected(self):
        result = make_schedule_result()
        del result["completed_batch_count"]

        with pytest.raises(ValueError, match="completed_batch_count"):
            build_runtime_audit_report(result)

    def test_missing_runtime_results_rejected(self):
        result = make_schedule_result()
        del result["runtime_results"]

        with pytest.raises(ValueError, match="runtime_results"):
            build_runtime_audit_report(result)

    def test_extra_schedule_level_field_rejected(self):
        result = make_schedule_result()
        result["extra_field"] = "not_allowed"

        with pytest.raises(ValueError, match="unexpected fields"):
            build_runtime_audit_report(result)

    def test_wrong_type_schedule_id_rejected(self):
        result = make_schedule_result()
        result["schedule_id"] = 123

        with pytest.raises(TypeError, match="schedule_id"):
            build_runtime_audit_report(result)

    def test_wrong_type_schedule_scope_rejected(self):
        result = make_schedule_result()
        result["schedule_scope"] = 999

        with pytest.raises(TypeError, match="schedule_scope"):
            build_runtime_audit_report(result)

    def test_wrong_type_scheduled_batch_count_rejected(self):
        result = make_schedule_result()
        result["scheduled_batch_count"] = "not_an_int"

        with pytest.raises(TypeError, match="scheduled_batch_count"):
            build_runtime_audit_report(result)

    def test_wrong_type_completed_batch_count_rejected(self):
        result = make_schedule_result()
        result["completed_batch_count"] = "not_an_int"

        with pytest.raises(TypeError, match="completed_batch_count"):
            build_runtime_audit_report(result)

    def test_non_list_runtime_results_rejected(self):
        result = make_schedule_result()
        result["runtime_results"] = {"not": "a list"}

        with pytest.raises(TypeError, match="runtime_results"):
            build_runtime_audit_report(result)


# ---------------------------------------------------------------------------
# Validation — runtime_results items
# ---------------------------------------------------------------------------


class TestRuntimeResultItemValidation:
    """Closed-schema validation of individual runtime_results entries."""

    def test_non_dict_runtime_result_rejected(self):
        result = make_schedule_result(runtime_results=["not a dict"])

        with pytest.raises(TypeError, match=r"runtime_results\[0\] must be a dict"):
            build_runtime_audit_report(result)

    def test_missing_run_id_rejected(self):
        item = make_runtime_result()
        del item["run_id"]
        result = make_schedule_result(runtime_results=[item])

        with pytest.raises(ValueError, match="run_id"):
            build_runtime_audit_report(result)

    def test_missing_runtime_state_rejected(self):
        item = make_runtime_result()
        del item["runtime_state"]
        result = make_schedule_result(runtime_results=[item])

        with pytest.raises(ValueError, match="runtime_state"):
            build_runtime_audit_report(result)

    def test_missing_workflow_result_rejected(self):
        item = make_runtime_result()
        del item["workflow_result"]
        result = make_schedule_result(runtime_results=[item])

        with pytest.raises(ValueError, match="workflow_result"):
            build_runtime_audit_report(result)

    def test_missing_input_record_count_rejected(self):
        item = make_runtime_result()
        del item["input_record_count"]
        result = make_schedule_result(runtime_results=[item])

        with pytest.raises(ValueError, match="input_record_count"):
            build_runtime_audit_report(result)

    def test_missing_output_record_count_rejected(self):
        item = make_runtime_result()
        del item["output_record_count"]
        result = make_schedule_result(runtime_results=[item])

        with pytest.raises(ValueError, match="output_record_count"):
            build_runtime_audit_report(result)

    def test_extra_runtime_result_field_rejected(self):
        item = make_runtime_result()
        item["extra_field"] = "not_allowed"
        result = make_schedule_result(runtime_results=[item])

        with pytest.raises(ValueError, match="unexpected fields"):
            build_runtime_audit_report(result)

    def test_wrong_type_run_id_rejected(self):
        item = make_runtime_result()
        item["run_id"] = 123
        result = make_schedule_result(runtime_results=[item])

        with pytest.raises(TypeError, match="run_id"):
            build_runtime_audit_report(result)

    def test_wrong_type_workflow_result_rejected(self):
        item = make_runtime_result()
        item["workflow_result"] = "not a dict"
        result = make_schedule_result(runtime_results=[item])

        with pytest.raises(TypeError, match="workflow_result"):
            build_runtime_audit_report(result)

    def test_missing_workflow_run_state_rejected(self):
        item = make_runtime_result()
        del item["workflow_result"]["workflow_run_state"]
        result = make_schedule_result(runtime_results=[item])

        with pytest.raises(ValueError, match="workflow_run_state"):
            build_runtime_audit_report(result)


# ---------------------------------------------------------------------------
# Core behavior
# ---------------------------------------------------------------------------


class TestCoreBehavior:
    """Nominal audit report construction."""

    def test_single_runtime_result_produces_valid_audit_report(self):
        result = make_schedule_result()

        report = build_runtime_audit_report(result)

        assert isinstance(report, dict)
        assert report["schedule_id"] == "sched_001"
        assert report["schedule_scope"] == "test_scope"
        assert report["schedule_state"] == "completed"

    def test_output_has_all_required_fields_only(self):
        result = make_schedule_result()

        report = build_runtime_audit_report(result)

        assert set(report.keys()) == {
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
        }

    def test_multiple_runtime_results_preserve_exact_order(self):
        items = [
            make_runtime_result(run_id="run_aaa"),
            make_runtime_result(run_id="run_bbb"),
            make_runtime_result(run_id="run_ccc"),
        ]
        result = make_schedule_result(
            scheduled_batch_count=3,
            completed_batch_count=3,
            runtime_results=items,
        )

        report = build_runtime_audit_report(result)

        assert report["audited_run_ids"] == ["run_aaa", "run_bbb", "run_ccc"]
        assert report["runtime_audit_entries"][0]["run_id"] == "run_aaa"
        assert report["runtime_audit_entries"][1]["run_id"] == "run_bbb"
        assert report["runtime_audit_entries"][2]["run_id"] == "run_ccc"

    def test_runtime_result_count_correct(self):
        items = [make_runtime_result(run_id=f"run_{i:03d}") for i in range(3)]
        result = make_schedule_result(
            scheduled_batch_count=3,
            completed_batch_count=3,
            runtime_results=items,
        )

        report = build_runtime_audit_report(result)

        assert report["runtime_result_count"] == 3

    def test_total_input_record_count_correct(self):
        items = [
            make_runtime_result(run_id="run_001", input_record_count=3),
            make_runtime_result(run_id="run_002", input_record_count=5),
        ]
        result = make_schedule_result(
            scheduled_batch_count=2,
            completed_batch_count=2,
            runtime_results=items,
        )

        report = build_runtime_audit_report(result)

        assert report["total_input_record_count"] == 8

    def test_total_output_record_count_correct(self):
        items = [
            make_runtime_result(run_id="run_001", output_record_count=2),
            make_runtime_result(run_id="run_002", output_record_count=4),
        ]
        result = make_schedule_result(
            scheduled_batch_count=2,
            completed_batch_count=2,
            runtime_results=items,
        )

        report = build_runtime_audit_report(result)

        assert report["total_output_record_count"] == 6

    def test_audited_run_ids_correct(self):
        items = [
            make_runtime_result(run_id="run_alpha"),
            make_runtime_result(run_id="run_beta"),
        ]
        result = make_schedule_result(
            scheduled_batch_count=2,
            completed_batch_count=2,
            runtime_results=items,
        )

        report = build_runtime_audit_report(result)

        assert report["audited_run_ids"] == ["run_alpha", "run_beta"]

    def test_runtime_audit_entries_embedded_correctly(self):
        item = make_runtime_result(
            run_id="run_001",
            runtime_state="completed",
            input_record_count=3,
            output_record_count=2,
            workflow_run_state="completed",
        )
        result = make_schedule_result(runtime_results=[item])

        report = build_runtime_audit_report(result)

        assert len(report["runtime_audit_entries"]) == 1
        entry = report["runtime_audit_entries"][0]
        assert set(entry.keys()) == {
            "run_id",
            "runtime_state",
            "input_record_count",
            "output_record_count",
            "workflow_state",
        }
        assert entry["run_id"] == "run_001"
        assert entry["runtime_state"] == "completed"
        assert entry["input_record_count"] == 3
        assert entry["output_record_count"] == 2
        assert entry["workflow_state"] == "completed"

    def test_workflow_state_extracted_from_workflow_run_state(self):
        item = make_runtime_result(run_id="run_001", workflow_run_state="partially_completed")
        result = make_schedule_result(runtime_results=[item])

        report = build_runtime_audit_report(result)

        assert report["runtime_audit_entries"][0]["workflow_state"] == "partially_completed"

    def test_deterministic_audit_report_id_same_inputs(self):
        result = make_schedule_result()
        result_copy = deepcopy(result)

        report1 = build_runtime_audit_report(result)
        report2 = build_runtime_audit_report(result_copy)

        assert report1["audit_report_id"] == report2["audit_report_id"]

    def test_different_run_ids_produce_different_audit_report_ids(self):
        result_a = make_schedule_result(
            runtime_results=[make_runtime_result(run_id="run_aaa")]
        )
        result_b = make_schedule_result(
            runtime_results=[make_runtime_result(run_id="run_bbb")]
        )

        report_a = build_runtime_audit_report(result_a)
        report_b = build_runtime_audit_report(result_b)

        assert report_a["audit_report_id"] != report_b["audit_report_id"]

    def test_audit_report_id_has_audit_prefix(self):
        result = make_schedule_result()

        report = build_runtime_audit_report(result)

        assert report["audit_report_id"].startswith("audit:")

    def test_empty_schedule_result_produces_valid_empty_audit_report(self):
        result = make_schedule_result(
            scheduled_batch_count=0,
            completed_batch_count=0,
            runtime_results=[],
        )

        report = build_runtime_audit_report(result)

        assert report["schedule_state"] == "completed"
        assert report["runtime_result_count"] == 0
        assert report["total_input_record_count"] == 0
        assert report["total_output_record_count"] == 0
        assert report["audited_run_ids"] == []
        assert report["runtime_audit_entries"] == []
        assert isinstance(report["audit_report_id"], str)
        assert report["audit_report_id"].startswith("audit:")

    def test_empty_schedule_audit_report_id_is_deterministic(self):
        result_a = make_schedule_result(
            schedule_id="sched_empty",
            scheduled_batch_count=0,
            completed_batch_count=0,
            runtime_results=[],
        )
        result_b = deepcopy(result_a)

        report_a = build_runtime_audit_report(result_a)
        report_b = build_runtime_audit_report(result_b)

        assert report_a["audit_report_id"] == report_b["audit_report_id"]

    def test_schedule_counts_echoed_correctly(self):
        result = make_schedule_result(
            scheduled_batch_count=5,
            completed_batch_count=3,
            runtime_results=[make_runtime_result(run_id=f"run_{i:03d}") for i in range(3)],
        )

        report = build_runtime_audit_report(result)

        assert report["scheduled_batch_count"] == 5
        assert report["completed_batch_count"] == 3


# ---------------------------------------------------------------------------
# Safety
# ---------------------------------------------------------------------------


class TestSafety:
    """Input safety: no mutation of schedule_result or its nested structures."""

    def test_schedule_result_not_mutated(self):
        result = make_schedule_result(
            runtime_results=[
                make_runtime_result(run_id="run_001"),
                make_runtime_result(run_id="run_002"),
            ],
            scheduled_batch_count=2,
            completed_batch_count=2,
        )
        original_copy = deepcopy(result)

        build_runtime_audit_report(result)

        assert result == original_copy
