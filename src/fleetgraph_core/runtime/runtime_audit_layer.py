"""
MB4 Execution Audit Layer.

Builds a deterministic, closed-schema audit report from a completed MB3
schedule result. Pure in-memory Python — no persistence, no logging side
effects, no timestamps, no UUIDs, no randomness.
"""

import hashlib
from typing import Any

_SCHEDULE_RESULT_REQUIRED_FIELDS: frozenset[str] = frozenset({
    "schedule_id",
    "schedule_scope",
    "schedule_state",
    "scheduled_batch_count",
    "completed_batch_count",
    "runtime_results",
})

_RUNTIME_RESULT_REQUIRED_FIELDS: frozenset[str] = frozenset({
    "run_id",
    "runtime_state",
    "workflow_result",
    "input_record_count",
    "output_record_count",
})

_SCHEDULE_RESULT_FIELD_TYPES: dict[str, type] = {
    "schedule_id": str,
    "schedule_scope": str,
    "schedule_state": str,
    "scheduled_batch_count": int,
    "completed_batch_count": int,
    "runtime_results": list,
}

_RUNTIME_RESULT_FIELD_TYPES: dict[str, type] = {
    "run_id": str,
    "runtime_state": str,
    "workflow_result": dict,
    "input_record_count": int,
    "output_record_count": int,
}


def _validate_schedule_result(schedule_result: Any) -> None:
    """
    Validate schedule_result against the closed MB3 output schema.

    Raises:
        TypeError: if schedule_result or any nested field has the wrong type.
        ValueError: if required fields are missing, extra fields are present,
                    or workflow_run_state is absent from any workflow_result.
    """
    if not isinstance(schedule_result, dict):
        raise TypeError("schedule_result must be a dict")

    present = set(schedule_result.keys())

    missing = _SCHEDULE_RESULT_REQUIRED_FIELDS - present
    if missing:
        raise ValueError(
            f"schedule_result is missing required fields: {', '.join(sorted(missing))}"
        )

    extra = present - _SCHEDULE_RESULT_REQUIRED_FIELDS
    if extra:
        raise ValueError(
            f"schedule_result contains unexpected fields: {', '.join(sorted(extra))}"
        )

    for field, expected_type in _SCHEDULE_RESULT_FIELD_TYPES.items():
        value = schedule_result[field]
        if not isinstance(value, expected_type):
            raise TypeError(
                f"schedule_result field '{field}' must be {expected_type.__name__}"
            )

    for i, item in enumerate(schedule_result["runtime_results"]):
        if not isinstance(item, dict):
            raise TypeError(f"runtime_results[{i}] must be a dict")

        item_present = set(item.keys())

        item_missing = _RUNTIME_RESULT_REQUIRED_FIELDS - item_present
        if item_missing:
            raise ValueError(
                f"runtime_results[{i}] is missing required fields: "
                f"{', '.join(sorted(item_missing))}"
            )

        item_extra = item_present - _RUNTIME_RESULT_REQUIRED_FIELDS
        if item_extra:
            raise ValueError(
                f"runtime_results[{i}] contains unexpected fields: "
                f"{', '.join(sorted(item_extra))}"
            )

        for field, expected_type in _RUNTIME_RESULT_FIELD_TYPES.items():
            value = item[field]
            if not isinstance(value, expected_type):
                raise TypeError(
                    f"runtime_results[{i}] field '{field}' must be {expected_type.__name__}"
                )

        if "workflow_run_state" not in item["workflow_result"]:
            raise ValueError(
                f"runtime_results[{i}] workflow_result is missing 'workflow_run_state'"
            )


def _build_audit_report_id(
    schedule_id: str,
    schedule_scope: str,
    run_ids: list[str],
) -> str:
    """Build a deterministic audit_report_id from schedule context and run IDs."""
    components = [schedule_id, schedule_scope] + run_ids
    combined = "|".join(components)
    digest = hashlib.sha256(combined.encode()).hexdigest()[:16]
    return f"audit:{digest}"


def build_runtime_audit_report(
    schedule_result: dict[str, Any],
) -> dict[str, Any]:
    """
    Build a deterministic closed-schema audit report from a completed MB3
    schedule result.

    Process:
    1. Validate schedule_result against the closed MB3 output schema.
    2. Extract ordered run_ids and compute input/output aggregates.
    3. Build per-runtime audit entries preserving exact input order.
    4. Build deterministic audit_report_id from schedule context + run_ids.
    5. Return closed-schema audit report.

    Args:
        schedule_result: Closed-schema dict matching the MB3 output contract.

    Returns:
        Audit report dict with locked structure:
        {
            "audit_report_id": str,
            "schedule_id": str,
            "schedule_scope": str,
            "schedule_state": str,
            "scheduled_batch_count": int,
            "completed_batch_count": int,
            "runtime_result_count": int,
            "total_input_record_count": int,
            "total_output_record_count": int,
            "audited_run_ids": list[str],
            "runtime_audit_entries": list[dict],
        }

    Raises:
        TypeError: if schedule_result or any field has the wrong type.
        ValueError: if required fields are missing, extra fields are present,
                    or workflow_run_state is absent from any workflow_result.
    """
    _validate_schedule_result(schedule_result)

    schedule_id: str = schedule_result["schedule_id"]
    schedule_scope: str = schedule_result["schedule_scope"]
    schedule_state: str = schedule_result["schedule_state"]
    scheduled_batch_count: int = schedule_result["scheduled_batch_count"]
    completed_batch_count: int = schedule_result["completed_batch_count"]
    runtime_results: list[dict[str, Any]] = schedule_result["runtime_results"]

    audited_run_ids: list[str] = []
    runtime_audit_entries: list[dict[str, Any]] = []
    total_input_record_count = 0
    total_output_record_count = 0

    for result in runtime_results:
        run_id: str = result["run_id"]
        runtime_state: str = result["runtime_state"]
        input_record_count: int = result["input_record_count"]
        output_record_count: int = result["output_record_count"]
        workflow_state: str = result["workflow_result"]["workflow_run_state"]

        audited_run_ids.append(run_id)
        total_input_record_count += input_record_count
        total_output_record_count += output_record_count

        runtime_audit_entries.append({
            "run_id": run_id,
            "runtime_state": runtime_state,
            "input_record_count": input_record_count,
            "output_record_count": output_record_count,
            "workflow_state": workflow_state,
        })

    audit_report_id = _build_audit_report_id(schedule_id, schedule_scope, audited_run_ids)

    return {
        "audit_report_id": audit_report_id,
        "schedule_id": schedule_id,
        "schedule_scope": schedule_scope,
        "schedule_state": schedule_state,
        "scheduled_batch_count": scheduled_batch_count,
        "completed_batch_count": completed_batch_count,
        "runtime_result_count": len(runtime_results),
        "total_input_record_count": total_input_record_count,
        "total_output_record_count": total_output_record_count,
        "audited_run_ids": audited_run_ids,
        "runtime_audit_entries": runtime_audit_entries,
    }
