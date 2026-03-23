"""
MB9 Runtime Metrics / Analytics Layer.

Consumes MB5 boundary results and produces a deterministic closed-schema
runtime metrics report.

Pure in-memory Python with strict contract validation.
"""

from __future__ import annotations

import time
from typing import Any

from fleetgraph_core.runtime.runtime_bootstrap import RuntimeBootstrap
from fleetgraph_core.runtime.runtime_health_api import build_runtime_health_response

_BOUNDARY_REQUIRED_FIELDS: frozenset[str] = frozenset({
    "boundary_state",
    "failure_category",
    "failure_message",
    "schedule_result",
    "audit_report",
})

_SCHEDULE_RESULT_REQUIRED_FIELDS: frozenset[str] = frozenset({
    "schedule_id",
    "schedule_scope",
    "schedule_state",
    "scheduled_batch_count",
    "completed_batch_count",
    "runtime_results",
})

_AUDIT_REPORT_REQUIRED_FIELDS: frozenset[str] = frozenset({
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
})

_OUTPUT_FIELDS: frozenset[str] = frozenset({
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
})

_EXPECTED_HEALTH_RESPONSE_KEYS: tuple[str, ...] = (
    "response_type",
    "response_schema_version",
    "status",
    "checks",
    "runtime",
)

_EXPECTED_RUNTIME_METRICS_KEYS: tuple[str, ...] = (
    "startup_success",
    "runtime_status",
)

_EXPECTED_REQUEST_METRICS_KEYS: tuple[str, ...] = (
    "request_count_total",
    "request_success_count",
    "request_failure_count",
    "execution_time_ms",
)

_EXPECTED_ERROR_METRICS_KEYS: tuple[str, ...] = (
    "exception_count",
    "failure_event_count",
)

_EXPECTED_HEALTH_ALIGNMENT_KEYS: tuple[str, ...] = (
    "health_endpoint_status",
    "health_is_healthy",
)

_EXPECTED_METRICS_RESPONSE_KEYS: tuple[str, ...] = (
    "response_type",
    "response_schema_version",
    "runtime_metrics",
    "request_metrics",
    "error_metrics",
    "health_alignment",
)


def _require_closed_schema(obj: dict, required_fields: frozenset[str], label: str) -> None:
    present = set(obj.keys())

    missing = required_fields - present
    if missing:
        raise ValueError(f"{label} is missing required fields: {', '.join(sorted(missing))}")

    extra = present - required_fields
    if extra:
        raise ValueError(f"{label} contains unexpected fields: {', '.join(sorted(extra))}")


def _require_type(value: object, expected_type: type, field_label: str) -> None:
    if not isinstance(value, expected_type):
        raise TypeError(f"{field_label} must be of type {expected_type.__name__}")


def _validate_schedule_result(schedule_result: dict[str, object]) -> None:
    _require_closed_schema(schedule_result, _SCHEDULE_RESULT_REQUIRED_FIELDS, "schedule_result")

    _require_type(schedule_result["schedule_id"], str, "schedule_result field 'schedule_id'")
    _require_type(schedule_result["schedule_scope"], str, "schedule_result field 'schedule_scope'")
    _require_type(schedule_result["schedule_state"], str, "schedule_result field 'schedule_state'")
    _require_type(
        schedule_result["scheduled_batch_count"],
        int,
        "schedule_result field 'scheduled_batch_count'",
    )
    _require_type(
        schedule_result["completed_batch_count"],
        int,
        "schedule_result field 'completed_batch_count'",
    )
    _require_type(schedule_result["runtime_results"], list, "schedule_result field 'runtime_results'")


def _validate_audit_report(audit_report: dict[str, object]) -> None:
    _require_closed_schema(audit_report, _AUDIT_REPORT_REQUIRED_FIELDS, "audit_report")

    _require_type(audit_report["audit_report_id"], str, "audit_report field 'audit_report_id'")
    _require_type(audit_report["schedule_id"], str, "audit_report field 'schedule_id'")
    _require_type(audit_report["schedule_scope"], str, "audit_report field 'schedule_scope'")
    _require_type(audit_report["schedule_state"], str, "audit_report field 'schedule_state'")
    _require_type(audit_report["scheduled_batch_count"], int, "audit_report field 'scheduled_batch_count'")
    _require_type(audit_report["completed_batch_count"], int, "audit_report field 'completed_batch_count'")
    _require_type(audit_report["runtime_result_count"], int, "audit_report field 'runtime_result_count'")
    _require_type(
        audit_report["total_input_record_count"],
        int,
        "audit_report field 'total_input_record_count'",
    )
    _require_type(
        audit_report["total_output_record_count"],
        int,
        "audit_report field 'total_output_record_count'",
    )
    _require_type(audit_report["audited_run_ids"], list, "audit_report field 'audited_run_ids'")
    _require_type(
        audit_report["runtime_audit_entries"],
        list,
        "audit_report field 'runtime_audit_entries'",
    )


def build_runtime_metrics_report(
    boundary_result: dict[str, object],
) -> dict[str, object]:
    """Build a deterministic closed-schema runtime metrics report."""
    if not isinstance(boundary_result, dict):
        raise TypeError("boundary_result must be a dict")

    _require_closed_schema(boundary_result, _BOUNDARY_REQUIRED_FIELDS, "boundary_result")

    boundary_state = boundary_result["boundary_state"]
    if not isinstance(boundary_state, str):
        raise TypeError("boundary_result field 'boundary_state' must be of type str")
    if boundary_state not in {"completed", "failed"}:
        raise ValueError(f"Invalid boundary_state: {boundary_state}")

    failure_category = boundary_result["failure_category"]
    if failure_category is not None and not isinstance(failure_category, str):
        raise TypeError("boundary_result field 'failure_category' must be a string or None")

    failure_message = boundary_result["failure_message"]
    if failure_message is not None and not isinstance(failure_message, str):
        raise TypeError("boundary_result field 'failure_message' must be a string or None")

    schedule_result = boundary_result["schedule_result"]
    if schedule_result is not None and not isinstance(schedule_result, dict):
        raise TypeError("boundary_result field 'schedule_result' must be a dict or None")

    audit_report = boundary_result["audit_report"]
    if audit_report is not None and not isinstance(audit_report, dict):
        raise TypeError("boundary_result field 'audit_report' must be a dict or None")

    if boundary_state == "completed":
        if schedule_result is None:
            raise ValueError("completed boundary_result requires schedule_result dict")
        if audit_report is None:
            raise ValueError("completed boundary_result requires audit_report dict")
        if failure_category is not None:
            raise ValueError("completed boundary_result requires failure_category to be None")
        if failure_message is not None:
            raise ValueError("completed boundary_result requires failure_message to be None")

        _validate_schedule_result(schedule_result)
        _validate_audit_report(audit_report)

        if schedule_result["schedule_id"] != audit_report["schedule_id"]:
            raise ValueError("schedule_id mismatch between schedule_result and audit_report")
        if schedule_result["schedule_scope"] != audit_report["schedule_scope"]:
            raise ValueError("schedule_scope mismatch between schedule_result and audit_report")
        if schedule_result["scheduled_batch_count"] != audit_report["scheduled_batch_count"]:
            raise ValueError("scheduled_batch_count mismatch between schedule_result and audit_report")
        if schedule_result["completed_batch_count"] != audit_report["completed_batch_count"]:
            raise ValueError("completed_batch_count mismatch between schedule_result and audit_report")

        report = {
            "metrics_state": "completed",
            "boundary_state": "completed",
            "schedule_id": schedule_result["schedule_id"],
            "schedule_scope": schedule_result["schedule_scope"],
            "runtime_result_count": audit_report["runtime_result_count"],
            "completed_batch_count": schedule_result["completed_batch_count"],
            "scheduled_batch_count": schedule_result["scheduled_batch_count"],
            "total_input_record_count": audit_report["total_input_record_count"],
            "total_output_record_count": audit_report["total_output_record_count"],
            "audited_run_id_count": len(audit_report["audited_run_ids"]),
            "failure_category": None,
            "failure_message": None,
        }
    else:
        if schedule_result is not None:
            raise ValueError("failed boundary_result requires schedule_result to be None")
        if audit_report is not None:
            raise ValueError("failed boundary_result requires audit_report to be None")
        if not isinstance(failure_category, str) or not failure_category:
            raise ValueError("failed boundary_result requires non-empty failure_category string")
        if not isinstance(failure_message, str):
            raise ValueError("failed boundary_result requires failure_message string")

        report = {
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
            "failure_category": failure_category,
            "failure_message": failure_message,
        }

    if set(report.keys()) != _OUTPUT_FIELDS:
        raise RuntimeError("internal error: metrics report schema mismatch")

    return report


def build_runtime_metrics_response(bootstrap: RuntimeBootstrap) -> dict[str, Any]:
    """Build the deterministic runtime metrics API response."""
    started_at_ns = time.perf_counter_ns()

    health_response = build_runtime_health_response(bootstrap)
    if tuple(health_response.keys()) != _EXPECTED_HEALTH_RESPONSE_KEYS:
        raise ValueError("Runtime health response does not match metrics API contract")

    health_status = health_response["status"]
    is_healthy = health_status == "healthy"

    response = {
        "response_type": "runtime_metrics_response",
        "response_schema_version": "1.0",
        "runtime_metrics": {
            "startup_success": True,
            "runtime_status": "running",
        },
        "request_metrics": {
            "request_count_total": 0,
            "request_success_count": 0,
            "request_failure_count": 0,
            "execution_time_ms": 0,
        },
        "error_metrics": {
            "exception_count": 0,
            "failure_event_count": 0 if is_healthy else 1,
        },
        "health_alignment": {
            "health_endpoint_status": health_status,
            "health_is_healthy": is_healthy,
        },
    }

    if tuple(response.keys()) != _EXPECTED_METRICS_RESPONSE_KEYS:
        raise RuntimeError("internal error: metrics response schema mismatch")
    if tuple(response["runtime_metrics"].keys()) != _EXPECTED_RUNTIME_METRICS_KEYS:
        raise RuntimeError("internal error: runtime_metrics schema mismatch")
    if tuple(response["request_metrics"].keys()) != _EXPECTED_REQUEST_METRICS_KEYS:
        raise RuntimeError("internal error: request_metrics schema mismatch")
    if tuple(response["error_metrics"].keys()) != _EXPECTED_ERROR_METRICS_KEYS:
        raise RuntimeError("internal error: error_metrics schema mismatch")
    if tuple(response["health_alignment"].keys()) != _EXPECTED_HEALTH_ALIGNMENT_KEYS:
        raise RuntimeError("internal error: health_alignment schema mismatch")

    ended_at_ns = time.perf_counter_ns()
    response["request_metrics"]["execution_time_ms"] = max(
        0,
        (ended_at_ns - started_at_ns) // 1_000_000,
    )

    return response
