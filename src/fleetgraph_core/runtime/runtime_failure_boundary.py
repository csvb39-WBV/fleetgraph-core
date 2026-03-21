"""
MB5 Failure Boundary Layer.

Wraps MB3 schedule execution and MB4 audit generation in a strict failure
boundary. Classifies failures deterministically, stops on first failure, and
returns a closed-schema success or failure result.

No retries, no backoff, no persistence, no logging side effects.
"""

from typing import Any

from fleetgraph_core.runtime.execution_registry import ExecutionRegistry
from fleetgraph_core.runtime.runtime_audit_layer import build_runtime_audit_report
from fleetgraph_core.runtime.runtime_scheduler import apply_runtime_schedule

_DUPLICATE_MARKERS = ("Duplicate execution detected", "duplicate run_id detected")


def _classify_scheduler_exception(exc: Exception) -> str:
    """
    Classify a scheduler-raised exception into a failure category string.

    Classification rules:
    - ValueError whose message contains a duplicate marker → duplicate_execution
    - TypeError or non-duplicate ValueError → validation_error
    - Any other exception type → runtime_failure
    """
    if isinstance(exc, ValueError):
        msg = str(exc)
        if any(marker in msg for marker in _DUPLICATE_MARKERS):
            return "duplicate_execution"
        return "validation_error"

    if isinstance(exc, TypeError):
        return "validation_error"

    return "runtime_failure"


def apply_runtime_failure_boundary(
    schedule_request: dict[str, Any],
    execution_registry: ExecutionRegistry,
) -> dict[str, Any]:
    """
    Execute a schedule run within a strict failure boundary.

    Success path:
    1. Call apply_runtime_schedule(schedule_request, execution_registry).
    2. Call build_runtime_audit_report(schedule_result).
    3. Return closed-schema success result.

    Failure path:
    - Any exception from step 1 is caught, classified, and returned as a
      closed-schema failure result. Step 2 is not attempted.
    - Any exception from step 2 is caught and returned as an audit_failure.
    - Execution stops immediately on first failure.
    - No retries. No partial-success modes.

    Args:
        schedule_request: Closed-schema dict matching the MB3 input contract.
        execution_registry: Caller-owned ExecutionRegistry instance.

    Returns:
        Success result:
        {
            "boundary_state": "completed",
            "failure_category": None,
            "failure_message": None,
            "schedule_result": dict,
            "audit_report": dict,
        }

        Failure result:
        {
            "boundary_state": "failed",
            "failure_category": str,
            "failure_message": str,
            "schedule_result": None,
            "audit_report": None,
        }
    """
    # Step 1: Execute schedule
    try:
        schedule_result = apply_runtime_schedule(schedule_request, execution_registry)
    except Exception as exc:
        failure_category = _classify_scheduler_exception(exc)
        return {
            "boundary_state": "failed",
            "failure_category": failure_category,
            "failure_message": str(exc),
            "schedule_result": None,
            "audit_report": None,
        }

    # Step 2: Build audit report
    try:
        audit_report = build_runtime_audit_report(schedule_result)
    except Exception as exc:
        return {
            "boundary_state": "failed",
            "failure_category": "audit_failure",
            "failure_message": str(exc),
            "schedule_result": None,
            "audit_report": None,
        }

    return {
        "boundary_state": "completed",
        "failure_category": None,
        "failure_message": None,
        "schedule_result": schedule_result,
        "audit_report": audit_report,
    }
