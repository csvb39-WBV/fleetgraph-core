"""
MB3 Runtime Scheduler / Orchestration Layer.

Accepts a deterministic batch execution request, iterates through
scheduled batches in order, invokes apply_runtime_execution once per
batch, registers each successful run_id explicitly, and returns a
deterministic batch execution summary.

No cron, async, queues, threads, external schedulers, retries, or
persistence.
"""

from typing import Any

from fleetgraph_core.runtime.execution_registry import ExecutionRegistry
from fleetgraph_core.runtime.runtime_execution_layer import apply_runtime_execution

_REQUIRED_FIELDS: frozenset[str] = frozenset(
    {"schedule_id", "schedule_scope", "scheduled_batches"}
)


def _validate_schedule_request(schedule_request: dict[str, Any]) -> None:
    """
    Validate schedule_request against closed schema.

    Raises:
        TypeError: if schedule_request is not a dict, or fields have wrong types
        ValueError: if required fields are missing, extra fields present,
                    or string fields are empty / whitespace-only
    """
    if not isinstance(schedule_request, dict):
        raise TypeError("schedule_request must be a dict")

    present_fields = set(schedule_request.keys())

    missing = _REQUIRED_FIELDS - present_fields
    if missing:
        raise ValueError(
            f"schedule_request is missing required fields: {', '.join(sorted(missing))}"
        )

    extra = present_fields - _REQUIRED_FIELDS
    if extra:
        raise ValueError(
            f"schedule_request contains unexpected fields: {', '.join(sorted(extra))}"
        )

    schedule_id = schedule_request["schedule_id"]
    if not isinstance(schedule_id, str):
        raise TypeError("schedule_id must be a string")
    if not schedule_id.strip():
        raise ValueError("schedule_id must not be empty or whitespace-only")

    schedule_scope = schedule_request["schedule_scope"]
    if not isinstance(schedule_scope, str):
        raise TypeError("schedule_scope must be a string")
    if not schedule_scope.strip():
        raise ValueError("schedule_scope must not be empty or whitespace-only")

    scheduled_batches = schedule_request["scheduled_batches"]
    if not isinstance(scheduled_batches, list):
        raise TypeError("scheduled_batches must be a list")

    for i, batch in enumerate(scheduled_batches):
        if not isinstance(batch, list):
            raise TypeError(f"scheduled_batches[{i}] must be a list")


def apply_runtime_schedule(
    schedule_request: dict[str, Any],
    execution_registry: ExecutionRegistry,
) -> dict[str, Any]:
    """
    Execute a deterministic batch schedule against the runtime execution layer.

    Process:
    1. Validate schedule_request against closed schema.
    2. For each batch in scheduled_batches (in order):
       a. Call apply_runtime_execution(records=batch,
                                       execution_registry=execution_registry)
       b. Register the returned run_id via execution_registry.register_run(run_id)
       c. Append runtime result to runtime_results.
    3. Return batch execution summary.

    Duplicate detection or any execution error propagates immediately — no
    silent skipping, no auto-healing, no retries.

    Args:
        schedule_request: Closed-schema dict with schedule_id, schedule_scope,
                          and scheduled_batches.
        execution_registry: Caller-owned registry for duplicate detection and
                            explicit run_id registration.

    Returns:
        Batch execution summary dict with locked structure:
        {
            "schedule_id": str,
            "schedule_scope": str,
            "schedule_state": str,
            "scheduled_batch_count": int,
            "completed_batch_count": int,
            "runtime_results": list[dict],
        }

    Raises:
        TypeError: if schedule_request or its fields have wrong types.
        ValueError: if schedule_request schema is invalid, or a duplicate
                    run_id is detected.
        Exception: if apply_runtime_execution raises (propagated unchanged).
    """
    _validate_schedule_request(schedule_request)

    schedule_id: str = schedule_request["schedule_id"]
    schedule_scope: str = schedule_request["schedule_scope"]
    scheduled_batches: list[list[dict[str, Any]]] = schedule_request["scheduled_batches"]

    runtime_results: list[dict[str, Any]] = []

    for batch in scheduled_batches:
        result = apply_runtime_execution(records=batch, execution_registry=execution_registry)
        execution_registry.register_run(result["run_id"])
        runtime_results.append(result)

    return {
        "schedule_id": schedule_id,
        "schedule_scope": schedule_scope,
        "schedule_state": "completed",
        "scheduled_batch_count": len(scheduled_batches),
        "completed_batch_count": len(runtime_results),
        "runtime_results": runtime_results,
    }
