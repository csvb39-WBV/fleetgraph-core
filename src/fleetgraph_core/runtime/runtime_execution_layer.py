"""
MB1 Runtime Execution Layer.

Pure in-process runtime coordinator that executes one FleetGraph run
deterministically from FG5-MB1 records, enforces explicit duplicate
rejection based on caller-supplied execution registry object, and returns
a run-level result envelope.

Idempotency is caller-managed via execution registry.
No persistence, scheduling, email, billing, or network I/O.
"""

import hashlib
from typing import Any

from fleetgraph_core.output.workflow_run_director import apply_workflow_run_director
from fleetgraph_core.runtime.execution_registry import ExecutionRegistry


# Fields required for run_id construction
_RUN_ID_REQUIRED_FIELDS = {"canonical_organization_key", "source_id", "opportunity_rank"}


def _validate_run_id_fields(record: dict[str, Any], record_index: int) -> None:
    """
    Validate that a record contains all fields required for run_id construction.

    Args:
        record: Record to validate
        record_index: Position of record in list (for error reporting)

    Raises:
        ValueError: if any required field is missing
    """
    missing_fields = _RUN_ID_REQUIRED_FIELDS - set(record.keys())
    if missing_fields:
        raise ValueError(
            f"Record at index {record_index} is missing fields required for "
            f"run_id construction: {', '.join(sorted(missing_fields))}"
        )


def _build_run_id_from_records(records: list[dict[str, Any]]) -> str:
    """
    Build a deterministic run_id from the full record set.

    The run_id is derived from:
    - Record count
    - All record signatures (org_key:source_id:rank) hashed together
    - Special marker for empty records

    This ensures:
    - Same logical record set always produces same run_id
    - Different record sets do not collide
    - Reordered records produce different run_ids if content differs

    Args:
        records: List of FG5-MB1 format records (assumed format-valid)

    Returns:
        Deterministic run_id string

    Raises:
        ValueError: if any record missing required fields for run_id construction
    """
    if not records:
        return "runtime:0:empty:empty"

    # Validate all records have required fields before computing run_id
    for i, record in enumerate(records):
        _validate_run_id_fields(record, i)

    # Build signature for each record
    signatures = []
    for record in records:
        sig = (
            record["canonical_organization_key"]
            + ":"
            + record["source_id"]
            + ":"
            + str(record["opportunity_rank"])
        )
        signatures.append(sig)

    # Hash all signatures to create deterministic run_id
    # Include order (signatures are in input order, not sorted), so reordering changes hash
    all_sigs = "|".join(signatures)
    sig_hash = hashlib.sha256(all_sigs.encode()).hexdigest()[:16]

    return f"runtime:{len(records)}:{sig_hash}"


def validate_runtime_request(records: list[dict[str, Any]]) -> None:
    """
    Validate runtime request records format.

    Validates:
    - records is a list
    - each record is a dict

    Detailed record content validation (FG5-MB1 schema) is delegated to
    apply_workflow_run_director which calls validate_workflow_run_inputs.

    Args:
        records: Records to validate

    Raises:
        TypeError: if records is not a list or contains non-dict entries
    """
    if not isinstance(records, list):
        raise TypeError("records must be a list")

    for i, record in enumerate(records):
        if not isinstance(record, dict):
            raise TypeError(f"Record at index {i} must be a dict")


def validate_execution_registry(execution_registry: ExecutionRegistry) -> None:
    """Validate that the caller provided an execution registry object."""
    if not isinstance(execution_registry, ExecutionRegistry):
        raise TypeError("execution_registry must be an ExecutionRegistry instance")


def apply_runtime_execution(
    records: list[dict[str, Any]],
    execution_registry: ExecutionRegistry,
) -> dict[str, Any]:
    """
    Execute a single FleetGraph run with FG5-MB1 records.

    Process:
    1. Validate records format
    2. Compute deterministic run_id from full record set
    3. Check caller-supplied execution registry object for duplicate
    4. If duplicate found: raise ValueError
    5. If new: execute apply_workflow_run_director
    6. Build and return runtime result envelope

    Idempotency is caller-managed. Caller is responsible for:
    - Initializing and maintaining execution_registry
    - Adding run_id to registry after successful execution
    - Reusing registry across invocations as needed

    Args:
        records: List of FG5-MB1 format records
        execution_registry: Registry of already-executed run_ids.
                   Required; caller must provide and maintain.

    Returns:
        Runtime result envelope dict with locked structure:
        {
            "run_id": str,
            "runtime_state": str,
            "workflow_result": dict,
            "input_record_count": int,
            "output_record_count": int,
        }

    Raises:
        TypeError: if records format validation fails
                   or execution_registry is not an ExecutionRegistry
        ValueError: if run_id found in execution_registry, or if record
                   missing required fields for run_id construction
        Exception: if apply_workflow_run_director raises (propagated unchanged)
    """
    # Step 1: Validate records format
    validate_runtime_request(records)
    validate_execution_registry(execution_registry)

    # Step 2: Compute deterministic run_id from full record set
    run_id = _build_run_id_from_records(records)

    # Step 3 & 4: Check caller-supplied execution registry
    execution_registry.assert_not_executed(run_id)

    # Step 5: Execute workflow
    workflow_result = apply_workflow_run_director(records=records)

    # Step 6: Build runtime result envelope
    result_envelope = {
        "run_id": run_id,
        "runtime_state": "completed",
        "workflow_result": workflow_result,
        "input_record_count": len(records),
        "output_record_count": len(workflow_result["final_results"]),
    }

    return result_envelope
