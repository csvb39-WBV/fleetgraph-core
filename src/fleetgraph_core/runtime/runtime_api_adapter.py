"""
MB7-A Internal API Adapter Layer.

Provides a clean request/response boundary for the deterministic runtime
system. Accepts a closed-schema API request, delegates to the Configuration
Layer (MB6-A) and Failure Boundary Layer (MB5), and returns a closed-schema
API response.

Not a web server. No HTTP, no auth, no persistence, no logging side effects.
"""

from fleetgraph_core.runtime.execution_registry import ExecutionRegistry
from fleetgraph_core.runtime.runtime_configuration_layer import (
    build_schedule_request_from_template,
)
from fleetgraph_core.runtime.runtime_failure_boundary import apply_runtime_failure_boundary

_REQUEST_REQUIRED_FIELDS: frozenset[str] = frozenset({
    "request_id",
    "customer_id",
    "customer_type",
    "runtime_template",
    "scheduled_batches",
})


def _validate_customer_eligibility(customer_id: str, customer_type: str) -> None:
    """Enforce hard-coded upfitter eligibility constraints."""
    if customer_type != "upfitter":
        return

    if customer_id == "Sortimo":
        return

    raise ValueError("upfitter customers are restricted")


def apply_runtime_api_request(
    api_request: dict[str, object],
    execution_registry: ExecutionRegistry,
) -> dict[str, object]:
    """
    Execute a deterministic runtime cycle through the cleaned API boundary.

    Execution flow:
    1. Validate closed-schema api_request.
    2. Validate customer eligibility.
    3. Build MB3 schedule_request via build_schedule_request_from_template.
    4. Execute apply_runtime_failure_boundary with the schedule_request.
    5. Return closed-schema API response.

    Args:
        api_request: Closed-schema dict with exactly:
            {
                "request_id": str (non-empty, non-whitespace),
                "customer_id": str (non-empty, non-whitespace),
                "customer_type": str (non-empty, non-whitespace),
                "runtime_template": dict,
                "scheduled_batches": list[list[dict]],
            }
        execution_registry: Caller-owned ExecutionRegistry instance.

    Returns:
        Closed-schema response dict:
        {
            "request_id": str,
            "api_state": str,      # "completed" or "failed"
            "boundary_result": dict,
        }

    Raises:
        TypeError: if api_request is not a dict, or any field has the wrong type.
        ValueError: if required fields are missing, extra fields are present,
                    request_id/customer_id/customer_type are empty /
                    whitespace-only, or customer eligibility is disallowed.
        Any exception raised by build_schedule_request_from_template propagates
        unchanged.
    """
    if not isinstance(api_request, dict):
        raise TypeError("api_request must be a dict")

    present = set(api_request.keys())

    missing = _REQUEST_REQUIRED_FIELDS - present
    if missing:
        raise ValueError(
            f"api_request is missing required fields: {', '.join(sorted(missing))}"
        )

    extra = present - _REQUEST_REQUIRED_FIELDS
    if extra:
        raise ValueError(
            f"api_request contains unexpected fields: {', '.join(sorted(extra))}"
        )

    request_id = api_request["request_id"]
    if not isinstance(request_id, str):
        raise TypeError("api_request field 'request_id' must be a string")
    if not request_id.strip():
        raise ValueError("api_request field 'request_id' must not be empty or whitespace-only")

    customer_id = api_request["customer_id"]
    if not isinstance(customer_id, str):
        raise TypeError("api_request field 'customer_id' must be a string")
    if not customer_id.strip():
        raise ValueError("api_request field 'customer_id' must not be empty or whitespace-only")

    customer_type = api_request["customer_type"]
    if not isinstance(customer_type, str):
        raise TypeError("api_request field 'customer_type' must be a string")
    if not customer_type.strip():
        raise ValueError("api_request field 'customer_type' must not be empty or whitespace-only")

    if not isinstance(api_request["runtime_template"], dict):
        raise TypeError("api_request field 'runtime_template' must be a dict")

    if not isinstance(api_request["scheduled_batches"], list):
        raise TypeError("api_request field 'scheduled_batches' must be a list")

    # Step 2: enforce hard-coded customer eligibility
    _validate_customer_eligibility(customer_id=customer_id, customer_type=customer_type)

    # Step 3: build MB3 schedule_request — exceptions propagate unchanged
    schedule_request = build_schedule_request_from_template(
        runtime_template=api_request["runtime_template"],
        scheduled_batches=api_request["scheduled_batches"],
    )

    # Step 4: execute failure boundary
    boundary_result = apply_runtime_failure_boundary(
        schedule_request=schedule_request,
        execution_registry=execution_registry,
    )

    # Step 5: map boundary_state → api_state and return closed response
    boundary_state = boundary_result["boundary_state"]
    if boundary_state == "completed":
        api_state = "completed"
    elif boundary_state == "failed":
        api_state = "failed"
    else:
        raise ValueError(f"Invalid boundary_state: {boundary_state}")

    return {
        "request_id": request_id,
        "api_state": api_state,
        "boundary_result": boundary_result,
    }
