"""
MB10 External API Layer.

Provides a transport-ready envelope boundary over MB7-A without introducing
web framework dependencies.
"""

from fleetgraph_core.runtime.execution_registry import ExecutionRegistry
from fleetgraph_core.runtime.runtime_api_adapter import apply_runtime_api_request

_ENVELOPE_REQUIRED_FIELDS: frozenset[str] = frozenset({"request"})


def handle_runtime_request(
    request_envelope: dict[str, object],
    execution_registry: ExecutionRegistry,
) -> dict[str, object]:
    """Validate request envelope, delegate to MB7-A, and wrap response."""
    if not isinstance(request_envelope, dict):
        raise TypeError("request_envelope must be a dict")

    present = set(request_envelope.keys())

    missing = _ENVELOPE_REQUIRED_FIELDS - present
    if missing:
        raise ValueError(
            f"request_envelope is missing required fields: {', '.join(sorted(missing))}"
        )

    extra = present - _ENVELOPE_REQUIRED_FIELDS
    if extra:
        raise ValueError(
            f"request_envelope contains unexpected fields: {', '.join(sorted(extra))}"
        )

    request = request_envelope["request"]
    if not isinstance(request, dict):
        raise TypeError("request_envelope field 'request' must be a dict")

    response = apply_runtime_api_request(
        api_request=request,
        execution_registry=execution_registry,
    )

    return {
        "response": response,
    }
