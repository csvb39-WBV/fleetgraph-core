"""
D13-MB2 Runtime Resource Guardrails Layer.

Validates bounded runtime execution inputs and deterministically decides whether
processing may proceed.

Pure in-memory Python with strict closed-schema validation.
No timestamps, no filesystem access, no external dependencies, no mutation.
"""

from __future__ import annotations

from typing import Any

_REQUIRED_FIELDS: frozenset[str] = frozenset({
    "payload_size_bytes",
    "document_count",
    "requested_graph_depth",
    "max_payload_size_bytes",
    "max_document_count",
    "max_graph_depth",
})

_INT_FIELDS: tuple[str, ...] = (
    "payload_size_bytes",
    "document_count",
    "requested_graph_depth",
    "max_payload_size_bytes",
    "max_document_count",
    "max_graph_depth",
)

_EXPECTED_OUTPUT_KEYS: tuple[str, ...] = (
    "status",
    "violations",
)

_VIOLATION_ORDER: tuple[str, ...] = (
    "payload_size_limit_exceeded",
    "document_count_limit_exceeded",
    "graph_depth_limit_exceeded",
)


def _require_closed_schema(obj: dict[str, Any], required_fields: frozenset[str], label: str) -> None:
    present = set(obj.keys())

    missing = required_fields - present
    if missing:
        raise ValueError(f"{label} is missing required fields: {', '.join(sorted(missing))}")

    extra = present - required_fields
    if extra:
        raise ValueError(f"{label} contains unexpected fields: {', '.join(sorted(extra))}")


def build_runtime_resource_guardrails_response(
    guardrail_input: dict[str, Any],
) -> dict[str, Any]:
    """Build deterministic allow/reject guardrail decision for runtime inputs."""
    if not isinstance(guardrail_input, dict):
        raise TypeError("guardrail_input must be a dict")

    _require_closed_schema(guardrail_input, _REQUIRED_FIELDS, "guardrail_input")

    for field in _INT_FIELDS:
        value = guardrail_input[field]
        if not isinstance(value, int) or isinstance(value, bool):
            raise TypeError(f"guardrail_input field '{field}' must be an int")
        if value < 0:
            raise ValueError(f"guardrail_input field '{field}' must not be negative")

    violations: list[str] = []

    if guardrail_input["payload_size_bytes"] > guardrail_input["max_payload_size_bytes"]:
        violations.append("payload_size_limit_exceeded")

    if guardrail_input["document_count"] > guardrail_input["max_document_count"]:
        violations.append("document_count_limit_exceeded")

    if guardrail_input["requested_graph_depth"] > guardrail_input["max_graph_depth"]:
        violations.append("graph_depth_limit_exceeded")

    if violations:
        status = "reject"
        violations = [violation for violation in _VIOLATION_ORDER if violation in violations]
    else:
        status = "allow"
        violations = ["within_resource_limits"]

    response: dict[str, Any] = {
        "status": status,
        "violations": violations,
    }

    if tuple(response.keys()) != _EXPECTED_OUTPUT_KEYS:
        raise RuntimeError("internal error: resource guardrails response schema mismatch")

    return response
