"""
D13-MB3 Runtime Retrieval Guardrails Layer.

Validates bounded retrieval request inputs and deterministically decides whether
retrieval processing may proceed.

Pure in-memory Python with strict closed-schema validation.
No timestamps, no filesystem access, no external dependencies, no mutation.
"""

from __future__ import annotations

from typing import Any

_REQUIRED_FIELDS: frozenset[str] = frozenset({
    "requested_result_count",
    "requested_relationship_expansion_count",
    "requested_evidence_link_count",
    "max_result_count",
    "max_relationship_expansion_count",
    "max_evidence_link_count",
})

_INT_FIELDS: tuple[str, ...] = (
    "requested_result_count",
    "requested_relationship_expansion_count",
    "requested_evidence_link_count",
    "max_result_count",
    "max_relationship_expansion_count",
    "max_evidence_link_count",
)

_EXPECTED_OUTPUT_KEYS: tuple[str, ...] = (
    "status",
    "violations",
)

_VIOLATION_ORDER: tuple[str, ...] = (
    "result_count_limit_exceeded",
    "relationship_expansion_limit_exceeded",
    "evidence_link_limit_exceeded",
)


def _require_closed_schema(obj: dict[str, Any], required_fields: frozenset[str], label: str) -> None:
    present = set(obj.keys())

    missing = required_fields - present
    if missing:
        raise ValueError(f"{label} is missing required fields: {', '.join(sorted(missing))}")

    extra = present - required_fields
    if extra:
        raise ValueError(f"{label} contains unexpected fields: {', '.join(sorted(extra))}")


def build_runtime_retrieval_guardrails_response(
    guardrail_input: dict[str, Any],
) -> dict[str, Any]:
    """Build deterministic allow/reject guardrail decision for retrieval inputs."""
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

    if guardrail_input["requested_result_count"] > guardrail_input["max_result_count"]:
        violations.append("result_count_limit_exceeded")

    if (
        guardrail_input["requested_relationship_expansion_count"]
        > guardrail_input["max_relationship_expansion_count"]
    ):
        violations.append("relationship_expansion_limit_exceeded")

    if (
        guardrail_input["requested_evidence_link_count"]
        > guardrail_input["max_evidence_link_count"]
    ):
        violations.append("evidence_link_limit_exceeded")

    if violations:
        status = "reject"
        violations = [violation for violation in _VIOLATION_ORDER if violation in violations]
    else:
        status = "allow"
        violations = ["within_retrieval_limits"]

    response: dict[str, Any] = {
        "status": status,
        "violations": violations,
    }

    if tuple(response.keys()) != _EXPECTED_OUTPUT_KEYS:
        raise RuntimeError("internal error: retrieval guardrails response schema mismatch")

    return response
