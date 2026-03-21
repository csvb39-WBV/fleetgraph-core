"""
MB6-A Configuration / Template Layer.

Validates deterministic schedule template definitions and builds closed-schema
MB3 schedule_request dicts from a template and record batches.

Pure in-memory Python — no file loading, no persistence, no logging side
effects, no timestamps, no UUIDs, no randomness.
"""

from copy import deepcopy
from typing import Any

_TEMPLATE_REQUIRED_FIELDS: frozenset[str] = frozenset({
    "template_id",
    "template_scope",
    "default_schedule_id",
    "default_schedule_scope",
})

_TEMPLATE_STRING_FIELDS: tuple[str, ...] = (
    "template_id",
    "template_scope",
    "default_schedule_id",
    "default_schedule_scope",
)


def validate_runtime_template(runtime_template: Any) -> None:
    """
    Validate a runtime_template against the closed template schema.

    Raises:
        TypeError: if runtime_template is not a dict, or any field has the
                   wrong type.
        ValueError: if required fields are missing, extra fields are present,
                    or any string field is empty or whitespace-only.
    """
    if not isinstance(runtime_template, dict):
        raise TypeError("runtime_template must be a dict")

    present = set(runtime_template.keys())

    missing = _TEMPLATE_REQUIRED_FIELDS - present
    if missing:
        raise ValueError(
            f"runtime_template is missing required fields: {', '.join(sorted(missing))}"
        )

    extra = present - _TEMPLATE_REQUIRED_FIELDS
    if extra:
        raise ValueError(
            f"runtime_template contains unexpected fields: {', '.join(sorted(extra))}"
        )

    for field in _TEMPLATE_STRING_FIELDS:
        value = runtime_template[field]
        if not isinstance(value, str):
            raise TypeError(f"runtime_template field '{field}' must be a string")
        if not value.strip():
            raise ValueError(f"runtime_template field '{field}' must not be empty or whitespace-only")


def build_schedule_request_from_template(
    runtime_template: dict[str, Any],
    scheduled_batches: list[list[dict[str, Any]]],
) -> dict[str, Any]:
    """
    Build a closed-schema MB3 schedule_request from a validated template and
    record batches.

    Process:
    1. Validate runtime_template.
    2. Validate scheduled_batches shape.
    3. Deep-copy batches to ensure caller reference safety.
    4. Return closed-schema schedule_request ready for MB3.

    Args:
        runtime_template: Closed-schema template dict with template_id,
                          template_scope, default_schedule_id, and
                          default_schedule_scope.
        scheduled_batches: Ordered list of record-batch lists. Each batch is a
                           list of record dicts. May be empty.

    Returns:
        Closed-schema schedule_request dict:
        {
            "schedule_id": str,
            "schedule_scope": str,
            "scheduled_batches": list[list[dict]],
        }

    Raises:
        TypeError: if runtime_template or any field / batch entry has the wrong
                   type.
        ValueError: if runtime_template schema is invalid or any string field is
                    empty / whitespace-only.
    """
    validate_runtime_template(runtime_template)

    if not isinstance(scheduled_batches, list):
        raise TypeError("scheduled_batches must be a list")

    for i, batch in enumerate(scheduled_batches):
        if not isinstance(batch, list):
            raise TypeError(f"scheduled_batches[{i}] must be a list")
        for j, record in enumerate(batch):
            if not isinstance(record, dict):
                raise TypeError(f"scheduled_batches[{i}][{j}] must be a dict")

    return {
        "schedule_id": runtime_template["default_schedule_id"],
        "schedule_scope": runtime_template["default_schedule_scope"],
        "scheduled_batches": deepcopy(scheduled_batches),
    }
