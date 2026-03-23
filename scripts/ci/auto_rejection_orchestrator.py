"""
D12-MB4 CI Auto-Rejection Orchestrator.

Combines precomputed results from file_format, schema, and determinism checks
into a single deterministic rejection decision.

Pure validation logic — no I/O, no filesystem access, no subprocess,
no external dependencies, no randomness, no side effects.
"""

from __future__ import annotations

from typing import Any

_COMPONENT_KEYS: tuple[str, ...] = ("file_format", "schema", "determinism")
_VALID_STATUSES: frozenset[str] = frozenset({"pass", "fail"})
_REQUIRED_COMPONENT_FIELDS: frozenset[str] = frozenset({"status", "errors"})


def run_auto_rejection(check_results: Any) -> dict[str, Any]:
    """Evaluate precomputed check results and return an aggregated decision.

    Args:
        check_results: Must be a dict with exactly the keys
                       "file_format", "schema", "determinism".  Each value
                       must be a dict with "status" ("pass"|"fail") and
                       "errors" (list).

    Returns:
        {"status": "pass" | "fail", "errors": [<message>, ...]}
        Key order is fixed; errors are aggregated in the order:
        file_format → schema → determinism.

    Raises:
        TypeError:  if check_results is not a dict, or a component value is
                    not a dict.
        ValueError: if required component keys are missing, unexpected keys
                    are present, required sub-keys are missing, status is
                    invalid, or errors is not a list.
    """
    # -----------------------------------------------------------------------
    # Top-level type guard
    # -----------------------------------------------------------------------
    if not isinstance(check_results, dict):
        raise TypeError("check_results must be a dict")

    # -----------------------------------------------------------------------
    # Key presence / absence validation
    # -----------------------------------------------------------------------
    input_keys = set(check_results.keys())
    expected_keys = set(_COMPONENT_KEYS)

    missing = expected_keys - input_keys
    if missing:
        missing_sorted = sorted(missing)
        raise ValueError(
            f"check_results is missing component keys: {missing_sorted}"
        )

    extra = input_keys - expected_keys
    if extra:
        extra_sorted = sorted(extra)
        raise ValueError(
            f"check_results contains unexpected component keys: {extra_sorted}"
        )

    # -----------------------------------------------------------------------
    # Per-component validation
    # -----------------------------------------------------------------------
    for key in _COMPONENT_KEYS:
        component = check_results[key]

        if not isinstance(component, dict):
            raise TypeError(
                f"component '{key}' must be a dict, got {type(component).__name__}"
            )

        component_keys = set(component.keys())
        missing_fields = _REQUIRED_COMPONENT_FIELDS - component_keys
        if missing_fields:
            raise ValueError(
                f"component '{key}' is missing required fields: "
                f"{sorted(missing_fields)}"
            )

        status = component["status"]
        if status not in _VALID_STATUSES:
            raise ValueError(
                f"component '{key}' has invalid status {status!r}; "
                f"must be 'pass' or 'fail'"
            )

        errors_value = component["errors"]
        if not isinstance(errors_value, list):
            raise ValueError(
                f"component '{key}' errors must be a list, "
                f"got {type(errors_value).__name__}"
            )

    # -----------------------------------------------------------------------
    # Aggregate
    # -----------------------------------------------------------------------
    aggregated_errors: list[str] = []
    for key in _COMPONENT_KEYS:
        aggregated_errors.extend(check_results[key]["errors"])

    overall_pass = all(
        check_results[key]["status"] == "pass" for key in _COMPONENT_KEYS
    )

    result: dict[str, Any] = {
        "status": "pass" if overall_pass else "fail",
        "errors": aggregated_errors,
    }
    assert tuple(result.keys()) == ("status", "errors")
    return result
