"""
D12-MB3 CI Determinism Harness.

Validates whether repeated calls to a callable with identical input
produce identical output.

Pure validation logic — no I/O, no filesystem access, no external
dependencies, no randomness, no side effects.
"""

from __future__ import annotations

from copy import deepcopy
from typing import Any, Callable


def run_determinism_check(
    callable_under_test: Callable[..., Any],
    input_payload: Any,
    repeat_count: int,
) -> dict[str, Any]:
    """Call callable_under_test repeat_count times with the same input and
    verify every output exactly matches the first.

    Args:
        callable_under_test: Any callable to exercise.
        input_payload:       Input passed unchanged to every call.
        repeat_count:        Number of invocations; must be an int >= 2.

    Returns:
        {"status": "pass" | "fail", "errors": [<message>, ...]}
        Key order is fixed; errors list is deterministic.
    """
    if not callable(callable_under_test):
        raise TypeError("callable_under_test must be callable")

    if isinstance(repeat_count, bool) or not isinstance(repeat_count, int):
        raise TypeError("repeat_count must be an int")

    if repeat_count < 2:
        raise ValueError("repeat_count must be >= 2")

    errors: list[str] = []

    # Snapshot the input before any calls so we can verify non-mutation later.
    # We do NOT mutate it ourselves.
    baseline_output: Any = None
    baseline_captured = False

    for run_index in range(repeat_count):
        try:
            output = callable_under_test(input_payload)
        except Exception as exc:
            errors.append(
                f"run {run_index + 1} raised an exception: "
                f"{type(exc).__name__}: {exc}"
            )
            # After a raise we cannot compare; stop collecting further errors
            # to keep the error list stable and minimal.
            break

        if not baseline_captured:
            baseline_output = output
            baseline_captured = True
            continue

        if output != baseline_output:
            errors.append(
                f"run {run_index + 1} output differs from run 1"
            )

    status = "pass" if not errors else "fail"
    result: dict[str, Any] = {
        "status": status,
        "errors": errors,
    }

    assert tuple(result.keys()) == ("status", "errors"), (
        "internal error: harness response schema mismatch"
    )

    return result
