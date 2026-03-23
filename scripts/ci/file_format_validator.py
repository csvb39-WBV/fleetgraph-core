"""
D12-MB1 CI File Format Validator.

Validates a builder delivery against a set of required and allowed file paths.

Pure validation logic — no filesystem writes, no external dependencies,
no randomness, no side effects.
"""

from __future__ import annotations

from typing import Any


def _is_invalid_path(path: str) -> bool:
    """Return True if path has structural problems."""
    if not isinstance(path, str) or not path:
        return True
    if "\x00" in path:
        return True
    if path.startswith("/"):
        return True
    parts = path.replace("\\", "/").split("/")
    if ".." in parts or "." in parts:
        return True
    return False


def validate_delivery(
    delivery: dict[str, str],
    required_files: frozenset[str],
    allowed_files: frozenset[str],
) -> dict[str, Any]:
    """Validate a builder delivery against required and allowed file sets.

    Args:
        delivery:       Mapping of file path -> file content for the delivery.
        required_files: File paths that must be present in the delivery.
        allowed_files:  Complete set of permitted file paths (must be a superset
                        of required_files).

    Returns:
        {"status": "pass" | "fail", "errors": [<message>, ...]}
        Key order is fixed; errors list is deterministic.
    """
    if not isinstance(delivery, dict):
        raise TypeError("delivery must be a dict")
    if not isinstance(required_files, frozenset):
        raise TypeError("required_files must be a frozenset")
    if not isinstance(allowed_files, frozenset):
        raise TypeError("allowed_files must be a frozenset")

    errors: list[str] = []

    # 1. Invalid file paths — structural problems in delivered paths
    invalid_paths = sorted(p for p in delivery if _is_invalid_path(p))
    for path in invalid_paths:
        errors.append(f"invalid file path: {path}")

    valid_delivery_paths: set[str] = {p for p in delivery if not _is_invalid_path(p)}

    # 2. Empty files — structurally valid but zero-content
    empty_paths = sorted(p for p in valid_delivery_paths if not delivery[p].strip())
    for path in empty_paths:
        errors.append(f"empty file: {path}")

    # 3. Missing required files — required but absent from delivery entirely
    missing = sorted(required_files - set(delivery.keys()))
    for path in missing:
        errors.append(f"missing required file: {path}")

    # 4. Unexpected files — present and valid path but not in allowed set
    unexpected = sorted(valid_delivery_paths - allowed_files)
    for path in unexpected:
        errors.append(f"unexpected file: {path}")

    status = "pass" if not errors else "fail"

    result: dict[str, Any] = {
        "status": status,
        "errors": errors,
    }

    assert tuple(result.keys()) == ("status", "errors"), (
        "internal error: validator response schema mismatch"
    )

    return result
