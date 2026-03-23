"""
D13-MB6 Storage Tiering Policy Evaluator.

Deterministically selects storage tier for persisted artifact categories based on
artifact category, access frequency, and retention class.

Pure in-memory Python with strict closed-schema validation.
No timestamps, no filesystem access, no external dependencies, no mutation.
"""

from __future__ import annotations

from typing import Any

_REQUIRED_FIELDS: frozenset[str] = frozenset({
    "artifact_category",
    "access_frequency",
    "retention_class",
})

_ALLOWED_ARTIFACT_CATEGORIES: frozenset[str] = frozenset({
    "manifest",
    "canonical_store",
    "retrieval_projection",
    "source_reference",
})

_ALLOWED_ACCESS_FREQUENCIES: frozenset[str] = frozenset({
    "high",
    "medium",
    "low",
})

_ALLOWED_RETENTION_CLASSES: frozenset[str] = frozenset({
    "active",
    "standard",
    "archive",
})

_EXPECTED_OUTPUT_KEYS: tuple[str, ...] = (
    "storage_tier",
    "reasons",
)


def _require_closed_schema(obj: dict[str, Any], required_fields: frozenset[str], label: str) -> None:
    present = set(obj.keys())

    missing = required_fields - present
    if missing:
        raise ValueError(f"{label} is missing required fields: {', '.join(sorted(missing))}")

    extra = present - required_fields
    if extra:
        raise ValueError(f"{label} contains unexpected fields: {', '.join(sorted(extra))}")


def evaluate_storage_tiering_policy(policy_input: dict[str, Any]) -> dict[str, Any]:
    """Evaluate deterministic storage tier and reason for a persisted artifact."""
    if not isinstance(policy_input, dict):
        raise TypeError("policy_input must be a dict")

    _require_closed_schema(policy_input, _REQUIRED_FIELDS, "policy_input")

    artifact_category = policy_input["artifact_category"]
    if not isinstance(artifact_category, str):
        raise TypeError("policy_input field 'artifact_category' must be a str")
    if not artifact_category:
        raise ValueError("policy_input field 'artifact_category' must be a non-empty string")
    if artifact_category not in _ALLOWED_ARTIFACT_CATEGORIES:
        raise ValueError(
            "policy_input field 'artifact_category' must be one of "
            f"{sorted(_ALLOWED_ARTIFACT_CATEGORIES)}"
        )

    access_frequency = policy_input["access_frequency"]
    if not isinstance(access_frequency, str):
        raise TypeError("policy_input field 'access_frequency' must be a str")
    if not access_frequency:
        raise ValueError("policy_input field 'access_frequency' must be a non-empty string")
    if access_frequency not in _ALLOWED_ACCESS_FREQUENCIES:
        raise ValueError(
            "policy_input field 'access_frequency' must be one of "
            f"{sorted(_ALLOWED_ACCESS_FREQUENCIES)}"
        )

    retention_class = policy_input["retention_class"]
    if not isinstance(retention_class, str):
        raise TypeError("policy_input field 'retention_class' must be a str")
    if not retention_class:
        raise ValueError("policy_input field 'retention_class' must be a non-empty string")
    if retention_class not in _ALLOWED_RETENTION_CLASSES:
        raise ValueError(
            "policy_input field 'retention_class' must be one of "
            f"{sorted(_ALLOWED_RETENTION_CLASSES)}"
        )

    if artifact_category == "retrieval_projection" and access_frequency == "high":
        storage_tier = "hot"
        reasons = ["retrieval_projection_high_access"]
    elif artifact_category == "source_reference" and retention_class == "archive":
        storage_tier = "cold"
        reasons = ["source_reference_archive_cold"]
    elif access_frequency == "low" and retention_class == "archive":
        storage_tier = "cold"
        reasons = ["low_access_archive_cold"]
    elif artifact_category == "manifest":
        storage_tier = "warm"
        reasons = ["manifest_default_warm"]
    elif artifact_category == "canonical_store":
        storage_tier = "warm"
        reasons = ["canonical_store_default_warm"]
    else:
        storage_tier = "warm"
        reasons = ["default_warm_tier"]

    response: dict[str, Any] = {
        "storage_tier": storage_tier,
        "reasons": reasons,
    }

    if tuple(response.keys()) != _EXPECTED_OUTPUT_KEYS:
        raise RuntimeError("internal error: storage tiering policy response schema mismatch")

    return response
