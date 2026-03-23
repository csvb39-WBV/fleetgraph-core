"""
P13A-MB1 Ingestion Artifact Manifest Builder.

Builds a deterministic closed-schema manifest for persisted ingest outputs.

Pure in-memory Python with strict validation.
No filesystem access, no subprocess, no external dependencies, no timestamps.
"""

from __future__ import annotations

from typing import Any

_REQUIRED_KEYS: tuple[str, ...] = (
    "matter_id",
    "document_set_version",
    "ingestion_run_id",
    "pipeline_version",
    "schema_version",
    "source_hash",
    "artifact_keys",
)

_STRING_FIELDS: tuple[str, ...] = (
    "matter_id",
    "document_set_version",
    "ingestion_run_id",
    "pipeline_version",
    "schema_version",
    "source_hash",
)


def build_ingestion_artifact_manifest(manifest_input: dict[str, Any]) -> dict[str, Any]:
    """Validate and build a deterministic ingestion artifact manifest.

    Args:
        manifest_input: Closed-schema payload containing all required fields.

    Returns:
        Deterministic manifest dict with fixed key ordering and sorted
        artifact_keys.
    """
    if not isinstance(manifest_input, dict):
        raise TypeError("manifest_input must be a dict")

    present = set(manifest_input.keys())
    required = set(_REQUIRED_KEYS)

    missing = required - present
    if missing:
        raise ValueError(f"manifest_input is missing required fields: {', '.join(sorted(missing))}")

    extra = present - required
    if extra:
        raise ValueError(f"manifest_input contains unexpected fields: {', '.join(sorted(extra))}")

    for field in _STRING_FIELDS:
        value = manifest_input[field]
        if not isinstance(value, str):
            raise TypeError(f"manifest_input field '{field}' must be of type str")
        if not value:
            raise ValueError(f"manifest_input field '{field}' must be a non-empty string")

    artifact_keys = manifest_input["artifact_keys"]
    if not isinstance(artifact_keys, list):
        raise TypeError("manifest_input field 'artifact_keys' must be of type list")
    if not artifact_keys:
        raise ValueError("manifest_input field 'artifact_keys' must be a non-empty list")

    normalized_artifact_keys: list[str] = []
    for idx, key in enumerate(artifact_keys):
        if not isinstance(key, str):
            raise TypeError(
                f"manifest_input field 'artifact_keys' entry at index {idx} must be of type str"
            )
        if not key:
            raise ValueError(
                f"manifest_input field 'artifact_keys' entry at index {idx} must be a non-empty string"
            )
        normalized_artifact_keys.append(key)

    result: dict[str, Any] = {
        "matter_id": manifest_input["matter_id"],
        "document_set_version": manifest_input["document_set_version"],
        "ingestion_run_id": manifest_input["ingestion_run_id"],
        "pipeline_version": manifest_input["pipeline_version"],
        "schema_version": manifest_input["schema_version"],
        "source_hash": manifest_input["source_hash"],
        "artifact_keys": sorted(normalized_artifact_keys),
    }

    assert tuple(result.keys()) == _REQUIRED_KEYS, (
        "internal error: ingestion artifact manifest schema mismatch"
    )

    return result
