"""
P13A-MB2 Canonical Artifact Store Builder.

Builds a deterministic closed-schema canonical artifact store for computed
ingest outputs.

Pure in-memory Python with strict validation.
No filesystem access, no subprocess, no external dependencies, no timestamps.
"""

from __future__ import annotations

from typing import Any

from fleetgraph_core.persistence.ingestion_artifact_manifest import (
    build_ingestion_artifact_manifest,
)

_TOP_LEVEL_KEYS: tuple[str, ...] = ("manifest", "artifacts")
_ARTIFACT_BUCKET_KEYS: tuple[str, ...] = (
    "entities",
    "events",
    "relationships",
    "evidence_links",
    "graph_artifacts",
)


def build_canonical_artifact_store(store_input: dict[str, Any]) -> dict[str, Any]:
    """Validate and build a deterministic canonical artifact store.

    Args:
        store_input: Closed-schema payload with top-level keys:
                     "manifest" and "artifacts".

    Returns:
        Deterministic store dict:
        {
            "manifest": <validated manifest>,
            "artifacts": {
                "entities": [ ... ],
                "events": [ ... ],
                "relationships": [ ... ],
                "evidence_links": [ ... ],
                "graph_artifacts": [ ... ],
            },
        }
    """
    if not isinstance(store_input, dict):
        raise TypeError("store_input must be a dict")

    present_top = set(store_input.keys())
    required_top = set(_TOP_LEVEL_KEYS)

    missing_top = required_top - present_top
    if missing_top:
        raise ValueError(
            f"store_input is missing required fields: {', '.join(sorted(missing_top))}"
        )

    extra_top = present_top - required_top
    if extra_top:
        raise ValueError(
            f"store_input contains unexpected fields: {', '.join(sorted(extra_top))}"
        )

    manifest_input = store_input["manifest"]
    if not isinstance(manifest_input, dict):
        raise TypeError("store_input field 'manifest' must be of type dict")

    artifacts_input = store_input["artifacts"]
    if not isinstance(artifacts_input, dict):
        raise TypeError("store_input field 'artifacts' must be of type dict")

    present_buckets = set(artifacts_input.keys())
    required_buckets = set(_ARTIFACT_BUCKET_KEYS)

    missing_buckets = required_buckets - present_buckets
    if missing_buckets:
        raise ValueError(
            "store_input field 'artifacts' is missing required buckets: "
            f"{', '.join(sorted(missing_buckets))}"
        )

    extra_buckets = present_buckets - required_buckets
    if extra_buckets:
        raise ValueError(
            "store_input field 'artifacts' contains unexpected buckets: "
            f"{', '.join(sorted(extra_buckets))}"
        )

    validated_manifest = build_ingestion_artifact_manifest(manifest_input)

    normalized_artifacts: dict[str, list[Any]] = {}
    for bucket in _ARTIFACT_BUCKET_KEYS:
        value = artifacts_input[bucket]
        if not isinstance(value, list):
            raise TypeError(
                f"store_input field 'artifacts.{bucket}' must be of type list"
            )
        normalized_artifacts[bucket] = list(value)

    result: dict[str, Any] = {
        "manifest": validated_manifest,
        "artifacts": {
            "entities": normalized_artifacts["entities"],
            "events": normalized_artifacts["events"],
            "relationships": normalized_artifacts["relationships"],
            "evidence_links": normalized_artifacts["evidence_links"],
            "graph_artifacts": normalized_artifacts["graph_artifacts"],
        },
    }

    assert tuple(result.keys()) == _TOP_LEVEL_KEYS, (
        "internal error: canonical artifact store top-level schema mismatch"
    )
    assert tuple(result["artifacts"].keys()) == _ARTIFACT_BUCKET_KEYS, (
        "internal error: canonical artifact store artifact bucket schema mismatch"
    )

    return result
