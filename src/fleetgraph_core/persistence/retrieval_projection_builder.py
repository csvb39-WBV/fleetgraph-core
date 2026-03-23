"""
P13A-MB3 Retrieval Projection Builder.

Builds deterministic retrieval projections from a canonical artifact store.

Pure in-memory Python with strict validation.
No timestamps, no I/O, no external dependencies, no mutation.
"""

from __future__ import annotations

import json
from typing import Any

from fleetgraph_core.persistence.canonical_artifact_store import (
    build_canonical_artifact_store,
)

_TOP_LEVEL_KEYS: tuple[str, ...] = ("manifest", "projections")
_PROJECTION_KEYS: tuple[str, ...] = (
    "entity_index",
    "event_timeline",
    "relationship_index",
)


def _stable_sort_key(item: Any) -> str:
    return json.dumps(item, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def _sorted_projection(items: list[Any]) -> list[Any]:
    if not isinstance(items, list):
        raise TypeError("projection source must be a list")
    return sorted(list(items), key=_stable_sort_key)


def build_retrieval_projection(canonical_store_input: dict[str, Any]) -> dict[str, Any]:
    """Validate canonical store and build deterministic retrieval projections."""
    canonical_store = build_canonical_artifact_store(canonical_store_input)

    artifacts = canonical_store["artifacts"]
    result: dict[str, Any] = {
        "manifest": canonical_store["manifest"],
        "projections": {
            "entity_index": _sorted_projection(artifacts["entities"]),
            "event_timeline": _sorted_projection(artifacts["events"]),
            "relationship_index": _sorted_projection(artifacts["relationships"]),
        },
    }

    if tuple(result.keys()) != _TOP_LEVEL_KEYS:
        raise RuntimeError("internal error: retrieval projection top-level schema mismatch")
    if tuple(result["projections"].keys()) != _PROJECTION_KEYS:
        raise RuntimeError("internal error: retrieval projection schema mismatch")

    for key in _PROJECTION_KEYS:
        if not isinstance(result["projections"][key], list):
            raise RuntimeError(
                f"internal error: projection '{key}' must be a list"
            )

    return result
