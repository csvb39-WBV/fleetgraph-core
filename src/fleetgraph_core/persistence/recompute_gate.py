"""
P13A-MB4 Recompute Gate / Policy Evaluator.

Deterministically decides whether stored artifacts may be reused or
recomputation is required.

Pure in-memory Python with strict validation.
No timestamps, no filesystem access, no external dependencies, no mutation.
"""

from __future__ import annotations

from typing import Any

from fleetgraph_core.persistence.ingestion_artifact_manifest import (
    build_ingestion_artifact_manifest,
)

_TOP_LEVEL_KEYS: tuple[str, ...] = ("stored_manifest", "requested_state")
_REQUESTED_STATE_KEYS: tuple[str, ...] = (
    "document_set_version",
    "pipeline_version",
    "schema_version",
    "source_hash",
    "force_reprocess",
)
_OUTPUT_KEYS: tuple[str, ...] = ("decision", "reasons")
_RECOMPUTE_REASON_ORDER: tuple[str, ...] = (
    "document_set_version_changed",
    "pipeline_version_changed",
    "schema_version_changed",
    "source_hash_changed",
    "force_reprocess_requested",
)


def evaluate_recompute_gate(gate_input: dict[str, Any]) -> dict[str, Any]:
    """Evaluate whether stored artifacts can be reused or must be recomputed."""
    if not isinstance(gate_input, dict):
        raise TypeError("gate_input must be a dict")

    present_top = set(gate_input.keys())
    required_top = set(_TOP_LEVEL_KEYS)

    missing_top = required_top - present_top
    if missing_top:
        raise ValueError(
            f"gate_input is missing required fields: {', '.join(sorted(missing_top))}"
        )

    extra_top = present_top - required_top
    if extra_top:
        raise ValueError(
            f"gate_input contains unexpected fields: {', '.join(sorted(extra_top))}"
        )

    stored_manifest = gate_input["stored_manifest"]
    if not isinstance(stored_manifest, dict):
        raise TypeError("gate_input field 'stored_manifest' must be of type dict")

    requested_state = gate_input["requested_state"]
    if not isinstance(requested_state, dict):
        raise TypeError("gate_input field 'requested_state' must be of type dict")

    present_requested = set(requested_state.keys())
    required_requested = set(_REQUESTED_STATE_KEYS)

    missing_requested = required_requested - present_requested
    if missing_requested:
        raise ValueError(
            "gate_input field 'requested_state' is missing required fields: "
            f"{', '.join(sorted(missing_requested))}"
        )

    extra_requested = present_requested - required_requested
    if extra_requested:
        raise ValueError(
            "gate_input field 'requested_state' contains unexpected fields: "
            f"{', '.join(sorted(extra_requested))}"
        )

    for field in (
        "document_set_version",
        "pipeline_version",
        "schema_version",
        "source_hash",
    ):
        value = requested_state[field]
        if not isinstance(value, str):
            raise TypeError(
                f"gate_input field 'requested_state.{field}' must be of type str"
            )
        if not value:
            raise ValueError(
                f"gate_input field 'requested_state.{field}' must be a non-empty string"
            )

    force_reprocess = requested_state["force_reprocess"]
    if not isinstance(force_reprocess, bool):
        raise TypeError(
            "gate_input field 'requested_state.force_reprocess' must be of type bool"
        )

    validated_manifest = build_ingestion_artifact_manifest(stored_manifest)

    reasons: list[str] = []

    if requested_state["document_set_version"] != validated_manifest["document_set_version"]:
        reasons.append("document_set_version_changed")

    if requested_state["pipeline_version"] != validated_manifest["pipeline_version"]:
        reasons.append("pipeline_version_changed")

    if requested_state["schema_version"] != validated_manifest["schema_version"]:
        reasons.append("schema_version_changed")

    if requested_state["source_hash"] != validated_manifest["source_hash"]:
        reasons.append("source_hash_changed")

    if force_reprocess:
        reasons.append("force_reprocess_requested")

    if reasons:
        decision = "recompute_required"
        # Keep reasons strictly bounded and deterministically ordered.
        reasons = [reason for reason in _RECOMPUTE_REASON_ORDER if reason in reasons]
    else:
        decision = "reuse_stored_artifacts"
        reasons = ["stored_artifacts_valid"]

    result: dict[str, Any] = {
        "decision": decision,
        "reasons": reasons,
    }

    assert tuple(result.keys()) == _OUTPUT_KEYS, (
        "internal error: recompute gate output schema mismatch"
    )

    return result
