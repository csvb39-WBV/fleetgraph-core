"""
D13-MB4 Runtime Query Cost Classification Layer.

Classifies bounded retrieval requests into deterministic cost tiers.

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
    "classification",
    "reasons",
)

_REJECT_REASON_ORDER: tuple[str, ...] = (
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


def _is_above_percent(requested: int, max_allowed: int, percent: int) -> bool:
    # Integer-only threshold check to avoid float behavior drift.
    return requested * 100 > max_allowed * percent


def build_runtime_query_cost_classification(
    classifier_input: dict[str, Any],
) -> dict[str, Any]:
    """Build deterministic query cost classification for bounded retrieval input."""
    if not isinstance(classifier_input, dict):
        raise TypeError("classifier_input must be a dict")

    _require_closed_schema(classifier_input, _REQUIRED_FIELDS, "classifier_input")

    for field in _INT_FIELDS:
        value = classifier_input[field]
        if not isinstance(value, int) or isinstance(value, bool):
            raise TypeError(f"classifier_input field '{field}' must be an int")
        if value < 0:
            raise ValueError(f"classifier_input field '{field}' must not be negative")

    reject_reasons: list[str] = []

    if classifier_input["requested_result_count"] > classifier_input["max_result_count"]:
        reject_reasons.append("result_count_limit_exceeded")

    if (
        classifier_input["requested_relationship_expansion_count"]
        > classifier_input["max_relationship_expansion_count"]
    ):
        reject_reasons.append("relationship_expansion_limit_exceeded")

    if (
        classifier_input["requested_evidence_link_count"]
        > classifier_input["max_evidence_link_count"]
    ):
        reject_reasons.append("evidence_link_limit_exceeded")

    if reject_reasons:
        classification = "reject"
        reasons = [reason for reason in _REJECT_REASON_ORDER if reason in reject_reasons]
    else:
        any_above_75 = any(
            (
                _is_above_percent(
                    classifier_input["requested_result_count"],
                    classifier_input["max_result_count"],
                    75,
                ),
                _is_above_percent(
                    classifier_input["requested_relationship_expansion_count"],
                    classifier_input["max_relationship_expansion_count"],
                    75,
                ),
                _is_above_percent(
                    classifier_input["requested_evidence_link_count"],
                    classifier_input["max_evidence_link_count"],
                    75,
                ),
            )
        )

        if any_above_75:
            classification = "high_cost"
            reasons = ["within_high_cost_range"]
        else:
            any_above_25 = any(
                (
                    _is_above_percent(
                        classifier_input["requested_result_count"],
                        classifier_input["max_result_count"],
                        25,
                    ),
                    _is_above_percent(
                        classifier_input["requested_relationship_expansion_count"],
                        classifier_input["max_relationship_expansion_count"],
                        25,
                    ),
                    _is_above_percent(
                        classifier_input["requested_evidence_link_count"],
                        classifier_input["max_evidence_link_count"],
                        25,
                    ),
                )
            )

            if any_above_25:
                classification = "medium_cost"
                reasons = ["within_medium_cost_range"]
            else:
                classification = "low_cost"
                reasons = ["within_low_cost_range"]

    response: dict[str, Any] = {
        "classification": classification,
        "reasons": reasons,
    }

    if tuple(response.keys()) != _EXPECTED_OUTPUT_KEYS:
        raise RuntimeError("internal error: query cost classifier response schema mismatch")

    return response
