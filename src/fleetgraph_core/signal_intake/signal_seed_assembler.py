"""Deterministic signal seed assembly."""

from copy import deepcopy

from fleetgraph_core.signal_intake.signal_query_compiler import build_query_identity
from fleetgraph_core.signal_intake.signal_source_catalog import (
    REQUIRED_SOURCE_FIELDS,
    build_source_identity,
    validate_signal_sources,
)


SEED_FIELDS = (
    "seed_id",
    "source_id",
    "source_label",
    "base_url",
    "query_text",
    "collection_state",
)


def build_seed_identity(signal_seed: dict) -> tuple:
    """Build a deterministic seed identity tuple."""
    if not isinstance(signal_seed, dict):
        raise TypeError("signal_seed must be a dictionary")

    return (
        signal_seed["source_id"],
        signal_seed["query_text"],
    )


def validate_seed_assembly_inputs(query_texts: list[str], source_catalog: list[dict]) -> None:
    """Validate inputs before seed assembly."""
    if not isinstance(query_texts, list):
        raise TypeError("query_texts must be a list")

    seen_query_texts = set()
    for query_text in query_texts:
        if not isinstance(query_text, str):
            raise TypeError("each query_text must be a non-empty string")
        if query_text.strip() == "":
            raise ValueError("each query_text must be a non-empty string")

        normalized_query_text = query_text.strip()
        if normalized_query_text in seen_query_texts:
            raise ValueError(f"duplicate query_text detected: {normalized_query_text}")

        seen_query_texts.add(normalized_query_text)
        build_query_identity(normalized_query_text)

    validate_signal_sources(source_catalog)

    for signal_source in source_catalog:
        field_names = tuple(signal_source.keys())
        if set(field_names) != set(REQUIRED_SOURCE_FIELDS):
            raise ValueError("source_catalog entries must contain only required fields")


def assemble_signal_seeds(query_texts: list[str], source_catalog: list[dict]) -> list[dict]:
    """Assemble deterministic pending-collection seeds from queries and active sources."""
    validate_seed_assembly_inputs(query_texts, source_catalog)

    normalized_query_texts = sorted(
        {query_text.strip() for query_text in deepcopy(query_texts)},
        key=build_query_identity,
    )

    canonical_sources = []
    for signal_source in deepcopy(source_catalog):
        canonical_source = {
            "source_id": signal_source["source_id"].strip(),
            "source_label": signal_source["source_label"].strip(),
            "base_url": signal_source["base_url"].strip(),
            "channel_type": signal_source["channel_type"].strip(),
            "is_active": signal_source["is_active"],
        }
        canonical_sources.append(canonical_source)

    active_sources = []
    for signal_source in canonical_sources:
        if signal_source["is_active"]:
            active_sources.append(signal_source)

    sorted_sources = sorted(active_sources, key=build_source_identity)

    seeds = []
    seen_seed_ids = set()

    for query_text in normalized_query_texts:
        for signal_source in sorted_sources:
            seed_id = f"{signal_source['source_id']}::{query_text}"
            if seed_id in seen_seed_ids:
                raise ValueError(f"duplicate seed_id detected: {seed_id}")

            seed_record = {
                "seed_id": seed_id,
                "source_id": signal_source["source_id"],
                "source_label": signal_source["source_label"],
                "base_url": signal_source["base_url"],
                "query_text": query_text,
                "collection_state": "pending_collection",
            }

            if tuple(seed_record.keys()) != SEED_FIELDS:
                raise ValueError("seed record fields must match the required contract exactly")

            seeds.append(seed_record)
            seen_seed_ids.add(seed_id)

    return deepcopy(sorted(seeds, key=build_seed_identity))
