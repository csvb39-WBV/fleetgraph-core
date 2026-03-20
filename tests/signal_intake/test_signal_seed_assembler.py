"""Tests for deterministic signal seed assembly."""

import copy
import pathlib
import sys


PROJECT_ROOT = pathlib.Path(__file__).resolve().parents[2]
SRC_ROOT = PROJECT_ROOT / "src"

if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from fleetgraph_core.signal_intake.signal_seed_assembler import assemble_signal_seeds


def test_assemble_signal_seeds_builds_pending_collection_seed_records():
    result = assemble_signal_seeds(
        query_texts=["beta forum", "alpha forum"],
        source_catalog=[
            {
                "source_id": "src_b",
                "source_label": "Bravo",
                "base_url": "https://bravo.example",
                "channel_type": "web",
                "is_active": True,
            },
            {
                "source_id": "src_a",
                "source_label": "Alpha",
                "base_url": "https://alpha.example",
                "channel_type": "forum",
                "is_active": True,
            },
        ],
    )

    assert result == [
        {
            "seed_id": "src_a::alpha forum",
            "source_id": "src_a",
            "source_label": "Alpha",
            "base_url": "https://alpha.example",
            "query_text": "alpha forum",
            "collection_state": "pending_collection",
        },
        {
            "seed_id": "src_a::beta forum",
            "source_id": "src_a",
            "source_label": "Alpha",
            "base_url": "https://alpha.example",
            "query_text": "beta forum",
            "collection_state": "pending_collection",
        },
        {
            "seed_id": "src_b::alpha forum",
            "source_id": "src_b",
            "source_label": "Bravo",
            "base_url": "https://bravo.example",
            "query_text": "alpha forum",
            "collection_state": "pending_collection",
        },
        {
            "seed_id": "src_b::beta forum",
            "source_id": "src_b",
            "source_label": "Bravo",
            "base_url": "https://bravo.example",
            "query_text": "beta forum",
            "collection_state": "pending_collection",
        },
    ]


def test_assemble_signal_seeds_includes_exact_required_fields():
    result = assemble_signal_seeds(
        query_texts=["alpha forum"],
        source_catalog=[
            {
                "source_id": "src_a",
                "source_label": "Alpha",
                "base_url": "https://alpha.example",
                "channel_type": "forum",
                "is_active": True,
            }
        ],
    )

    assert list(result[0].keys()) == [
        "seed_id",
        "source_id",
        "source_label",
        "base_url",
        "query_text",
        "collection_state",
    ]
    assert result[0]["collection_state"] == "pending_collection"


def test_assemble_signal_seeds_rejects_invalid_query_input():
    try:
        assemble_signal_seeds(
            query_texts="alpha forum",
            source_catalog=[],
        )
    except TypeError as error:
        assert str(error) == "query_texts must be a list"
    else:
        raise AssertionError("TypeError was not raised for invalid query_texts")


def test_assemble_signal_seeds_rejects_invalid_source_catalog_input():
    try:
        assemble_signal_seeds(
            query_texts=["alpha forum"],
            source_catalog="invalid",
        )
    except TypeError as error:
        assert str(error) == "signal_sources must be a list"
    else:
        raise AssertionError("TypeError was not raised for invalid source catalog")


def test_assemble_signal_seeds_excludes_inactive_sources_deterministically():
    result = assemble_signal_seeds(
        query_texts=["alpha forum"],
        source_catalog=[
            {
                "source_id": "src_b",
                "source_label": "Bravo",
                "base_url": "https://bravo.example",
                "channel_type": "web",
                "is_active": False,
            },
            {
                "source_id": "src_a",
                "source_label": "Alpha",
                "base_url": "https://alpha.example",
                "channel_type": "forum",
                "is_active": True,
            },
        ],
    )

    assert result == [
        {
            "seed_id": "src_a::alpha forum",
            "source_id": "src_a",
            "source_label": "Alpha",
            "base_url": "https://alpha.example",
            "query_text": "alpha forum",
            "collection_state": "pending_collection",
        }
    ]


def test_assemble_signal_seeds_rejects_duplicate_query_texts():
    try:
        assemble_signal_seeds(
            query_texts=["alpha forum", "alpha forum"],
            source_catalog=[
                {
                    "source_id": "src_a",
                    "source_label": "Alpha",
                    "base_url": "https://alpha.example",
                    "channel_type": "forum",
                    "is_active": True,
                }
            ],
        )
    except ValueError as error:
        assert str(error) == "duplicate query_text detected: alpha forum"
    else:
        raise AssertionError("ValueError was not raised for duplicate query_text")


def test_assemble_signal_seeds_does_not_mutate_inputs():
    query_texts = ["beta forum", "alpha forum"]
    source_catalog = [
        {
            "source_id": "src_b",
            "source_label": "Bravo",
            "base_url": "https://bravo.example",
            "channel_type": "web",
            "is_active": True,
        },
        {
            "source_id": "src_a",
            "source_label": "Alpha",
            "base_url": "https://alpha.example",
            "channel_type": "forum",
            "is_active": True,
        },
    ]

    original_query_texts = copy.deepcopy(query_texts)
    original_source_catalog = copy.deepcopy(source_catalog)

    assemble_signal_seeds(query_texts=query_texts, source_catalog=source_catalog)

    assert query_texts == original_query_texts
    assert source_catalog == original_source_catalog
