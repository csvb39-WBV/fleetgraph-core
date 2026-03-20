"""Tests for deterministic raw signal candidate intake."""

import copy
import pathlib
import sys


PROJECT_ROOT = pathlib.Path(__file__).resolve().parents[2]
SRC_ROOT = PROJECT_ROOT / "src"

if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from fleetgraph_core.signal_intake.signal_candidate_intake import (
    assemble_signal_candidates,
    build_signal_candidate,
    simulate_raw_signal,
    validate_seed_records,
)


def test_validate_seed_records_accepts_valid_seed_records():
    validate_seed_records(
        [
            {
                "seed_id": "seed_b",
                "source_id": "src_b",
                "source_label": "Bravo",
                "base_url": "https://bravo.example",
                "query_text": "beta forum",
                "collection_state": "pending_collection",
            },
            {
                "seed_id": "seed_a",
                "source_id": "src_a",
                "source_label": "Alpha",
                "base_url": "https://alpha.example",
                "query_text": "alpha forum",
                "collection_state": "pending_collection",
            },
        ]
    )


def test_validate_seed_records_rejects_non_list_input():
    try:
        validate_seed_records("invalid")
    except TypeError as error:
        assert str(error) == "seed_records must be a list"
    else:
        raise AssertionError("TypeError was not raised for invalid seed_records")


def test_validate_seed_records_rejects_non_dictionary_entries():
    try:
        validate_seed_records(["invalid"])
    except TypeError as error:
        assert str(error) == "each seed record must be a dictionary"
    else:
        raise AssertionError("TypeError was not raised for invalid seed record")


def test_validate_seed_records_rejects_missing_required_fields():
    try:
        validate_seed_records(
            [
                {
                    "seed_id": "seed_a",
                    "source_id": "src_a",
                    "source_label": "Alpha",
                    "base_url": "https://alpha.example",
                    "collection_state": "pending_collection",
                }
            ]
        )
    except ValueError as error:
        assert str(error) == "seed record is missing required fields: query_text"
    else:
        raise AssertionError("ValueError was not raised for missing seed fields")


def test_validate_seed_records_rejects_unknown_fields():
    try:
        validate_seed_records(
            [
                {
                    "seed_id": "seed_a",
                    "source_id": "src_a",
                    "source_label": "Alpha",
                    "base_url": "https://alpha.example",
                    "query_text": "alpha forum",
                    "collection_state": "pending_collection",
                    "extra": "value",
                }
            ]
        )
    except ValueError as error:
        assert str(error) == "seed record contains unknown fields: extra"
    else:
        raise AssertionError("ValueError was not raised for unknown seed fields")


def test_validate_seed_records_rejects_invalid_field_types():
    try:
        validate_seed_records(
            [
                {
                    "seed_id": 1,
                    "source_id": "src_a",
                    "source_label": "Alpha",
                    "base_url": "https://alpha.example",
                    "query_text": "alpha forum",
                    "collection_state": "pending_collection",
                }
            ]
        )
    except TypeError as error:
        assert str(error) == "seed_id must be a non-empty string"
    else:
        raise AssertionError("TypeError was not raised for invalid seed field type")


def test_validate_seed_records_rejects_empty_required_strings():
    try:
        validate_seed_records(
            [
                {
                    "seed_id": "",
                    "source_id": "src_a",
                    "source_label": "Alpha",
                    "base_url": "https://alpha.example",
                    "query_text": "alpha forum",
                    "collection_state": "pending_collection",
                }
            ]
        )
    except ValueError as error:
        assert str(error) == "seed_id must be a non-empty string"
    else:
        raise AssertionError("ValueError was not raised for empty seed field")


def test_validate_seed_records_rejects_invalid_collection_state():
    try:
        validate_seed_records(
            [
                {
                    "seed_id": "seed_a",
                    "source_id": "src_a",
                    "source_label": "Alpha",
                    "base_url": "https://alpha.example",
                    "query_text": "alpha forum",
                    "collection_state": "collected",
                }
            ]
        )
    except ValueError as error:
        assert str(error) == "collection_state must be exactly 'pending_collection'"
    else:
        raise AssertionError("ValueError was not raised for invalid collection_state")


def test_validate_seed_records_rejects_duplicate_seed_ids():
    try:
        validate_seed_records(
            [
                {
                    "seed_id": "seed_a",
                    "source_id": "src_a",
                    "source_label": "Alpha",
                    "base_url": "https://alpha.example",
                    "query_text": "alpha forum",
                    "collection_state": "pending_collection",
                },
                {
                    "seed_id": "seed_a",
                    "source_id": "src_b",
                    "source_label": "Bravo",
                    "base_url": "https://bravo.example",
                    "query_text": "beta forum",
                    "collection_state": "pending_collection",
                },
            ]
        )
    except ValueError as error:
        assert str(error) == "duplicate seed_id detected: seed_a"
    else:
        raise AssertionError("ValueError was not raised for duplicate seed_id")


def test_simulate_raw_signal_is_deterministic_and_minimal():
    seed_record = {
        "seed_id": "seed_a",
        "source_id": "src_a",
        "source_label": "Alpha",
        "base_url": "https://alpha.example",
        "query_text": "alpha forum",
        "collection_state": "pending_collection",
    }

    first_result = simulate_raw_signal(seed_record)
    second_result = simulate_raw_signal(seed_record)

    assert first_result == second_result
    assert list(first_result.keys()) == [
        "raw_signal_text",
        "raw_signal_timestamp",
    ]


def test_build_signal_candidate_returns_exact_required_fields():
    seed_record = {
        "seed_id": "seed_a",
        "source_id": "src_a",
        "source_label": "Alpha",
        "base_url": "https://alpha.example",
        "query_text": "alpha forum",
        "collection_state": "pending_collection",
    }

    raw_signal = simulate_raw_signal(seed_record)
    candidate = build_signal_candidate(raw_signal, seed_record)

    assert list(candidate.keys()) == [
        "candidate_id",
        "seed_id",
        "source_id",
        "source_label",
        "base_url",
        "query_text",
        "raw_signal_text",
        "raw_signal_timestamp",
        "collection_state",
    ]
    assert candidate["collection_state"] == "collected"


def test_assemble_signal_candidates_sorts_candidates_deterministically():
    result = assemble_signal_candidates(
        [
            {
                "seed_id": "seed_b",
                "source_id": "src_b",
                "source_label": "Bravo",
                "base_url": "https://bravo.example",
                "query_text": "beta forum",
                "collection_state": "pending_collection",
            },
            {
                "seed_id": "seed_a",
                "source_id": "src_a",
                "source_label": "Alpha",
                "base_url": "https://alpha.example",
                "query_text": "alpha forum",
                "collection_state": "pending_collection",
            },
        ]
    )

    assert [candidate["seed_id"] for candidate in result] == ["seed_a", "seed_b"]


def test_build_signal_candidate_rejects_invalid_raw_signal_structure():
    seed_record = {
        "seed_id": "seed_a",
        "source_id": "src_a",
        "source_label": "Alpha",
        "base_url": "https://alpha.example",
        "query_text": "alpha forum",
        "collection_state": "pending_collection",
    }

    try:
        build_signal_candidate(
            {"raw_signal_text": "value"},
            seed_record,
        )
    except ValueError as error:
        assert str(error) == "raw_signal is missing required fields: raw_signal_timestamp"
    else:
        raise AssertionError("ValueError was not raised for invalid raw_signal")


def test_assemble_signal_candidates_does_not_mutate_inputs():
    seed_records = [
        {
            "seed_id": "seed_b",
            "source_id": "src_b",
            "source_label": "Bravo",
            "base_url": "https://bravo.example",
            "query_text": "beta forum",
            "collection_state": "pending_collection",
        },
        {
            "seed_id": "seed_a",
            "source_id": "src_a",
            "source_label": "Alpha",
            "base_url": "https://alpha.example",
            "query_text": "alpha forum",
            "collection_state": "pending_collection",
        },
    ]

    original_seed_records = copy.deepcopy(seed_records)

    assemble_signal_candidates(seed_records)

    assert seed_records == original_seed_records
