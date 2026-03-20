"""Tests for deterministic organization signal parsing."""

import copy
import pathlib
import sys


PROJECT_ROOT = pathlib.Path(__file__).resolve().parents[2]
SRC_ROOT = PROJECT_ROOT / "src"

if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from fleetgraph_core.entity_intake.organization_signal_parser import (
    assemble_organization_candidates,
    build_organization_candidate,
    parse_organization_name,
    validate_signal_candidates,
)


def test_validate_signal_candidates_accepts_valid_input():
    validate_signal_candidates(
        [
            {
                "candidate_id": "cand_b",
                "seed_id": "seed_b",
                "source_id": "src_b",
                "source_label": "Bravo Labs",
                "base_url": "https://bravo.example",
                "query_text": "bravo forum",
                "raw_signal_text": "Bravo Labs | bravo forum | https://bravo.example",
                "raw_signal_timestamp": "2024-01-01T01:02:03Z",
                "collection_state": "collected",
            },
            {
                "candidate_id": "cand_a",
                "seed_id": "seed_a",
                "source_id": "src_a",
                "source_label": "Alpha Systems",
                "base_url": "https://alpha.example",
                "query_text": "alpha forum",
                "raw_signal_text": "Alpha Systems | alpha forum | https://alpha.example",
                "raw_signal_timestamp": "2024-01-01T00:00:00Z",
                "collection_state": "collected",
            },
        ]
    )


def test_validate_signal_candidates_rejects_non_list_input():
    try:
        validate_signal_candidates("invalid")
    except TypeError as error:
        assert str(error) == "signal_candidates must be a list"
    else:
        raise AssertionError("TypeError was not raised for invalid signal_candidates")


def test_validate_signal_candidates_rejects_non_dictionary_entries():
    try:
        validate_signal_candidates(["invalid"])
    except TypeError as error:
        assert str(error) == "each signal candidate must be a dictionary"
    else:
        raise AssertionError("TypeError was not raised for invalid signal candidate")


def test_validate_signal_candidates_rejects_missing_required_fields():
    try:
        validate_signal_candidates(
            [
                {
                    "candidate_id": "cand_a",
                    "seed_id": "seed_a",
                    "source_id": "src_a",
                    "source_label": "Alpha Systems",
                    "base_url": "https://alpha.example",
                    "query_text": "alpha forum",
                    "raw_signal_text": "Alpha Systems | alpha forum | https://alpha.example",
                    "collection_state": "collected",
                }
            ]
        )
    except ValueError as error:
        assert str(error) == "signal candidate is missing required fields: raw_signal_timestamp"
    else:
        raise AssertionError("ValueError was not raised for missing signal fields")


def test_validate_signal_candidates_rejects_unknown_fields():
    try:
        validate_signal_candidates(
            [
                {
                    "candidate_id": "cand_a",
                    "seed_id": "seed_a",
                    "source_id": "src_a",
                    "source_label": "Alpha Systems",
                    "base_url": "https://alpha.example",
                    "query_text": "alpha forum",
                    "raw_signal_text": "Alpha Systems | alpha forum | https://alpha.example",
                    "raw_signal_timestamp": "2024-01-01T00:00:00Z",
                    "collection_state": "collected",
                    "extra": "value",
                }
            ]
        )
    except ValueError as error:
        assert str(error) == "signal candidate contains unknown fields: extra"
    else:
        raise AssertionError("ValueError was not raised for unknown signal fields")


def test_validate_signal_candidates_rejects_invalid_field_types():
    try:
        validate_signal_candidates(
            [
                {
                    "candidate_id": 1,
                    "seed_id": "seed_a",
                    "source_id": "src_a",
                    "source_label": "Alpha Systems",
                    "base_url": "https://alpha.example",
                    "query_text": "alpha forum",
                    "raw_signal_text": "Alpha Systems | alpha forum | https://alpha.example",
                    "raw_signal_timestamp": "2024-01-01T00:00:00Z",
                    "collection_state": "collected",
                }
            ]
        )
    except TypeError as error:
        assert str(error) == "candidate_id must be a non-empty string"
    else:
        raise AssertionError("TypeError was not raised for invalid signal field type")


def test_validate_signal_candidates_rejects_empty_required_strings():
    try:
        validate_signal_candidates(
            [
                {
                    "candidate_id": "",
                    "seed_id": "seed_a",
                    "source_id": "src_a",
                    "source_label": "Alpha Systems",
                    "base_url": "https://alpha.example",
                    "query_text": "alpha forum",
                    "raw_signal_text": "Alpha Systems | alpha forum | https://alpha.example",
                    "raw_signal_timestamp": "2024-01-01T00:00:00Z",
                    "collection_state": "collected",
                }
            ]
        )
    except ValueError as error:
        assert str(error) == "candidate_id must be a non-empty string"
    else:
        raise AssertionError("ValueError was not raised for empty signal field")


def test_validate_signal_candidates_rejects_invalid_collection_state():
    try:
        validate_signal_candidates(
            [
                {
                    "candidate_id": "cand_a",
                    "seed_id": "seed_a",
                    "source_id": "src_a",
                    "source_label": "Alpha Systems",
                    "base_url": "https://alpha.example",
                    "query_text": "alpha forum",
                    "raw_signal_text": "Alpha Systems | alpha forum | https://alpha.example",
                    "raw_signal_timestamp": "2024-01-01T00:00:00Z",
                    "collection_state": "pending_collection",
                }
            ]
        )
    except ValueError as error:
        assert str(error) == "collection_state must be exactly 'collected'"
    else:
        raise AssertionError("ValueError was not raised for invalid collection_state")


def test_parse_organization_name_is_deterministic_from_raw_signal_text():
    signal_candidate = {
        "candidate_id": "cand_a",
        "seed_id": "seed_a",
        "source_id": "src_a",
        "source_label": "Alpha Systems",
        "base_url": "https://alpha.example",
        "query_text": "alpha forum",
        "raw_signal_text": "Alpha Systems | alpha forum | https://alpha.example",
        "raw_signal_timestamp": "2024-01-01T00:00:00Z",
        "collection_state": "collected",
    }

    first_result = parse_organization_name(signal_candidate)
    second_result = parse_organization_name(signal_candidate)

    assert first_result == "Alpha Systems"
    assert first_result == second_result


def test_parse_organization_name_rejects_non_parseable_raw_signal_text():
    signal_candidate = {
        "candidate_id": "cand_a",
        "seed_id": "seed_a",
        "source_id": "src_a",
        "source_label": "Alpha Systems",
        "base_url": "https://alpha.example",
        "query_text": "alpha forum",
        "raw_signal_text": "invalid text without delimiters",
        "raw_signal_timestamp": "2024-01-01T00:00:00Z",
        "collection_state": "collected",
    }

    try:
        parse_organization_name(signal_candidate)
    except ValueError as error:
        assert str(error) == "no valid organization name could be derived from raw_signal_text"
    else:
        raise AssertionError("ValueError was not raised for non-parseable raw_signal_text")


def test_build_organization_candidate_returns_exact_required_fields():
    signal_candidate = {
        "candidate_id": "cand_a",
        "seed_id": "seed_a",
        "source_id": "src_a",
        "source_label": "Alpha Systems",
        "base_url": "https://alpha.example",
        "query_text": "alpha forum",
        "raw_signal_text": "Alpha Systems | alpha forum | https://alpha.example",
        "raw_signal_timestamp": "2024-01-01T00:00:00Z",
        "collection_state": "collected",
    }

    organization_candidate = build_organization_candidate(
        signal_candidate,
        "Alpha Systems",
    )

    assert list(organization_candidate.keys()) == [
        "organization_candidate_id",
        "candidate_id",
        "seed_id",
        "source_id",
        "source_label",
        "base_url",
        "query_text",
        "organization_name",
        "raw_signal_text",
        "raw_signal_timestamp",
        "candidate_state",
    ]
    assert organization_candidate["candidate_state"] == "parsed"


def test_assemble_organization_candidates_sorts_output_deterministically():
    result = assemble_organization_candidates(
        [
            {
                "candidate_id": "cand_b",
                "seed_id": "seed_b",
                "source_id": "src_b",
                "source_label": "Bravo Labs",
                "base_url": "https://bravo.example",
                "query_text": "bravo forum",
                "raw_signal_text": "Bravo Labs | bravo forum | https://bravo.example",
                "raw_signal_timestamp": "2024-01-01T01:02:03Z",
                "collection_state": "collected",
            },
            {
                "candidate_id": "cand_a",
                "seed_id": "seed_a",
                "source_id": "src_a",
                "source_label": "Alpha Systems",
                "base_url": "https://alpha.example",
                "query_text": "alpha forum",
                "raw_signal_text": "Alpha Systems | alpha forum | https://alpha.example",
                "raw_signal_timestamp": "2024-01-01T00:00:00Z",
                "collection_state": "collected",
            },
        ]
    )

    assert [item["candidate_id"] for item in result] == ["cand_a", "cand_b"]


def test_validate_signal_candidates_rejects_duplicate_candidate_ids():
    try:
        validate_signal_candidates(
            [
                {
                    "candidate_id": "cand_a",
                    "seed_id": "seed_a",
                    "source_id": "src_a",
                    "source_label": "Alpha Systems",
                    "base_url": "https://alpha.example",
                    "query_text": "alpha forum",
                    "raw_signal_text": "Alpha Systems | alpha forum | https://alpha.example",
                    "raw_signal_timestamp": "2024-01-01T00:00:00Z",
                    "collection_state": "collected",
                },
                {
                    "candidate_id": "cand_a",
                    "seed_id": "seed_b",
                    "source_id": "src_b",
                    "source_label": "Bravo Labs",
                    "base_url": "https://bravo.example",
                    "query_text": "bravo forum",
                    "raw_signal_text": "Bravo Labs | bravo forum | https://bravo.example",
                    "raw_signal_timestamp": "2024-01-01T01:02:03Z",
                    "collection_state": "collected",
                },
            ]
        )
    except ValueError as error:
        assert str(error) == "duplicate candidate_id detected: cand_a"
    else:
        raise AssertionError("ValueError was not raised for duplicate candidate_id")


def test_assemble_organization_candidates_does_not_mutate_inputs():
    signal_candidates = [
        {
            "candidate_id": "cand_b",
            "seed_id": "seed_b",
            "source_id": "src_b",
            "source_label": "Bravo Labs",
            "base_url": "https://bravo.example",
            "query_text": "bravo forum",
            "raw_signal_text": "Bravo Labs | bravo forum | https://bravo.example",
            "raw_signal_timestamp": "2024-01-01T01:02:03Z",
            "collection_state": "collected",
        },
        {
            "candidate_id": "cand_a",
            "seed_id": "seed_a",
            "source_id": "src_a",
            "source_label": "Alpha Systems",
            "base_url": "https://alpha.example",
            "query_text": "alpha forum",
            "raw_signal_text": "Alpha Systems | alpha forum | https://alpha.example",
            "raw_signal_timestamp": "2024-01-01T00:00:00Z",
            "collection_state": "collected",
        },
    ]

    original_signal_candidates = copy.deepcopy(signal_candidates)

    assemble_organization_candidates(signal_candidates)

    assert signal_candidates == original_signal_candidates
