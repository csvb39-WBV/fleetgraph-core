"""Tests for deterministic organization domain extraction."""

import copy
import pathlib
import sys


PROJECT_ROOT = pathlib.Path(__file__).resolve().parents[2]
SRC_ROOT = PROJECT_ROOT / "src"

if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from fleetgraph_core.entity_intake.organization_domain_extractor import (
    assemble_organization_domain_candidates,
    build_organization_domain_candidate,
    derive_domain_candidate,
    validate_organization_candidates,
)


def test_validate_organization_candidates_accepts_valid_input():
    validate_organization_candidates(
        [
            {
                "organization_candidate_id": "orgcand_b",
                "candidate_id": "cand_b",
                "seed_id": "seed_b",
                "source_id": "src_b",
                "source_label": "Bravo Labs",
                "base_url": "https://bravo.example/path",
                "query_text": "bravo forum",
                "organization_name": "Bravo Labs",
                "raw_signal_text": "Bravo Labs | bravo forum | https://bravo.example",
                "raw_signal_timestamp": "2024-01-01T01:02:03Z",
                "candidate_state": "parsed",
            },
            {
                "organization_candidate_id": "orgcand_a",
                "candidate_id": "cand_a",
                "seed_id": "seed_a",
                "source_id": "src_a",
                "source_label": "Alpha Systems",
                "base_url": "https://alpha.example",
                "query_text": "alpha forum",
                "organization_name": "Alpha Systems",
                "raw_signal_text": "Alpha Systems | alpha forum | https://alpha.example",
                "raw_signal_timestamp": "2024-01-01T00:00:00Z",
                "candidate_state": "parsed",
            },
        ]
    )


def test_validate_organization_candidates_rejects_non_list_input():
    try:
        validate_organization_candidates("invalid")
    except TypeError as error:
        assert str(error) == "organization_candidates must be a list"
    else:
        raise AssertionError("TypeError was not raised for invalid organization_candidates")


def test_validate_organization_candidates_rejects_non_dictionary_entries():
    try:
        validate_organization_candidates(["invalid"])
    except TypeError as error:
        assert str(error) == "each organization candidate must be a dictionary"
    else:
        raise AssertionError("TypeError was not raised for invalid organization candidate")


def test_validate_organization_candidates_rejects_missing_required_fields():
    try:
        validate_organization_candidates(
            [
                {
                    "organization_candidate_id": "orgcand_a",
                    "candidate_id": "cand_a",
                    "seed_id": "seed_a",
                    "source_id": "src_a",
                    "source_label": "Alpha Systems",
                    "base_url": "https://alpha.example",
                    "query_text": "alpha forum",
                    "organization_name": "Alpha Systems",
                    "raw_signal_text": "Alpha Systems | alpha forum | https://alpha.example",
                    "candidate_state": "parsed",
                }
            ]
        )
    except ValueError as error:
        assert str(error) == "organization candidate is missing required fields: raw_signal_timestamp"
    else:
        raise AssertionError("ValueError was not raised for missing organization fields")


def test_validate_organization_candidates_rejects_unknown_fields():
    try:
        validate_organization_candidates(
            [
                {
                    "organization_candidate_id": "orgcand_a",
                    "candidate_id": "cand_a",
                    "seed_id": "seed_a",
                    "source_id": "src_a",
                    "source_label": "Alpha Systems",
                    "base_url": "https://alpha.example",
                    "query_text": "alpha forum",
                    "organization_name": "Alpha Systems",
                    "raw_signal_text": "Alpha Systems | alpha forum | https://alpha.example",
                    "raw_signal_timestamp": "2024-01-01T00:00:00Z",
                    "candidate_state": "parsed",
                    "extra": "value",
                }
            ]
        )
    except ValueError as error:
        assert str(error) == "organization candidate contains unknown fields: extra"
    else:
        raise AssertionError("ValueError was not raised for unknown organization fields")


def test_validate_organization_candidates_rejects_invalid_field_types():
    try:
        validate_organization_candidates(
            [
                {
                    "organization_candidate_id": 1,
                    "candidate_id": "cand_a",
                    "seed_id": "seed_a",
                    "source_id": "src_a",
                    "source_label": "Alpha Systems",
                    "base_url": "https://alpha.example",
                    "query_text": "alpha forum",
                    "organization_name": "Alpha Systems",
                    "raw_signal_text": "Alpha Systems | alpha forum | https://alpha.example",
                    "raw_signal_timestamp": "2024-01-01T00:00:00Z",
                    "candidate_state": "parsed",
                }
            ]
        )
    except TypeError as error:
        assert str(error) == "organization_candidate_id must be a non-empty string"
    else:
        raise AssertionError("TypeError was not raised for invalid organization field type")


def test_validate_organization_candidates_rejects_empty_required_strings():
    try:
        validate_organization_candidates(
            [
                {
                    "organization_candidate_id": "",
                    "candidate_id": "cand_a",
                    "seed_id": "seed_a",
                    "source_id": "src_a",
                    "source_label": "Alpha Systems",
                    "base_url": "https://alpha.example",
                    "query_text": "alpha forum",
                    "organization_name": "Alpha Systems",
                    "raw_signal_text": "Alpha Systems | alpha forum | https://alpha.example",
                    "raw_signal_timestamp": "2024-01-01T00:00:00Z",
                    "candidate_state": "parsed",
                }
            ]
        )
    except ValueError as error:
        assert str(error) == "organization_candidate_id must be a non-empty string"
    else:
        raise AssertionError("ValueError was not raised for empty organization field")


def test_validate_organization_candidates_rejects_invalid_candidate_state():
    try:
        validate_organization_candidates(
            [
                {
                    "organization_candidate_id": "orgcand_a",
                    "candidate_id": "cand_a",
                    "seed_id": "seed_a",
                    "source_id": "src_a",
                    "source_label": "Alpha Systems",
                    "base_url": "https://alpha.example",
                    "query_text": "alpha forum",
                    "organization_name": "Alpha Systems",
                    "raw_signal_text": "Alpha Systems | alpha forum | https://alpha.example",
                    "raw_signal_timestamp": "2024-01-01T00:00:00Z",
                    "candidate_state": "collected",
                }
            ]
        )
    except ValueError as error:
        assert str(error) == "candidate_state must be exactly 'parsed'"
    else:
        raise AssertionError("ValueError was not raised for invalid candidate_state")


def test_derive_domain_candidate_is_deterministic():
    organization_candidate = {
        "organization_candidate_id": "orgcand_a",
        "candidate_id": "cand_a",
        "seed_id": "seed_a",
        "source_id": "src_a",
        "source_label": "Alpha Systems",
        "base_url": "https://alpha.example/path",
        "query_text": "alpha forum",
        "organization_name": "Alpha Systems",
        "raw_signal_text": "Alpha Systems | alpha forum | https://alpha.example",
        "raw_signal_timestamp": "2024-01-01T00:00:00Z",
        "candidate_state": "parsed",
    }

    first_result = derive_domain_candidate(organization_candidate)
    second_result = derive_domain_candidate(organization_candidate)

    assert first_result == "alpha.example"
    assert first_result == second_result


def test_derive_domain_candidate_rejects_invalid_base_url():
    organization_candidate = {
        "organization_candidate_id": "orgcand_a",
        "candidate_id": "cand_a",
        "seed_id": "seed_a",
        "source_id": "src_a",
        "source_label": "Alpha Systems",
        "base_url": "invalid",
        "query_text": "alpha forum",
        "organization_name": "Alpha Systems",
        "raw_signal_text": "Alpha Systems | alpha forum | https://alpha.example",
        "raw_signal_timestamp": "2024-01-01T00:00:00Z",
        "candidate_state": "parsed",
    }

    try:
        derive_domain_candidate(organization_candidate)
    except ValueError as error:
        assert str(error) == "no valid domain candidate could be derived"
    else:
        raise AssertionError("ValueError was not raised for invalid domain candidate")


def test_build_organization_domain_candidate_returns_exact_required_fields():
    organization_candidate = {
        "organization_candidate_id": "orgcand_a",
        "candidate_id": "cand_a",
        "seed_id": "seed_a",
        "source_id": "src_a",
        "source_label": "Alpha Systems",
        "base_url": "https://alpha.example",
        "query_text": "alpha forum",
        "organization_name": "Alpha Systems",
        "raw_signal_text": "Alpha Systems | alpha forum | https://alpha.example",
        "raw_signal_timestamp": "2024-01-01T00:00:00Z",
        "candidate_state": "parsed",
    }

    domain_candidate_record = build_organization_domain_candidate(
        organization_candidate,
        "alpha.example",
    )

    assert list(domain_candidate_record.keys()) == [
        "organization_domain_candidate_id",
        "organization_candidate_id",
        "candidate_id",
        "seed_id",
        "source_id",
        "source_label",
        "organization_name",
        "domain_candidate",
        "base_url",
        "query_text",
        "candidate_state",
    ]
    assert domain_candidate_record["candidate_state"] == "domain_derived"


def test_assemble_organization_domain_candidates_sorts_output_deterministically():
    result = assemble_organization_domain_candidates(
        [
            {
                "organization_candidate_id": "orgcand_b",
                "candidate_id": "cand_b",
                "seed_id": "seed_b",
                "source_id": "src_b",
                "source_label": "Bravo Labs",
                "base_url": "https://bravo.example/path",
                "query_text": "bravo forum",
                "organization_name": "Bravo Labs",
                "raw_signal_text": "Bravo Labs | bravo forum | https://bravo.example",
                "raw_signal_timestamp": "2024-01-01T01:02:03Z",
                "candidate_state": "parsed",
            },
            {
                "organization_candidate_id": "orgcand_a",
                "candidate_id": "cand_a",
                "seed_id": "seed_a",
                "source_id": "src_a",
                "source_label": "Alpha Systems",
                "base_url": "https://alpha.example",
                "query_text": "alpha forum",
                "organization_name": "Alpha Systems",
                "raw_signal_text": "Alpha Systems | alpha forum | https://alpha.example",
                "raw_signal_timestamp": "2024-01-01T00:00:00Z",
                "candidate_state": "parsed",
            },
        ]
    )

    assert [item["organization_candidate_id"] for item in result] == [
        "orgcand_a",
        "orgcand_b",
    ]


def test_validate_organization_candidates_rejects_duplicate_organization_candidate_ids():
    try:
        validate_organization_candidates(
            [
                {
                    "organization_candidate_id": "orgcand_a",
                    "candidate_id": "cand_a",
                    "seed_id": "seed_a",
                    "source_id": "src_a",
                    "source_label": "Alpha Systems",
                    "base_url": "https://alpha.example",
                    "query_text": "alpha forum",
                    "organization_name": "Alpha Systems",
                    "raw_signal_text": "Alpha Systems | alpha forum | https://alpha.example",
                    "raw_signal_timestamp": "2024-01-01T00:00:00Z",
                    "candidate_state": "parsed",
                },
                {
                    "organization_candidate_id": "orgcand_a",
                    "candidate_id": "cand_b",
                    "seed_id": "seed_b",
                    "source_id": "src_b",
                    "source_label": "Bravo Labs",
                    "base_url": "https://bravo.example",
                    "query_text": "bravo forum",
                    "organization_name": "Bravo Labs",
                    "raw_signal_text": "Bravo Labs | bravo forum | https://bravo.example",
                    "raw_signal_timestamp": "2024-01-01T01:02:03Z",
                    "candidate_state": "parsed",
                },
            ]
        )
    except ValueError as error:
        assert str(error) == "duplicate organization_candidate_id detected: orgcand_a"
    else:
        raise AssertionError("ValueError was not raised for duplicate organization_candidate_id")


def test_assemble_organization_domain_candidates_does_not_mutate_inputs():
    organization_candidates = [
        {
            "organization_candidate_id": "orgcand_b",
            "candidate_id": "cand_b",
            "seed_id": "seed_b",
            "source_id": "src_b",
            "source_label": "Bravo Labs",
            "base_url": "https://bravo.example/path",
            "query_text": "bravo forum",
            "organization_name": "Bravo Labs",
            "raw_signal_text": "Bravo Labs | bravo forum | https://bravo.example",
            "raw_signal_timestamp": "2024-01-01T01:02:03Z",
            "candidate_state": "parsed",
        },
        {
            "organization_candidate_id": "orgcand_a",
            "candidate_id": "cand_a",
            "seed_id": "seed_a",
            "source_id": "src_a",
            "source_label": "Alpha Systems",
            "base_url": "https://alpha.example",
            "query_text": "alpha forum",
            "organization_name": "Alpha Systems",
            "raw_signal_text": "Alpha Systems | alpha forum | https://alpha.example",
            "raw_signal_timestamp": "2024-01-01T00:00:00Z",
            "candidate_state": "parsed",
        },
    ]

    original_organization_candidates = copy.deepcopy(organization_candidates)

    assemble_organization_domain_candidates(organization_candidates)

    assert organization_candidates == original_organization_candidates
