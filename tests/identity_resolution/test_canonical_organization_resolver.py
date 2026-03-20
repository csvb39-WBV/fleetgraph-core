"""Tests for deterministic canonical organization resolution."""

import copy
import pathlib
import sys


PROJECT_ROOT = pathlib.Path(__file__).resolve().parents[2]
SRC_ROOT = PROJECT_ROOT / "src"

if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from fleetgraph_core.identity_resolution.canonical_organization_resolver import (
    assemble_canonical_organizations,
    build_canonical_organization_identity,
    build_canonical_organization_record,
    normalize_canonical_organization_name,
    resolve_canonical_organization_key,
    validate_organization_domain_candidates,
)


def sample_organization_domain_candidate(candidate_id="cand_a"):
    return {
        "organization_domain_candidate_id": f"org_{candidate_id}::alpha.com",
        "organization_candidate_id": f"org_{candidate_id}",
        "candidate_id": candidate_id,
        "seed_id": "seed_1",
        "source_id": "src_1",
        "source_label": "Alpha Systems",
        "organization_name": "Alpha Systems",
        "domain_candidate": "alpha.com",
        "base_url": "https://alpha.com",
        "query_text": "alpha query",
        "candidate_state": "domain_derived",
    }


def test_validate_organization_domain_candidates_accepts_valid_input():
    validate_organization_domain_candidates(
        [
            sample_organization_domain_candidate("cand_a"),
            sample_organization_domain_candidate("cand_b"),
        ]
    )


def test_validate_organization_domain_candidates_rejects_non_list_input():
    try:
        validate_organization_domain_candidates("invalid")
    except TypeError as error:
        assert str(error) == "domain_candidates must be a list"
    else:
        raise AssertionError("TypeError was not raised for invalid input")


def test_validate_organization_domain_candidates_rejects_missing_field():
    candidate = sample_organization_domain_candidate()
    del candidate["base_url"]

    try:
        validate_organization_domain_candidates([candidate])
    except ValueError as error:
        assert str(error) == "organization domain candidate is missing required fields: base_url"
    else:
        raise AssertionError("ValueError was not raised for missing field")


def test_validate_organization_domain_candidates_rejects_unknown_field():
    candidate = sample_organization_domain_candidate()
    candidate["extra_field"] = "x"

    try:
        validate_organization_domain_candidates([candidate])
    except ValueError as error:
        assert str(error) == "organization domain candidate contains unknown fields: extra_field"
    else:
        raise AssertionError("ValueError was not raised for unknown field")


def test_validate_organization_domain_candidates_rejects_incorrect_candidate_state():
    candidate = sample_organization_domain_candidate()
    candidate["candidate_state"] = "parsed"

    try:
        validate_organization_domain_candidates([candidate])
    except ValueError as error:
        assert str(error) == "candidate_state must be exactly 'domain_derived'"
    else:
        raise AssertionError("ValueError was not raised for bad candidate_state")


def test_normalize_canonical_organization_name_returns_stripped_name():
    candidate = sample_organization_domain_candidate()
    candidate["organization_name"] = "  Alpha Systems  "

    result = normalize_canonical_organization_name(candidate)
    assert result == "Alpha Systems"


def test_resolve_canonical_organization_key_constructs_key():
    candidate = sample_organization_domain_candidate()
    canonical_name = normalize_canonical_organization_name(candidate)

    result = resolve_canonical_organization_key(candidate, canonical_name)
    assert result == f"{candidate['organization_domain_candidate_id']}::canonical"


def test_resolve_canonical_organization_key_rejects_empty_canonical_name():
    candidate = sample_organization_domain_candidate()

    try:
        resolve_canonical_organization_key(candidate, "")
    except ValueError as error:
        assert str(error) == "canonical_name must be a non-empty string"
    else:
        raise AssertionError("ValueError was not raised for empty canonical_name")


def test_build_canonical_organization_record_builds_expected_canonical_output():
    candidate = sample_organization_domain_candidate()
    canonical_name = normalize_canonical_organization_name(candidate)
    canonical_key = resolve_canonical_organization_key(candidate, canonical_name)

    record = build_canonical_organization_record(candidate, canonical_name, canonical_key)

    assert record["canonical_organization_id"] == canonical_key
    assert record["organization_name"] == canonical_name
    assert record["candidate_state"] == "canonicalized"
    assert list(record.keys()) == [
        "canonical_organization_id",
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


def test_build_canonical_organization_record_rejects_empty_canonical_key():
    candidate = sample_organization_domain_candidate()
    canonical_name = normalize_canonical_organization_name(candidate)

    try:
        build_canonical_organization_record(candidate, canonical_name, "")
    except ValueError as error:
        assert str(error) == "canonical_key must be a non-empty string"
    else:
        raise AssertionError("ValueError was not raised for empty canonical_key")


def test_build_canonical_organization_identity_is_deterministic():
    candidate1 = sample_organization_domain_candidate("cand_b")
    candidate2 = sample_organization_domain_candidate("cand_a")

    records = assemble_canonical_organizations([candidate1, candidate2])

    assert [r["candidate_id"] for r in records] == ["cand_a", "cand_b"]

    identity1 = build_canonical_organization_identity(records[0])
    identity2 = build_canonical_organization_identity(records[1])

    assert identity1[0] == "alpha.com"
    assert identity1 != identity2


def test_assemble_canonical_organizations_rejects_duplicate_canonical_id():
    base = sample_organization_domain_candidate("cand_a")
    other = sample_organization_domain_candidate("cand_a")
    other["organization_domain_candidate_id"] = base["organization_domain_candidate_id"]

    try:
        assemble_canonical_organizations([base, other])
    except ValueError as error:
        assert str(error).startswith("duplicate canonical_organization_id detected:")
    else:
        raise AssertionError("ValueError was not raised for duplicate canonical_organization_id")


def test_assemble_canonical_organizations_does_not_mutate_inputs():
    candidates = [sample_organization_domain_candidate("cand_a"), sample_organization_domain_candidate("cand_b")]
    original = copy.deepcopy(candidates)

    _ = assemble_canonical_organizations(candidates)
    assert candidates == original
