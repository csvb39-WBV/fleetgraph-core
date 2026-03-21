"""Tests for deterministic canonical organization duplicate suppression."""

import copy
import pathlib
import sys


PROJECT_ROOT = pathlib.Path(__file__).resolve().parents[2]
SRC_ROOT = PROJECT_ROOT / "src"

if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from fleetgraph_core.identity_resolution.canonical_organization_deduper import (
    build_deduplication_key,
    deduplicate_canonical_organizations,
    suppress_duplicate_canonical_organizations,
    validate_canonical_organizations,
)


def sample_canonical_record(canonical_id="can_1", key="key1", name="Org A", domain="a.com", source="src1"):
    return {
        "canonical_organization_id": canonical_id,
        "organization_domain_candidate_id": "dom_" + canonical_id,
        "organization_candidate_id": "org_" + canonical_id,
        "candidate_id": "cand_" + canonical_id,
        "seed_id": "seed1",
        "source_id": source,
        "source_label": "Label",
        "canonical_organization_name": name,
        "canonical_organization_key": key,
        "domain_candidate": domain,
        "candidate_state": "canonicalized",
    }


def test_validate_canonical_organizations_accepts_valid_input():
    validate_canonical_organizations(
        [
            sample_canonical_record("can_1"),
            sample_canonical_record("can_2"),
        ]
    )


def test_validate_canonical_organizations_rejects_non_list_input():
    try:
        validate_canonical_organizations("invalid")
    except TypeError as error:
        assert str(error) == "canonical_records must be a list"
    else:
        raise AssertionError("TypeError was not raised for invalid input")


def test_validate_canonical_organizations_rejects_missing_field():
    record = sample_canonical_record()
    del record["canonical_organization_name"]

    try:
        validate_canonical_organizations([record])
    except ValueError as error:
        assert str(error) == "canonical record is missing required fields: canonical_organization_name"
    else:
        raise AssertionError("ValueError was not raised for missing field")


def test_validate_canonical_organizations_rejects_unknown_field():
    record = sample_canonical_record()
    record["extra_field"] = "x"

    try:
        validate_canonical_organizations([record])
    except ValueError as error:
        assert str(error) == "canonical record contains unknown fields: extra_field"
    else:
        raise AssertionError("ValueError was not raised for unknown field")


def test_validate_canonical_organizations_rejects_incorrect_candidate_state():
    record = sample_canonical_record()
    record["candidate_state"] = "parsed"

    try:
        validate_canonical_organizations([record])
    except ValueError as error:
        assert str(error) == "candidate_state must be exactly 'canonicalized'"
    else:
        raise AssertionError("ValueError was not raised for bad candidate_state")


def test_validate_canonical_organizations_allows_duplicate_canonical_organization_id():
    record1 = sample_canonical_record("can_1", key="key1")
    record2 = sample_canonical_record("can_1", key="key2")

    validate_canonical_organizations([record1, record2])


def test_build_deduplication_key_constructs_key():
    record = sample_canonical_record(key="key1")
    key = build_deduplication_key(record)
    assert key == ("key1",)


def test_deduplicate_canonical_organizations_keeps_first_seen_duplicate_key():
    records = [
        sample_canonical_record("can_1", "key1", "Org A", "a.com", "src1"),
        sample_canonical_record("can_2", "key1", "Org A Updated", "a.com", "src2"),
    ]

    result = deduplicate_canonical_organizations(records)

    assert len(result) == 1
    assert result[0]["canonical_organization_id"] == "can_1"
    assert result[0]["canonical_organization_key"] == "key1"


def test_deduplicate_canonical_organizations_preserves_different_keys():
    records = [
        sample_canonical_record("can_1", "key1", "Org A", "a.com", "src1"),
        sample_canonical_record("can_2", "key2", "Org B", "b.com", "src2"),
    ]

    result = deduplicate_canonical_organizations(records)

    assert len(result) == 2
    assert [item["canonical_organization_key"] for item in result] == ["key1", "key2"]


def test_deduplicate_canonical_organizations_stable_ordering_with_interleaved_duplicates():
    records = [
        sample_canonical_record("can_1", "key1", "Org A", "a.com", "src1"),
        sample_canonical_record("can_2", "key2", "Org B", "b.com", "src2"),
        sample_canonical_record("can_3", "key1", "Org A Again", "a.com", "src3"),
        sample_canonical_record("can_4", "key3", "Org C", "c.com", "src4"),
    ]

    result = deduplicate_canonical_organizations(records)

    assert [item["canonical_organization_id"] for item in result] == ["can_1", "can_2", "can_4"]


def test_suppress_duplicate_canonical_organizations_returns_empty_for_empty_input():
    result = suppress_duplicate_canonical_organizations([])

    assert result == []


def test_suppress_duplicate_canonical_organizations_deterministic_for_same_input():
    records = [
        sample_canonical_record("can_1", "key1", "Org A", "a.com", "src1"),
        sample_canonical_record("can_2", "key2", "Org B", "b.com", "src2"),
        sample_canonical_record("can_3", "key1", "Org A Again", "a.com", "src3"),
    ]

    first = suppress_duplicate_canonical_organizations(records)
    second = suppress_duplicate_canonical_organizations(records)

    assert first == second


def test_suppress_duplicate_canonical_organizations_preserves_schema_and_values():
    records = [
        sample_canonical_record("can_1", "key1", "Org A", "a.com", "src1"),
        sample_canonical_record("can_2", "key2", "Org B", "b.com", "src2"),
    ]

    output = suppress_duplicate_canonical_organizations(records)

    assert len(output) == 2
    for item in output:
        assert tuple(item.keys()) == (
            "canonical_organization_id",
            "organization_domain_candidate_id",
            "organization_candidate_id",
            "candidate_id",
            "seed_id",
            "source_id",
            "source_label",
            "canonical_organization_name",
            "canonical_organization_key",
            "domain_candidate",
            "candidate_state",
        )
        assert item["candidate_state"] == "canonicalized"


def test_suppress_duplicate_canonical_organizations_does_not_mutate_inputs():
    records = [
        sample_canonical_record("can_1", "key1"),
        sample_canonical_record("can_2", "key2"),
    ]
    original = copy.deepcopy(records)

    output = suppress_duplicate_canonical_organizations(records)

    assert records == original
    assert output is not records
    assert output[0] is not records[0]