"""Tests for deterministic canonical organization deduplication."""

import copy
import pathlib
import sys


PROJECT_ROOT = pathlib.Path(__file__).resolve().parents[2]
SRC_ROOT = PROJECT_ROOT / "src"

if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from fleetgraph_core.identity_resolution.canonical_organization_deduper import (
    assemble_unified_canonical_organizations,
    build_deduplication_key,
    build_unified_identity,
    merge_canonical_group,
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


def test_validate_canonical_organizations_rejects_duplicate_canonical_organization_id():
    record1 = sample_canonical_record("can_1")
    record2 = sample_canonical_record("can_1")

    try:
        validate_canonical_organizations([record1, record2])
    except ValueError as error:
        assert str(error) == "duplicate canonical_organization_id detected: can_1"
    else:
        raise AssertionError("ValueError was not raised for duplicate canonical_organization_id")


def test_build_deduplication_key_constructs_key():
    record = sample_canonical_record(key="key1")
    key = build_deduplication_key(record)
    assert key == ("key1",)


def test_merge_canonical_group_merges_duplicates():
    group = [
        sample_canonical_record("can_1", "key1", "Org A", "a.com", "src1"),
        sample_canonical_record("can_2", "key1", "Org A", "a.com", "src2"),
    ]

    merged = merge_canonical_group(group)

    assert merged["unified_organization_id"] == "key1::unified"
    assert merged["canonical_organization_ids"] == ["can_1", "can_2"]
    assert merged["source_ids"] == ["src1", "src2"]
    assert merged["canonical_organization_name"] == "Org A"
    assert merged["domain_candidate"] == "a.com"
    assert merged["candidate_state"] == "unified"


def test_merge_canonical_group_selects_first_record_for_name_and_domain():
    group = [
        sample_canonical_record("can_2", "key1", "Org B", "b.com", "src1"),
        sample_canonical_record("can_1", "key1", "Org A", "a.com", "src2"),
    ]

    merged = merge_canonical_group(group)

    # Sorted by canonical_organization_id, so can_1 first
    assert merged["canonical_organization_name"] == "Org A"
    assert merged["domain_candidate"] == "a.com"


def test_build_unified_identity_is_deterministic():
    record = {
        "unified_organization_id": "key1::unified",
        "canonical_organization_ids": ["can_1"],
        "canonical_organization_name": "Org A",
        "canonical_organization_key": "key1",
        "domain_candidate": "a.com",
        "source_ids": ["src1"],
        "candidate_state": "unified",
    }

    identity = build_unified_identity(record)
    assert identity == ("a.com", "key1", "key1::unified")


def test_assemble_unified_canonical_organizations_sorts_output():
    records = [
        sample_canonical_record("can_2", "key2", "Org B", "b.com", "src2"),
        sample_canonical_record("can_1", "key1", "Org A", "a.com", "src1"),
    ]

    result = assemble_unified_canonical_organizations(records)

    assert len(result) == 2
    assert result[0]["domain_candidate"] == "a.com"
    assert result[1]["domain_candidate"] == "b.com"


def test_assemble_unified_canonical_organizations_does_not_mutate_inputs():
    records = [
        sample_canonical_record("can_1", "key1"),
        sample_canonical_record("can_2", "key1"),
    ]
    original = copy.deepcopy(records)

    _ = assemble_unified_canonical_organizations(records)
    assert records == original