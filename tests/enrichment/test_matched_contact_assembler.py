from pathlib import Path
import copy
import sys


ROOT = Path(__file__).resolve().parents[2]
SRC = ROOT / "src"

if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from fleetgraph_core.enrichment.matched_contact_assembler import (  # noqa: E402
    FG5_MB1_FIELDS,
    MATCHED_CONTACT_STATE,
    apply_matched_contact_assembler,
    validate_fg5_mb1_records,
)


def sample_fg5_mb1_record(
    canonical_id: str = "can_1",
    key: str = "key1",
    name: str = "Org A",
    domain: str = "a.com",
    source: str = "src1",
    rank: int = 1,
) -> dict:
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
        "relevance_gate_outcome": "relevant",
        "opportunity_rank": rank,
        "contact_enrichment_request_id": "enrichrequest:" + key + ":" + source + ":" + str(rank),
        "contact_enrichment_request": {
            "request_type": "contact_enrichment",
            "canonical_organization_id": canonical_id,
            "canonical_organization_key": key,
            "canonical_organization_name": name,
            "domain_candidate": domain,
            "source_id": source,
            "opportunity_rank": rank,
        },
        "contact_coordination_state": "prepared",
    }


def test_validate_fg5_mb1_records_accepts_valid_input() -> None:
    validate_fg5_mb1_records(
        [
            sample_fg5_mb1_record("can_1", "key1", rank=1),
            sample_fg5_mb1_record("can_2", "key2", rank=2),
        ]
    )


def test_validate_fg5_mb1_records_rejects_non_list_input() -> None:
    try:
        validate_fg5_mb1_records("invalid")
    except TypeError as error:
        assert str(error) == "records must be a list"
    else:
        raise AssertionError("TypeError was not raised for non-list input")


def test_validate_fg5_mb1_records_rejects_non_dict_record() -> None:
    try:
        validate_fg5_mb1_records(["invalid"])
    except TypeError as error:
        assert str(error) == "each record must be a dictionary"
    else:
        raise AssertionError("TypeError was not raised for non-dict record")


def test_validate_fg5_mb1_records_rejects_missing_field() -> None:
    record = sample_fg5_mb1_record()
    del record["contact_coordination_state"]

    try:
        validate_fg5_mb1_records([record])
    except ValueError as error:
        assert str(error) == "record is missing required fields: contact_coordination_state"
    else:
        raise AssertionError("ValueError was not raised for missing field")


def test_validate_fg5_mb1_records_rejects_extra_field() -> None:
    record = sample_fg5_mb1_record()
    record["extra_field"] = "x"

    try:
        validate_fg5_mb1_records([record])
    except ValueError as error:
        assert str(error) == "record contains unknown fields: extra_field"
    else:
        raise AssertionError("ValueError was not raised for extra field")


def test_validate_fg5_mb1_records_rejects_wrong_contact_coordination_state() -> None:
    record = sample_fg5_mb1_record()
    record["contact_coordination_state"] = "queued"

    try:
        validate_fg5_mb1_records([record])
    except ValueError as error:
        assert str(error) == "contact_coordination_state must be exactly 'prepared'"
    else:
        raise AssertionError("ValueError was not raised for invalid contact_coordination_state")


def test_validate_fg5_mb1_records_rejects_non_dict_request() -> None:
    record = sample_fg5_mb1_record()
    record["contact_enrichment_request"] = "invalid"

    try:
        validate_fg5_mb1_records([record])
    except TypeError as error:
        assert str(error) == "contact_enrichment_request must be a dictionary"
    else:
        raise AssertionError("TypeError was not raised for non-dict contact_enrichment_request")


def test_validate_fg5_mb1_records_rejects_request_value_mismatch() -> None:
    record = sample_fg5_mb1_record()
    record["contact_enrichment_request"]["source_id"] = "wrong"

    try:
        validate_fg5_mb1_records([record])
    except ValueError as error:
        assert str(error) == "request source_id must match record"
    else:
        raise AssertionError("ValueError was not raised for request/record mismatch")


def test_apply_matched_contact_assembler_adds_exactly_three_fields() -> None:
    record = sample_fg5_mb1_record()

    result = apply_matched_contact_assembler([record])[0]

    assert set(result.keys()) == set(FG5_MB1_FIELDS) | {
        "matched_contact_id",
        "matched_contact",
        "matched_contact_state",
    }


def test_apply_matched_contact_assembler_sets_matched_state() -> None:
    record = sample_fg5_mb1_record()

    result = apply_matched_contact_assembler([record])[0]

    assert result["matched_contact_state"] == MATCHED_CONTACT_STATE
    assert MATCHED_CONTACT_STATE == "matched"


def test_apply_matched_contact_assembler_builds_expected_contact_projection() -> None:
    record = sample_fg5_mb1_record(
        canonical_id="can_5",
        key="keyX",
        name="Acme Corp",
        domain="acme.com",
        source="src9",
        rank=3,
    )

    result = apply_matched_contact_assembler([record])[0]
    matched_contact = result["matched_contact"]

    assert matched_contact["contact_id"] == "contact:keyX:src9:3"
    assert matched_contact["full_name"] == "Acme Corp Contact"
    assert matched_contact["email"] == "contact@acme.com"
    assert matched_contact["role"] == "decision_maker"
    assert matched_contact["source_id"] == "src9"
    assert matched_contact["canonical_organization_id"] == "can_5"
    assert matched_contact["opportunity_rank"] == 3


def test_apply_matched_contact_assembler_request_id_is_deterministic() -> None:
    record = sample_fg5_mb1_record(key="keyA", source="srcB", rank=2)

    id_first = apply_matched_contact_assembler([record])[0]["matched_contact_id"]
    id_second = apply_matched_contact_assembler([record])[0]["matched_contact_id"]

    assert id_first == id_second
    assert id_first == "matchedcontact:keyA:srcB:2"


def test_apply_matched_contact_assembler_preserves_input_order() -> None:
    records = [
        sample_fg5_mb1_record("can_3", "key3", rank=3),
        sample_fg5_mb1_record("can_1", "key1", rank=1),
        sample_fg5_mb1_record("can_2", "key2", rank=2),
    ]

    result = apply_matched_contact_assembler(records)

    assert [item["canonical_organization_id"] for item in result] == ["can_3", "can_1", "can_2"]


def test_apply_matched_contact_assembler_preserves_upstream_fields() -> None:
    record = sample_fg5_mb1_record("can_1", "key1", "Org A", "a.com", "src1", rank=1)

    result = apply_matched_contact_assembler([record])[0]

    for field_name in FG5_MB1_FIELDS:
        assert result[field_name] == record[field_name]


def test_apply_matched_contact_assembler_is_non_mutating() -> None:
    records = [
        sample_fg5_mb1_record("can_1", "key1", rank=1),
        sample_fg5_mb1_record("can_2", "key2", rank=2),
    ]
    original = copy.deepcopy(records)

    output = apply_matched_contact_assembler(records)

    assert records == original
    assert output is not records
    assert output[0] is not records[0]


def test_apply_matched_contact_assembler_is_deterministic() -> None:
    records = [
        sample_fg5_mb1_record("can_1", "key1", rank=1),
        sample_fg5_mb1_record("can_2", "key2", rank=2),
    ]

    first = apply_matched_contact_assembler(records)
    second = apply_matched_contact_assembler(records)

    assert first == second


def test_apply_matched_contact_assembler_accepts_empty_list() -> None:
    assert apply_matched_contact_assembler([]) == []