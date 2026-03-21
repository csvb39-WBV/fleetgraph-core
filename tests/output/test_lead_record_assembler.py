from pathlib import Path
import copy
import sys


ROOT = Path(__file__).resolve().parents[2]
SRC = ROOT / "src"

if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from fleetgraph_core.output.lead_record_assembler import (  # noqa: E402
    FG5_MB2_FIELDS,
    LEAD_RECORD_FIELDS,
    LEAD_RECORD_STATE,
    apply_lead_record_assembler,
    validate_fg5_mb2_records,
)


def sample_fg5_mb2_record(
    canonical_id: str = "can_1",
    key: str = "key1",
    name: str = "Org A",
    domain: str = "a.com",
    source: str = "src1",
    rank: int = 1,
    contact_match_state: str = "matched",
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
        "matched_contact": {
            "contact_name": "Alice Example",
            "contact_title": "Head of Procurement",
            "contact_email": "alice@" + domain,
            "contact_source": source,
            "contact_match_state": contact_match_state,
        },
        "contact_assembly_state": "assembled",
    }


def test_validate_fg5_mb2_records_accepts_valid_input() -> None:
    validate_fg5_mb2_records(
        [
            sample_fg5_mb2_record("can_1", "key1", rank=1, contact_match_state="matched"),
            sample_fg5_mb2_record("can_2", "key2", rank=2, contact_match_state="unmatched"),
        ]
    )


def test_validate_fg5_mb2_records_rejects_non_list_input() -> None:
    try:
        validate_fg5_mb2_records("invalid")
    except TypeError as error:
        assert str(error) == "records must be a list"
    else:
        raise AssertionError("TypeError was not raised for non-list input")


def test_validate_fg5_mb2_records_rejects_non_dict_record() -> None:
    try:
        validate_fg5_mb2_records(["invalid"])
    except TypeError as error:
        assert str(error) == "each record must be a dictionary"
    else:
        raise AssertionError("TypeError was not raised for non-dict record")


def test_validate_fg5_mb2_records_rejects_missing_field() -> None:
    record = sample_fg5_mb2_record()
    del record["contact_assembly_state"]

    try:
        validate_fg5_mb2_records([record])
    except ValueError as error:
        assert str(error) == "record is missing required fields: contact_assembly_state"
    else:
        raise AssertionError("ValueError was not raised for missing field")


def test_validate_fg5_mb2_records_rejects_extra_field() -> None:
    record = sample_fg5_mb2_record()
    record["extra_field"] = "x"

    try:
        validate_fg5_mb2_records([record])
    except ValueError as error:
        assert str(error) == "record contains unknown fields: extra_field"
    else:
        raise AssertionError("ValueError was not raised for extra field")


def test_validate_fg5_mb2_records_rejects_wrong_contact_assembly_state() -> None:
    record = sample_fg5_mb2_record()
    record["contact_assembly_state"] = "prepared"

    try:
        validate_fg5_mb2_records([record])
    except ValueError as error:
        assert str(error) == "contact_assembly_state must be exactly 'assembled'"
    else:
        raise AssertionError("ValueError was not raised for invalid contact_assembly_state")


def test_validate_fg5_mb2_records_rejects_missing_nested_matched_contact_field() -> None:
    record = sample_fg5_mb2_record()
    del record["matched_contact"]["contact_title"]

    try:
        validate_fg5_mb2_records([record])
    except ValueError as error:
        assert str(error) == "matched_contact is missing required fields: contact_title"
    else:
        raise AssertionError("ValueError was not raised for missing nested field")


def test_validate_fg5_mb2_records_rejects_invalid_contact_match_state() -> None:
    record = sample_fg5_mb2_record(contact_match_state="unknown")

    try:
        validate_fg5_mb2_records([record])
    except ValueError as error:
        assert str(error) == "contact_match_state must be exactly 'matched' or 'unmatched'"
    else:
        raise AssertionError("ValueError was not raised for invalid contact_match_state")


def test_apply_lead_record_assembler_adds_exactly_three_fields() -> None:
    record = sample_fg5_mb2_record()

    result = apply_lead_record_assembler([record])[0]

    assert set(result.keys()) == set(FG5_MB2_FIELDS) | {
        "lead_record_id",
        "lead_record",
        "lead_record_state",
    }


def test_apply_lead_record_assembler_sets_locked_lead_record_state() -> None:
    record = sample_fg5_mb2_record()

    result = apply_lead_record_assembler([record])[0]

    assert result["lead_record_state"] == LEAD_RECORD_STATE
    assert LEAD_RECORD_STATE == "assembled"


def test_apply_lead_record_assembler_lead_record_has_exact_keys() -> None:
    record = sample_fg5_mb2_record()

    result = apply_lead_record_assembler([record])[0]

    assert set(result["lead_record"].keys()) == set(LEAD_RECORD_FIELDS)


def test_apply_lead_record_assembler_projects_expected_values_for_matched() -> None:
    record = sample_fg5_mb2_record(
        canonical_id="can_5",
        key="keyX",
        name="Acme Corp",
        domain="acme.com",
        source="src9",
        rank=3,
        contact_match_state="matched",
    )

    result = apply_lead_record_assembler([record])[0]
    lead_record = result["lead_record"]

    assert lead_record["canonical_organization_id"] == "can_5"
    assert lead_record["canonical_organization_key"] == "keyX"
    assert lead_record["canonical_organization_name"] == "Acme Corp"
    assert lead_record["organization_domain_candidate_id"] == "dom_can_5"
    assert lead_record["organization_candidate_id"] == "org_can_5"
    assert lead_record["candidate_id"] == "cand_can_5"
    assert lead_record["seed_id"] == "seed1"
    assert lead_record["source_id"] == "src9"
    assert lead_record["source_label"] == "Label"
    assert lead_record["domain_candidate"] == "acme.com"
    assert lead_record["opportunity_rank"] == 3
    assert lead_record["contact_name"] == "Alice Example"
    assert lead_record["contact_title"] == "Head of Procurement"
    assert lead_record["contact_email"] == "alice@acme.com"
    assert lead_record["contact_source"] == "src9"
    assert lead_record["contact_match_state"] == "matched"


def test_apply_lead_record_assembler_preserves_unmatched_state_without_rejection() -> None:
    record = sample_fg5_mb2_record(contact_match_state="unmatched")

    result = apply_lead_record_assembler([record])[0]

    assert result["lead_record"]["contact_match_state"] == "unmatched"
    assert result["matched_contact"]["contact_match_state"] == "unmatched"


def test_apply_lead_record_assembler_lead_record_id_is_deterministic() -> None:
    record = sample_fg5_mb2_record(key="keyA", source="srcB", rank=2)

    id_first = apply_lead_record_assembler([record])[0]["lead_record_id"]
    id_second = apply_lead_record_assembler([record])[0]["lead_record_id"]

    assert id_first == id_second
    assert id_first == "leadrecord:keyA:srcB:2"


def test_apply_lead_record_assembler_preserves_input_order() -> None:
    records = [
        sample_fg5_mb2_record("can_3", "key3", rank=3),
        sample_fg5_mb2_record("can_1", "key1", rank=1),
        sample_fg5_mb2_record("can_2", "key2", rank=2),
    ]

    result = apply_lead_record_assembler(records)

    assert [item["canonical_organization_id"] for item in result] == ["can_3", "can_1", "can_2"]


def test_apply_lead_record_assembler_preserves_upstream_fields() -> None:
    record = sample_fg5_mb2_record("can_1", "key1", "Org A", "a.com", "src1", rank=1)

    result = apply_lead_record_assembler([record])[0]

    for field_name in FG5_MB2_FIELDS:
        assert result[field_name] == record[field_name]


def test_apply_lead_record_assembler_is_non_mutating() -> None:
    records = [
        sample_fg5_mb2_record("can_1", "key1", rank=1),
        sample_fg5_mb2_record("can_2", "key2", rank=2),
    ]
    original = copy.deepcopy(records)

    output = apply_lead_record_assembler(records)

    assert records == original
    assert output is not records
    assert output[0] is not records[0]


def test_apply_lead_record_assembler_is_deterministic() -> None:
    records = [
        sample_fg5_mb2_record("can_1", "key1", rank=1),
        sample_fg5_mb2_record("can_2", "key2", rank=2),
    ]

    first = apply_lead_record_assembler(records)
    second = apply_lead_record_assembler(records)

    assert first == second


def test_apply_lead_record_assembler_accepts_empty_list() -> None:
    assert apply_lead_record_assembler([]) == []
