from pathlib import Path
import copy
import sys


ROOT = Path(__file__).resolve().parents[2]
SRC = ROOT / "src"

if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from fleetgraph_core.output.flatfile_delivery_writer import (  # noqa: E402
    DELIVERY_ROW_STATE,
    FG6_MB1_FIELDS,
    FLATFILE_DELIVERY_ROW_FIELDS,
    LEAD_RECORD_FIELDS,
    apply_flatfile_delivery_writer,
    validate_fg6_mb1_records,
)


def sample_fg6_mb1_record(
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
        "lead_record_id": "leadrecord:" + key + ":" + source + ":" + str(rank),
        "lead_record": {
            "canonical_organization_id": canonical_id,
            "canonical_organization_key": key,
            "canonical_organization_name": name,
            "organization_domain_candidate_id": "dom_" + canonical_id,
            "organization_candidate_id": "org_" + canonical_id,
            "candidate_id": "cand_" + canonical_id,
            "seed_id": "seed1",
            "source_id": source,
            "source_label": "Label",
            "domain_candidate": domain,
            "opportunity_rank": rank,
            "contact_name": "Alice Example",
            "contact_title": "Head of Procurement",
            "contact_email": "alice@" + domain,
            "contact_source": source,
            "contact_match_state": contact_match_state,
        },
        "lead_record_state": "assembled",
    }


# ---------------------------------------------------------------------------
# Validation tests — top-level structure
# ---------------------------------------------------------------------------

def test_validate_fg6_mb1_records_accepts_valid_input() -> None:
    validate_fg6_mb1_records(
        [
            sample_fg6_mb1_record("can_1", "key1", rank=1, contact_match_state="matched"),
            sample_fg6_mb1_record("can_2", "key2", rank=2, contact_match_state="unmatched"),
        ]
    )


def test_validate_fg6_mb1_records_rejects_non_list_input() -> None:
    try:
        validate_fg6_mb1_records("invalid")
    except TypeError as error:
        assert str(error) == "records must be a list"
    else:
        raise AssertionError("TypeError was not raised for non-list input")


def test_validate_fg6_mb1_records_rejects_non_dict_record() -> None:
    try:
        validate_fg6_mb1_records(["invalid"])
    except TypeError as error:
        assert str(error) == "each record must be a dictionary"
    else:
        raise AssertionError("TypeError was not raised for non-dict record")


def test_validate_fg6_mb1_records_rejects_missing_field() -> None:
    record = sample_fg6_mb1_record()
    del record["lead_record_state"]

    try:
        validate_fg6_mb1_records([record])
    except ValueError as error:
        assert str(error) == "record is missing required fields: lead_record_state"
    else:
        raise AssertionError("ValueError was not raised for missing field")


def test_validate_fg6_mb1_records_rejects_extra_field() -> None:
    record = sample_fg6_mb1_record()
    record["extra_field"] = "x"

    try:
        validate_fg6_mb1_records([record])
    except ValueError as error:
        assert str(error) == "record contains unknown fields: extra_field"
    else:
        raise AssertionError("ValueError was not raised for extra field")


def test_validate_fg6_mb1_records_rejects_wrong_lead_record_state() -> None:
    record = sample_fg6_mb1_record()
    record["lead_record_state"] = "pending"

    try:
        validate_fg6_mb1_records([record])
    except ValueError as error:
        assert str(error) == "lead_record_state must be exactly 'assembled'"
    else:
        raise AssertionError("ValueError was not raised for invalid lead_record_state")


def test_validate_fg6_mb1_records_rejects_non_int_opportunity_rank() -> None:
    record = sample_fg6_mb1_record()
    record["opportunity_rank"] = "1"

    try:
        validate_fg6_mb1_records([record])
    except TypeError as error:
        assert str(error) == "opportunity_rank must be an integer"
    else:
        raise AssertionError("TypeError was not raised for non-int opportunity_rank")


# ---------------------------------------------------------------------------
# Validation tests — lead_record nested structure
# ---------------------------------------------------------------------------

def test_validate_fg6_mb1_records_rejects_non_dict_lead_record() -> None:
    record = sample_fg6_mb1_record()
    record["lead_record"] = "bad"

    try:
        validate_fg6_mb1_records([record])
    except TypeError as error:
        assert str(error) == "lead_record must be a dictionary"
    else:
        raise AssertionError("TypeError was not raised for non-dict lead_record")


def test_validate_fg6_mb1_records_rejects_missing_nested_field() -> None:
    record = sample_fg6_mb1_record()
    del record["lead_record"]["contact_title"]

    try:
        validate_fg6_mb1_records([record])
    except ValueError as error:
        assert str(error) == "lead_record is missing required fields: contact_title"
    else:
        raise AssertionError("ValueError was not raised for missing nested field")


def test_validate_fg6_mb1_records_rejects_nested_blank_string() -> None:
    record = sample_fg6_mb1_record()
    record["lead_record"]["contact_name"] = "   "

    try:
        validate_fg6_mb1_records([record])
    except ValueError as error:
        assert str(error) == "lead_record.contact_name must be a non-empty string"
    else:
        raise AssertionError("ValueError was not raised for blank nested string")


def test_validate_fg6_mb1_records_rejects_nested_wrong_type() -> None:
    record = sample_fg6_mb1_record()
    record["lead_record"]["contact_email"] = 42

    try:
        validate_fg6_mb1_records([record])
    except TypeError as error:
        assert str(error) == "lead_record.contact_email must be a non-empty string"
    else:
        raise AssertionError("TypeError was not raised for wrong-type nested field")


def test_validate_fg6_mb1_records_rejects_nested_non_int_opportunity_rank() -> None:
    record = sample_fg6_mb1_record()
    record["lead_record"]["opportunity_rank"] = "1"

    try:
        validate_fg6_mb1_records([record])
    except TypeError as error:
        assert str(error) == "lead_record.opportunity_rank must be an integer"
    else:
        raise AssertionError("TypeError was not raised for non-int nested opportunity_rank")


# ---------------------------------------------------------------------------
# Validation tests — top-level / lead_record consistency
# ---------------------------------------------------------------------------

def test_validate_fg6_mb1_records_rejects_consistency_mismatch() -> None:
    record = sample_fg6_mb1_record()
    record["lead_record"]["source_id"] = "wrong_source"

    try:
        validate_fg6_mb1_records([record])
    except ValueError as error:
        assert str(error) == "lead_record.source_id must match record.source_id"
    else:
        raise AssertionError("ValueError was not raised for consistency mismatch")


def test_validate_fg6_mb1_records_rejects_opportunity_rank_mismatch() -> None:
    record = sample_fg6_mb1_record()
    record["lead_record"]["opportunity_rank"] = 99

    try:
        validate_fg6_mb1_records([record])
    except ValueError as error:
        assert str(error) == "lead_record.opportunity_rank must match record.opportunity_rank"
    else:
        raise AssertionError("ValueError was not raised for opportunity_rank mismatch")


# ---------------------------------------------------------------------------
# Core output contract tests
# ---------------------------------------------------------------------------

def test_apply_flatfile_delivery_writer_adds_exactly_three_fields() -> None:
    record = sample_fg6_mb1_record()

    result = apply_flatfile_delivery_writer([record])[0]

    assert set(result.keys()) == set(FG6_MB1_FIELDS) | {
        "delivery_row_id",
        "delivery_row",
        "delivery_row_state",
    }


def test_apply_flatfile_delivery_writer_sets_locked_delivery_row_state() -> None:
    record = sample_fg6_mb1_record()

    result = apply_flatfile_delivery_writer([record])[0]

    assert result["delivery_row_state"] == DELIVERY_ROW_STATE
    assert DELIVERY_ROW_STATE == "flatfile_ready"


def test_apply_flatfile_delivery_writer_delivery_row_has_exact_keys() -> None:
    record = sample_fg6_mb1_record()

    result = apply_flatfile_delivery_writer([record])[0]

    assert set(result["delivery_row"].keys()) == set(FLATFILE_DELIVERY_ROW_FIELDS)


def test_apply_flatfile_delivery_writer_delivery_row_has_exact_key_order() -> None:
    record = sample_fg6_mb1_record()

    result = apply_flatfile_delivery_writer([record])[0]

    assert list(result["delivery_row"].keys()) == list(FLATFILE_DELIVERY_ROW_FIELDS)


def test_apply_flatfile_delivery_writer_delivery_row_id_is_deterministic() -> None:
    record = sample_fg6_mb1_record(key="keyX", source="srcY", rank=4)

    id_first = apply_flatfile_delivery_writer([record])[0]["delivery_row_id"]
    id_second = apply_flatfile_delivery_writer([record])[0]["delivery_row_id"]

    assert id_first == id_second
    assert id_first == "deliveryrow:keyX:srcY:4"


def test_apply_flatfile_delivery_writer_projects_expected_values() -> None:
    record = sample_fg6_mb1_record(
        canonical_id="can_5",
        key="keyX",
        name="Acme Corp",
        domain="acme.com",
        source="src9",
        rank=3,
        contact_match_state="matched",
    )

    result = apply_flatfile_delivery_writer([record])[0]
    delivery_row = result["delivery_row"]

    assert delivery_row["lead_record_id"] == "leadrecord:keyX:src9:3"
    assert delivery_row["canonical_organization_id"] == "can_5"
    assert delivery_row["canonical_organization_key"] == "keyX"
    assert delivery_row["canonical_organization_name"] == "Acme Corp"
    assert delivery_row["organization_domain_candidate_id"] == "dom_can_5"
    assert delivery_row["organization_candidate_id"] == "org_can_5"
    assert delivery_row["candidate_id"] == "cand_can_5"
    assert delivery_row["seed_id"] == "seed1"
    assert delivery_row["source_id"] == "src9"
    assert delivery_row["source_label"] == "Label"
    assert delivery_row["domain_candidate"] == "acme.com"
    assert delivery_row["opportunity_rank"] == 3
    assert delivery_row["contact_name"] == "Alice Example"
    assert delivery_row["contact_title"] == "Head of Procurement"
    assert delivery_row["contact_email"] == "alice@acme.com"
    assert delivery_row["contact_source"] == "src9"
    assert delivery_row["contact_match_state"] == "matched"


def test_apply_flatfile_delivery_writer_passes_through_matched_and_unmatched() -> None:
    matched = sample_fg6_mb1_record(
        key="key1", source="src1", rank=1, contact_match_state="matched"
    )
    unmatched = sample_fg6_mb1_record(
        canonical_id="can_2",
        key="key2",
        source="src2",
        rank=2,
        contact_match_state="unmatched",
    )

    result = apply_flatfile_delivery_writer([matched, unmatched])

    assert result[0]["delivery_row"]["contact_match_state"] == "matched"
    assert result[1]["delivery_row"]["contact_match_state"] == "unmatched"


# ---------------------------------------------------------------------------
# Stable ordering, non-mutation, and determinism tests
# ---------------------------------------------------------------------------

def test_apply_flatfile_delivery_writer_preserves_input_order() -> None:
    records = [
        sample_fg6_mb1_record("can_3", "key3", rank=3),
        sample_fg6_mb1_record("can_1", "key1", rank=1),
        sample_fg6_mb1_record("can_2", "key2", rank=2),
    ]

    result = apply_flatfile_delivery_writer(records)

    assert [item["canonical_organization_id"] for item in result] == ["can_3", "can_1", "can_2"]


def test_apply_flatfile_delivery_writer_preserves_upstream_fields() -> None:
    record = sample_fg6_mb1_record("can_1", "key1", "Org A", "a.com", "src1", rank=1)

    result = apply_flatfile_delivery_writer([record])[0]

    for field_name in FG6_MB1_FIELDS:
        assert result[field_name] == record[field_name]


def test_apply_flatfile_delivery_writer_is_non_mutating() -> None:
    records = [
        sample_fg6_mb1_record("can_1", "key1", rank=1),
        sample_fg6_mb1_record("can_2", "key2", rank=2),
    ]
    original = copy.deepcopy(records)

    output = apply_flatfile_delivery_writer(records)

    assert records == original
    assert output is not records
    assert output[0] is not records[0]


def test_apply_flatfile_delivery_writer_is_deterministic() -> None:
    records = [
        sample_fg6_mb1_record("can_1", "key1", rank=1),
        sample_fg6_mb1_record("can_2", "key2", rank=2),
    ]

    first = apply_flatfile_delivery_writer(records)
    second = apply_flatfile_delivery_writer(records)

    assert first == second


def test_apply_flatfile_delivery_writer_accepts_empty_list() -> None:
    assert apply_flatfile_delivery_writer([]) == []
