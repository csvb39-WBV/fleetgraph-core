from pathlib import Path
import copy
import sys


ROOT = Path(__file__).resolve().parents[2]
SRC = ROOT / "src"

if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from fleetgraph_core.output.crm_push_gateway import (  # noqa: E402
    CRM_PAYLOAD_FIELDS,
    CRM_PAYLOAD_STATE,
    DELIVERY_ROW_FIELDS,
    FG6_MB2_FIELDS,
    apply_crm_push_gateway,
    validate_fg6_mb2_records,
)


def sample_fg6_mb2_record(
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
        "delivery_row_id": "deliveryrow:" + key + ":" + source + ":" + str(rank),
        "delivery_row": {
            "lead_record_id": "leadrecord:" + key + ":" + source + ":" + str(rank),
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
        "delivery_row_state": "flatfile_ready",
    }


# ---------------------------------------------------------------------------
# Validation tests — top-level structure
# ---------------------------------------------------------------------------

def test_validate_fg6_mb2_records_accepts_valid_input() -> None:
    validate_fg6_mb2_records(
        [
            sample_fg6_mb2_record("can_1", "key1", rank=1, contact_match_state="matched"),
            sample_fg6_mb2_record("can_2", "key2", rank=2, contact_match_state="unmatched"),
        ]
    )


def test_validate_fg6_mb2_records_rejects_non_list_input() -> None:
    try:
        validate_fg6_mb2_records("invalid")
    except TypeError as error:
        assert str(error) == "records must be a list"
    else:
        raise AssertionError("TypeError was not raised for non-list input")


def test_validate_fg6_mb2_records_rejects_non_dict_record() -> None:
    try:
        validate_fg6_mb2_records(["invalid"])
    except TypeError as error:
        assert str(error) == "each record must be a dictionary"
    else:
        raise AssertionError("TypeError was not raised for non-dict record")


def test_validate_fg6_mb2_records_rejects_missing_field() -> None:
    record = sample_fg6_mb2_record()
    del record["delivery_row_state"]

    try:
        validate_fg6_mb2_records([record])
    except ValueError as error:
        assert str(error) == "record is missing required fields: delivery_row_state"
    else:
        raise AssertionError("ValueError was not raised for missing field")


def test_validate_fg6_mb2_records_rejects_extra_field() -> None:
    record = sample_fg6_mb2_record()
    record["extra"] = "x"

    try:
        validate_fg6_mb2_records([record])
    except ValueError as error:
        assert str(error) == "record contains unknown fields: extra"
    else:
        raise AssertionError("ValueError was not raised for extra field")


def test_validate_fg6_mb2_records_rejects_wrong_delivery_row_state() -> None:
    record = sample_fg6_mb2_record()
    record["delivery_row_state"] = "pending"

    try:
        validate_fg6_mb2_records([record])
    except ValueError as error:
        assert str(error) == "delivery_row_state must be exactly 'flatfile_ready'"
    else:
        raise AssertionError("ValueError was not raised for invalid delivery_row_state")


def test_validate_fg6_mb2_records_rejects_non_dict_delivery_row() -> None:
    record = sample_fg6_mb2_record()
    record["delivery_row"] = "bad"

    try:
        validate_fg6_mb2_records([record])
    except TypeError as error:
        assert str(error) == "delivery_row must be a dictionary"
    else:
        raise AssertionError("TypeError was not raised for non-dict delivery_row")


def test_validate_fg6_mb2_records_rejects_non_int_opportunity_rank() -> None:
    record = sample_fg6_mb2_record()
    record["opportunity_rank"] = "1"

    try:
        validate_fg6_mb2_records([record])
    except TypeError as error:
        assert str(error) == "opportunity_rank must be an integer"
    else:
        raise AssertionError("TypeError was not raised for non-int opportunity_rank")


# ---------------------------------------------------------------------------
# Validation tests — delivery_row nested structure
# ---------------------------------------------------------------------------

def test_validate_fg6_mb2_records_rejects_missing_nested_field() -> None:
    record = sample_fg6_mb2_record()
    del record["delivery_row"]["contact_title"]

    try:
        validate_fg6_mb2_records([record])
    except ValueError as error:
        assert str(error) == "delivery_row is missing required fields: contact_title"
    else:
        raise AssertionError("ValueError was not raised for missing nested field")


def test_validate_fg6_mb2_records_rejects_nested_extra_field() -> None:
    record = sample_fg6_mb2_record()
    record["delivery_row"]["unexpected_field"] = "x"

    try:
        validate_fg6_mb2_records([record])
    except ValueError as error:
        assert str(error) == "delivery_row contains unknown fields: unexpected_field"
    else:
        raise AssertionError("ValueError was not raised for extra nested field")


def test_validate_fg6_mb2_records_rejects_nested_blank_string() -> None:
    record = sample_fg6_mb2_record()
    record["delivery_row"]["contact_name"] = "   "

    try:
        validate_fg6_mb2_records([record])
    except ValueError as error:
        assert str(error) == "delivery_row.contact_name must be a non-empty string"
    else:
        raise AssertionError("ValueError was not raised for blank nested string")


def test_validate_fg6_mb2_records_rejects_nested_wrong_type() -> None:
    record = sample_fg6_mb2_record()
    record["delivery_row"]["contact_email"] = 42

    try:
        validate_fg6_mb2_records([record])
    except TypeError as error:
        assert str(error) == "delivery_row.contact_email must be a non-empty string"
    else:
        raise AssertionError("TypeError was not raised for wrong-type nested field")


def test_validate_fg6_mb2_records_rejects_nested_opportunity_rank_below_one() -> None:
    record = sample_fg6_mb2_record()
    record["delivery_row"]["opportunity_rank"] = 0

    try:
        validate_fg6_mb2_records([record])
    except ValueError as error:
        assert str(error) == "delivery_row.opportunity_rank must be >= 1"
    else:
        raise AssertionError("ValueError was not raised for nested opportunity_rank < 1")


# ---------------------------------------------------------------------------
# Validation tests — top-level / delivery_row consistency
# ---------------------------------------------------------------------------

def test_validate_fg6_mb2_records_rejects_consistency_mismatch() -> None:
    record = sample_fg6_mb2_record()
    record["delivery_row"]["source_id"] = "wrong_source"

    try:
        validate_fg6_mb2_records([record])
    except ValueError as error:
        assert str(error) == "delivery_row.source_id must match record.source_id"
    else:
        raise AssertionError("ValueError was not raised for consistency mismatch")


def test_validate_fg6_mb2_records_rejects_lead_record_id_mismatch() -> None:
    record = sample_fg6_mb2_record()
    record["delivery_row"]["lead_record_id"] = "leadrecord:wrong:wrong:0"

    try:
        validate_fg6_mb2_records([record])
    except ValueError as error:
        assert str(error) == "delivery_row.lead_record_id must match record.lead_record_id"
    else:
        raise AssertionError("ValueError was not raised for lead_record_id mismatch")


# ---------------------------------------------------------------------------
# Core output contract tests
# ---------------------------------------------------------------------------

def test_apply_crm_push_gateway_result_has_exact_fields() -> None:
    record = sample_fg6_mb2_record()

    result = apply_crm_push_gateway([record])[0]

    assert set(result.keys()) == {"crm_payload_id", "crm_payload", "crm_payload_state", "delivery_row_id"}


def test_apply_crm_push_gateway_sets_locked_payload_state() -> None:
    record = sample_fg6_mb2_record()

    result = apply_crm_push_gateway([record])[0]

    assert result["crm_payload_state"] == CRM_PAYLOAD_STATE
    assert CRM_PAYLOAD_STATE == "gateway_ready"


def test_apply_crm_push_gateway_crm_payload_has_exact_keys() -> None:
    record = sample_fg6_mb2_record()

    result = apply_crm_push_gateway([record])[0]

    assert set(result["crm_payload"].keys()) == set(CRM_PAYLOAD_FIELDS)


def test_apply_crm_push_gateway_crm_payload_has_exact_key_order() -> None:
    record = sample_fg6_mb2_record()

    result = apply_crm_push_gateway([record])[0]

    assert list(result["crm_payload"].keys()) == list(CRM_PAYLOAD_FIELDS)


def test_apply_crm_push_gateway_crm_payload_id_is_deterministic() -> None:
    record = sample_fg6_mb2_record(key="keyX", source="srcY", rank=4)

    id_first = apply_crm_push_gateway([record])[0]["crm_payload_id"]
    id_second = apply_crm_push_gateway([record])[0]["crm_payload_id"]

    assert id_first == id_second
    assert id_first == "crmpayload:keyX:srcY:4"


def test_apply_crm_push_gateway_projects_expected_crm_values() -> None:
    record = sample_fg6_mb2_record(
        canonical_id="can_5",
        key="keyX",
        name="Acme Corp",
        domain="acme.com",
        source="src9",
        rank=3,
        contact_match_state="matched",
    )

    result = apply_crm_push_gateway([record])[0]
    crm_payload = result["crm_payload"]

    assert crm_payload["company_name"] == "Acme Corp"
    assert crm_payload["company_domain"] == "acme.com"
    assert crm_payload["company_source"] == "Label"
    assert crm_payload["contact_full_name"] == "Alice Example"
    assert crm_payload["contact_job_title"] == "Head of Procurement"
    assert crm_payload["contact_email_address"] == "alice@acme.com"
    assert crm_payload["contact_data_source"] == "src9"
    assert crm_payload["contact_match_status"] == "matched"
    assert crm_payload["fleet_opportunity_rank"] == 3
    assert crm_payload["internal_org_id"] == "can_5"
    assert crm_payload["internal_org_key"] == "keyX"
    assert crm_payload["internal_lead_id"] == "leadrecord:keyX:src9:3"
    assert crm_payload["internal_candidate_id"] == "cand_can_5"
    assert crm_payload["internal_source_id"] == "src9"


def test_apply_crm_push_gateway_carries_delivery_row_id() -> None:
    record = sample_fg6_mb2_record(key="keyZ", source="srcZ", rank=7)

    result = apply_crm_push_gateway([record])[0]

    assert result["delivery_row_id"] == record["delivery_row_id"]
    assert result["delivery_row_id"] == "deliveryrow:keyZ:srcZ:7"


def test_apply_crm_push_gateway_passes_through_matched_and_unmatched() -> None:
    matched = sample_fg6_mb2_record(
        key="key1", source="src1", rank=1, contact_match_state="matched"
    )
    unmatched = sample_fg6_mb2_record(
        canonical_id="can_2",
        key="key2",
        source="src2",
        rank=2,
        contact_match_state="unmatched",
    )

    result = apply_crm_push_gateway([matched, unmatched])

    assert result[0]["crm_payload"]["contact_match_status"] == "matched"
    assert result[1]["crm_payload"]["contact_match_status"] == "unmatched"


# ---------------------------------------------------------------------------
# Stable ordering, non-mutation, and determinism tests
# ---------------------------------------------------------------------------

def test_apply_crm_push_gateway_preserves_input_order() -> None:
    records = [
        sample_fg6_mb2_record("can_3", "key3", rank=3),
        sample_fg6_mb2_record("can_1", "key1", rank=1),
        sample_fg6_mb2_record("can_2", "key2", rank=2),
    ]

    result = apply_crm_push_gateway(records)

    assert [item["crm_payload"]["internal_org_id"] for item in result] == [
        "can_3",
        "can_1",
        "can_2",
    ]


def test_apply_crm_push_gateway_does_not_include_upstream_pipeline_fields() -> None:
    record = sample_fg6_mb2_record()

    result = apply_crm_push_gateway([record])[0]

    pipeline_fields = set(FG6_MB2_FIELDS)
    result_keys = set(result.keys())

    assert result_keys.isdisjoint(pipeline_fields - {"delivery_row_id"})


def test_apply_crm_push_gateway_is_non_mutating() -> None:
    records = [
        sample_fg6_mb2_record("can_1", "key1", rank=1),
        sample_fg6_mb2_record("can_2", "key2", rank=2),
    ]
    original = copy.deepcopy(records)

    output = apply_crm_push_gateway(records)

    assert records == original
    assert output is not records
    assert output[0] is not records[0]


def test_apply_crm_push_gateway_is_deterministic() -> None:
    records = [
        sample_fg6_mb2_record("can_1", "key1", rank=1),
        sample_fg6_mb2_record("can_2", "key2", rank=2),
    ]

    first = apply_crm_push_gateway(records)
    second = apply_crm_push_gateway(records)

    assert first == second


def test_apply_crm_push_gateway_accepts_empty_list() -> None:
    assert apply_crm_push_gateway([]) == []
