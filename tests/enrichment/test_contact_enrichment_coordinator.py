from pathlib import Path
import copy
import sys


ROOT = Path(__file__).resolve().parents[2]
SRC = ROOT / "src"

if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from fleetgraph_core.enrichment.contact_enrichment_coordinator import (  # noqa: E402
    CONTACT_COORDINATION_STATE,
    ENRICHMENT_REQUEST_TYPE,
    FG4_MB2_FIELDS,
    apply_contact_enrichment_coordinator,
    validate_fg4_mb2_records,
)


def sample_fg4_mb2_record(
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
    }


# ---------------------------------------------------------------------------
# Validation tests
# ---------------------------------------------------------------------------

def test_validate_fg4_mb2_records_accepts_valid_input() -> None:
    validate_fg4_mb2_records(
        [
            sample_fg4_mb2_record("can_1", "key1", rank=1),
            sample_fg4_mb2_record("can_2", "key2", rank=2),
        ]
    )


def test_validate_fg4_mb2_records_rejects_non_list_input() -> None:
    try:
        validate_fg4_mb2_records("invalid")
    except TypeError as error:
        assert str(error) == "records must be a list"
    else:
        raise AssertionError("TypeError was not raised for non-list input")


def test_validate_fg4_mb2_records_rejects_non_dict_record() -> None:
    try:
        validate_fg4_mb2_records(["invalid"])
    except TypeError as error:
        assert str(error) == "each record must be a dictionary"
    else:
        raise AssertionError("TypeError was not raised for non-dict record")


def test_validate_fg4_mb2_records_rejects_missing_field() -> None:
    record = sample_fg4_mb2_record()
    del record["canonical_organization_name"]

    try:
        validate_fg4_mb2_records([record])
    except ValueError as error:
        assert str(error) == "record is missing required fields: canonical_organization_name"
    else:
        raise AssertionError("ValueError was not raised for missing field")


def test_validate_fg4_mb2_records_rejects_extra_field() -> None:
    record = sample_fg4_mb2_record()
    record["extra_field"] = "x"

    try:
        validate_fg4_mb2_records([record])
    except ValueError as error:
        assert str(error) == "record contains unknown fields: extra_field"
    else:
        raise AssertionError("ValueError was not raised for extra field")


def test_validate_fg4_mb2_records_rejects_wrong_candidate_state() -> None:
    record = sample_fg4_mb2_record()
    record["candidate_state"] = "parsed"

    try:
        validate_fg4_mb2_records([record])
    except ValueError as error:
        assert str(error) == "candidate_state must be exactly 'canonicalized'"
    else:
        raise AssertionError("ValueError was not raised for invalid candidate_state")


def test_validate_fg4_mb2_records_rejects_wrong_relevance_gate_outcome() -> None:
    record = sample_fg4_mb2_record()
    record["relevance_gate_outcome"] = "not_relevant"

    try:
        validate_fg4_mb2_records([record])
    except ValueError as error:
        assert str(error) == "relevance_gate_outcome must be exactly 'relevant'"
    else:
        raise AssertionError("ValueError was not raised for invalid relevance_gate_outcome")


def test_validate_fg4_mb2_records_rejects_non_int_opportunity_rank() -> None:
    record = sample_fg4_mb2_record()
    record["opportunity_rank"] = "1"

    try:
        validate_fg4_mb2_records([record])
    except TypeError as error:
        assert str(error) == "opportunity_rank must be an integer"
    else:
        raise AssertionError("TypeError was not raised for non-int opportunity_rank")


def test_validate_fg4_mb2_records_rejects_zero_opportunity_rank() -> None:
    record = sample_fg4_mb2_record(rank=0)

    try:
        validate_fg4_mb2_records([record])
    except ValueError as error:
        assert str(error) == "opportunity_rank must be >= 1"
    else:
        raise AssertionError("ValueError was not raised for rank < 1")


# ---------------------------------------------------------------------------
# Coordinator output contract tests
# ---------------------------------------------------------------------------

def test_apply_contact_enrichment_coordinator_adds_exactly_three_fields() -> None:
    record = sample_fg4_mb2_record()

    result = apply_contact_enrichment_coordinator([record])[0]

    assert set(result.keys()) == set(FG4_MB2_FIELDS) | {
        "contact_enrichment_request_id",
        "contact_enrichment_request",
        "contact_coordination_state",
    }


def test_apply_contact_enrichment_coordinator_sets_coordination_state() -> None:
    record = sample_fg4_mb2_record()

    result = apply_contact_enrichment_coordinator([record])[0]

    assert result["contact_coordination_state"] == CONTACT_COORDINATION_STATE
    assert CONTACT_COORDINATION_STATE == "prepared"


def test_apply_contact_enrichment_coordinator_request_type_is_locked() -> None:
    record = sample_fg4_mb2_record()

    result = apply_contact_enrichment_coordinator([record])[0]

    assert result["contact_enrichment_request"]["request_type"] == ENRICHMENT_REQUEST_TYPE
    assert ENRICHMENT_REQUEST_TYPE == "contact_enrichment"


def test_apply_contact_enrichment_coordinator_request_has_exact_keys() -> None:
    record = sample_fg4_mb2_record()

    result = apply_contact_enrichment_coordinator([record])[0]
    request = result["contact_enrichment_request"]

    assert set(request.keys()) == {
        "request_type",
        "canonical_organization_id",
        "canonical_organization_key",
        "canonical_organization_name",
        "domain_candidate",
        "source_id",
        "opportunity_rank",
    }


def test_apply_contact_enrichment_coordinator_request_projects_correct_values() -> None:
    record = sample_fg4_mb2_record(
        canonical_id="can_5",
        key="keyX",
        name="Acme Corp",
        domain="acme.com",
        source="src9",
        rank=3,
    )

    result = apply_contact_enrichment_coordinator([record])[0]
    request = result["contact_enrichment_request"]

    assert request["canonical_organization_id"] == "can_5"
    assert request["canonical_organization_key"] == "keyX"
    assert request["canonical_organization_name"] == "Acme Corp"
    assert request["domain_candidate"] == "acme.com"
    assert request["source_id"] == "src9"
    assert request["opportunity_rank"] == 3


def test_apply_contact_enrichment_coordinator_request_id_is_deterministic() -> None:
    record = sample_fg4_mb2_record(key="keyA", source="srcB", rank=2)

    id_first = apply_contact_enrichment_coordinator([record])[0]["contact_enrichment_request_id"]
    id_second = apply_contact_enrichment_coordinator([record])[0]["contact_enrichment_request_id"]

    assert id_first == id_second
    assert id_first == "enrichrequest:keyA:srcB:2"


def test_apply_contact_enrichment_coordinator_request_id_differs_by_key_fields() -> None:
    rec1 = sample_fg4_mb2_record(key="k1", source="s1", rank=1)
    rec2 = sample_fg4_mb2_record(key="k2", source="s1", rank=1)

    results = apply_contact_enrichment_coordinator([rec1, rec2])

    assert (
        results[0]["contact_enrichment_request_id"]
        != results[1]["contact_enrichment_request_id"]
    )


# ---------------------------------------------------------------------------
# Stable ordering, non-mutation, and determinism tests
# ---------------------------------------------------------------------------

def test_apply_contact_enrichment_coordinator_preserves_input_order() -> None:
    records = [
        sample_fg4_mb2_record("can_3", "key3", rank=3),
        sample_fg4_mb2_record("can_1", "key1", rank=1),
        sample_fg4_mb2_record("can_2", "key2", rank=2),
    ]

    result = apply_contact_enrichment_coordinator(records)

    assert [item["canonical_organization_id"] for item in result] == ["can_3", "can_1", "can_2"]


def test_apply_contact_enrichment_coordinator_preserves_upstream_fields() -> None:
    record = sample_fg4_mb2_record("can_1", "key1", "Org A", "a.com", "src1", rank=1)

    result = apply_contact_enrichment_coordinator([record])[0]

    for field_name in FG4_MB2_FIELDS:
        assert result[field_name] == record[field_name]


def test_apply_contact_enrichment_coordinator_is_non_mutating() -> None:
    records = [
        sample_fg4_mb2_record("can_1", "key1", rank=1),
        sample_fg4_mb2_record("can_2", "key2", rank=2),
    ]
    original = copy.deepcopy(records)

    output = apply_contact_enrichment_coordinator(records)

    assert records == original
    assert output is not records
    assert output[0] is not records[0]


def test_apply_contact_enrichment_coordinator_is_deterministic() -> None:
    records = [
        sample_fg4_mb2_record("can_1", "key1", rank=1),
        sample_fg4_mb2_record("can_2", "key2", rank=2),
    ]

    first = apply_contact_enrichment_coordinator(records)
    second = apply_contact_enrichment_coordinator(records)

    assert first == second


def test_apply_contact_enrichment_coordinator_accepts_empty_list() -> None:
    assert apply_contact_enrichment_coordinator([]) == []
