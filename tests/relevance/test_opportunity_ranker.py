from pathlib import Path
import copy
import sys


ROOT = Path(__file__).resolve().parents[2]
SRC = ROOT / "src"

if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from fleetgraph_core.relevance.opportunity_ranker import (  # noqa: E402
    FG4_MB1_FIELDS,
    OPPORTUNITY_RANK_FIELD,
    RELEVANCE_ALLOWED_OUTCOMES,
    apply_opportunity_ranker,
    validate_fg4_mb1_records,
)


def sample_fg4_mb1_record(
    canonical_id: str = "can_1",
    key: str = "key1",
    name: str = "Org A",
    domain: str = "a.com",
    source: str = "src1",
    outcome: str = "relevant",
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
        "relevance_gate_outcome": outcome,
    }


def test_validate_fg4_mb1_records_accepts_valid_input() -> None:
    validate_fg4_mb1_records(
        [
            sample_fg4_mb1_record("can_1", "key1"),
            sample_fg4_mb1_record("can_2", "key2"),
        ]
    )


def test_validate_fg4_mb1_records_rejects_non_list_input() -> None:
    try:
        validate_fg4_mb1_records("invalid")
    except TypeError as error:
        assert str(error) == "records must be a list"
    else:
        raise AssertionError("TypeError was not raised for non-list input")


def test_validate_fg4_mb1_records_rejects_non_dict_record() -> None:
    try:
        validate_fg4_mb1_records(["invalid"])
    except TypeError as error:
        assert str(error) == "each record must be a dictionary"
    else:
        raise AssertionError("TypeError was not raised for non-dict record")


def test_validate_fg4_mb1_records_rejects_missing_field() -> None:
    record = sample_fg4_mb1_record()
    del record["canonical_organization_name"]

    try:
        validate_fg4_mb1_records([record])
    except ValueError as error:
        assert str(error) == "record is missing required fields: canonical_organization_name"
    else:
        raise AssertionError("ValueError was not raised for missing field")


def test_validate_fg4_mb1_records_rejects_extra_field() -> None:
    record = sample_fg4_mb1_record()
    record["extra_field"] = "x"

    try:
        validate_fg4_mb1_records([record])
    except ValueError as error:
        assert str(error) == "record contains unknown fields: extra_field"
    else:
        raise AssertionError("ValueError was not raised for extra field")


def test_validate_fg4_mb1_records_rejects_wrong_candidate_state() -> None:
    record = sample_fg4_mb1_record()
    record["candidate_state"] = "parsed"

    try:
        validate_fg4_mb1_records([record])
    except ValueError as error:
        assert str(error) == "candidate_state must be exactly 'canonicalized'"
    else:
        raise AssertionError("ValueError was not raised for invalid candidate_state")


def test_validate_fg4_mb1_records_rejects_wrong_relevance_gate_outcome() -> None:
    record = sample_fg4_mb1_record(outcome="not_relevant")

    try:
        validate_fg4_mb1_records([record])
    except ValueError as error:
        assert str(error) == "relevance_gate_outcome must be exactly 'relevant'"
    else:
        raise AssertionError("ValueError was not raised for invalid relevance_gate_outcome")


def test_apply_opportunity_ranker_assigns_one_based_rank() -> None:
    records = [
        sample_fg4_mb1_record("can_1", "key1"),
        sample_fg4_mb1_record("can_2", "key2"),
        sample_fg4_mb1_record("can_3", "key3"),
    ]

    result = apply_opportunity_ranker(records)

    assert [item[OPPORTUNITY_RANK_FIELD] for item in result] == [1, 2, 3]


def test_apply_opportunity_ranker_preserves_input_order() -> None:
    records = [
        sample_fg4_mb1_record("can_3", "key3"),
        sample_fg4_mb1_record("can_1", "key1"),
        sample_fg4_mb1_record("can_2", "key2"),
    ]

    result = apply_opportunity_ranker(records)

    assert [item["canonical_organization_id"] for item in result] == ["can_3", "can_1", "can_2"]
    assert [item[OPPORTUNITY_RANK_FIELD] for item in result] == [1, 2, 3]


def test_apply_opportunity_ranker_preserves_upstream_fields() -> None:
    record = sample_fg4_mb1_record("can_1", "key1", "Org A", "a.com", "src1", "relevant")

    result = apply_opportunity_ranker([record])[0]

    for field_name in FG4_MB1_FIELDS:
        assert result[field_name] == record[field_name]


def test_apply_opportunity_ranker_output_schema_is_input_plus_rank_only() -> None:
    record = sample_fg4_mb1_record()

    result = apply_opportunity_ranker([record])[0]

    assert set(result.keys()) == set(FG4_MB1_FIELDS) | {OPPORTUNITY_RANK_FIELD}


def test_apply_opportunity_ranker_is_non_mutating() -> None:
    records = [
        sample_fg4_mb1_record("can_1", "key1"),
        sample_fg4_mb1_record("can_2", "key2"),
    ]
    original = copy.deepcopy(records)

    output = apply_opportunity_ranker(records)

    assert records == original
    assert output is not records
    assert output[0] is not records[0]


def test_apply_opportunity_ranker_does_not_mutate_relevance_gate_outcome() -> None:
    records = [sample_fg4_mb1_record("can_1", "key1", outcome="relevant")]

    result = apply_opportunity_ranker(records)

    assert result[0]["relevance_gate_outcome"] == "relevant"


def test_apply_opportunity_ranker_is_deterministic() -> None:
    records = [
        sample_fg4_mb1_record("can_1", "key1"),
        sample_fg4_mb1_record("can_2", "key2"),
    ]

    first = apply_opportunity_ranker(records)
    second = apply_opportunity_ranker(records)

    assert first == second


def test_apply_opportunity_ranker_accepts_empty_list() -> None:
    assert RELEVANCE_ALLOWED_OUTCOMES == ("relevant",)
    assert apply_opportunity_ranker([]) == []
