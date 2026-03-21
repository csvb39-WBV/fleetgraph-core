from pathlib import Path
import copy
import sys


ROOT = Path(__file__).resolve().parents[2]
SRC = ROOT / "src"

if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from fleetgraph_core.relevance.signal_relevance_gate import (  # noqa: E402
    FG3_CANONICAL_FIELDS,
    RELEVANCE_GATE_ALLOWED_OUTCOMES,
    apply_signal_relevance_gate,
    validate_fg3_canonical_records,
)


def sample_canonical_record(
    canonical_id: str = "can_1",
    key: str = "key1",
    name: str = "Org A",
    domain: str = "a.com",
    source: str = "src1",
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
    }


def test_validate_fg3_canonical_records_accepts_valid_input() -> None:
    validate_fg3_canonical_records(
        [
            sample_canonical_record("can_1", "key1"),
            sample_canonical_record("can_2", "key2"),
        ]
    )


def test_validate_fg3_canonical_records_rejects_non_list_input() -> None:
    try:
        validate_fg3_canonical_records("invalid")
    except TypeError as error:
        assert str(error) == "records must be a list"
    else:
        raise AssertionError("TypeError was not raised for non-list input")


def test_validate_fg3_canonical_records_rejects_non_dict_record() -> None:
    try:
        validate_fg3_canonical_records(["invalid"])
    except TypeError as error:
        assert str(error) == "each record must be a dictionary"
    else:
        raise AssertionError("TypeError was not raised for non-dict record")


def test_validate_fg3_canonical_records_rejects_missing_field() -> None:
    record = sample_canonical_record()
    del record["canonical_organization_name"]

    try:
        validate_fg3_canonical_records([record])
    except ValueError as error:
        assert str(error) == "canonical record is missing required fields: canonical_organization_name"
    else:
        raise AssertionError("ValueError was not raised for missing field")


def test_validate_fg3_canonical_records_rejects_extra_field() -> None:
    record = sample_canonical_record()
    record["extra_field"] = "x"

    try:
        validate_fg3_canonical_records([record])
    except ValueError as error:
        assert str(error) == "canonical record contains unknown fields: extra_field"
    else:
        raise AssertionError("ValueError was not raised for extra field")


def test_validate_fg3_canonical_records_rejects_wrong_candidate_state() -> None:
    record = sample_canonical_record()
    record["candidate_state"] = "parsed"

    try:
        validate_fg3_canonical_records([record])
    except ValueError as error:
        assert str(error) == "candidate_state must be exactly 'canonicalized'"
    else:
        raise AssertionError("ValueError was not raised for invalid candidate_state")


def test_apply_signal_relevance_gate_returns_relevant_for_valid_records() -> None:
    records = [
        sample_canonical_record("can_1", "key1"),
        sample_canonical_record("can_2", "key2"),
    ]

    result = apply_signal_relevance_gate(records)

    assert len(result) == 2
    assert RELEVANCE_GATE_ALLOWED_OUTCOMES == ("relevant",)
    assert result[0]["relevance_gate_outcome"] == "relevant"
    assert result[1]["relevance_gate_outcome"] == "relevant"


def test_apply_signal_relevance_gate_preserves_stable_ordering() -> None:
    records = [
        sample_canonical_record("can_2", "key2"),
        sample_canonical_record("can_1", "key1"),
        sample_canonical_record("can_3", "key3"),
    ]

    result = apply_signal_relevance_gate(records)

    assert [item["canonical_organization_id"] for item in result] == ["can_2", "can_1", "can_3"]


def test_apply_signal_relevance_gate_preserves_fg3_identity_fields() -> None:
    record = sample_canonical_record("can_1", "key1", "Org A", "a.com", "src1")

    result = apply_signal_relevance_gate([record])[0]

    for field_name in FG3_CANONICAL_FIELDS:
        assert result[field_name] == record[field_name]


def test_apply_signal_relevance_gate_output_schema_is_fg3_plus_single_field() -> None:
    record = sample_canonical_record()

    result = apply_signal_relevance_gate([record])[0]

    assert set(result.keys()) == set(FG3_CANONICAL_FIELDS) | {"relevance_gate_outcome"}


def test_apply_signal_relevance_gate_is_non_mutating() -> None:
    records = [
        sample_canonical_record("can_1", "key1"),
        sample_canonical_record("can_2", "key2"),
    ]
    original = copy.deepcopy(records)

    output = apply_signal_relevance_gate(records)

    assert records == original
    assert output is not records
    assert output[0] is not records[0]


def test_apply_signal_relevance_gate_is_deterministic() -> None:
    records = [
        sample_canonical_record("can_1", "key1"),
        sample_canonical_record("can_2", "key2"),
    ]

    first = apply_signal_relevance_gate(records)
    second = apply_signal_relevance_gate(records)

    assert first == second


def test_apply_signal_relevance_gate_accepts_empty_list() -> None:
    assert apply_signal_relevance_gate([]) == []
