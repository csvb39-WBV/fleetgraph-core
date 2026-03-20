import json

import pytest

from src.fleetgraph_core.output.relationship_signal_output_writer import (
    build_output_payload,
    serialize_relationship_signal_output,
    validate_formatted_relationship_signal_records,
    write_relationship_signal_output,
)


def _make_record(
    output_record_id: str = "formattedsignal:relationshipsignal:example.com",
    signal_id: str = "relationshipsignal:example.com",
    signal_type: str = "shared_domain_relationship_detected",
    domain: str = "example.com",
    domain_classification: str = "corporate",
    organization_count: int = 2,
    link_count: int = 1,
    organization_node_ids=None,
    organization_node_pairs=None,
    supporting_unified_organization_ids=None,
    supporting_source_ids=None,
):
    if organization_node_ids is None:
        organization_node_ids = ["node-1", "node-2"]
    if organization_node_pairs is None:
        organization_node_pairs = ["node-1|node-2"]
    if supporting_unified_organization_ids is None:
        supporting_unified_organization_ids = ["u1", "u2"]
    if supporting_source_ids is None:
        supporting_source_ids = ["s1", "s2"]

    return {
        "output_record_id": output_record_id,
        "output_schema_version": "1.0",
        "signal_id": signal_id,
        "signal_type": signal_type,
        "domain": domain,
        "domain_classification": domain_classification,
        "organization_count": organization_count,
        "link_count": link_count,
        "organization_node_ids": organization_node_ids,
        "organization_node_pairs": organization_node_pairs,
        "supporting_unified_organization_ids": supporting_unified_organization_ids,
        "supporting_source_ids": supporting_source_ids,
        "candidate_state": "relationship_signal_formatted",
    }


def test_validate_formatted_relationship_signal_records_accepts_valid_input() -> None:
    validate_formatted_relationship_signal_records([_make_record()])


def test_validate_formatted_relationship_signal_records_rejects_missing_field() -> None:
    record = _make_record()
    record.pop("signal_id")

    with pytest.raises(ValueError, match="missing or extra fields"):
        validate_formatted_relationship_signal_records([record])


def test_validate_formatted_relationship_signal_records_rejects_extra_field() -> None:
    record = _make_record()
    record["extra_field"] = "x"

    with pytest.raises(ValueError, match="missing or extra fields"):
        validate_formatted_relationship_signal_records([record])


def test_validate_formatted_relationship_signal_records_rejects_invalid_schema_version() -> None:
    record = _make_record()
    record["output_schema_version"] = "2.0"

    with pytest.raises(ValueError, match="output_schema_version must be exactly '1.0'"):
        validate_formatted_relationship_signal_records([record])


def test_validate_formatted_relationship_signal_records_rejects_invalid_classification() -> None:
    record = _make_record(domain_classification="invalid")

    with pytest.raises(ValueError, match="domain_classification"):
        validate_formatted_relationship_signal_records([record])


def test_validate_formatted_relationship_signal_records_rejects_invalid_candidate_state() -> None:
    record = _make_record()
    record["candidate_state"] = "bad"

    with pytest.raises(ValueError, match="relationship_signal_formatted"):
        validate_formatted_relationship_signal_records([record])


def test_validate_formatted_relationship_signal_records_rejects_negative_count() -> None:
    record = _make_record(link_count=-1)

    with pytest.raises(ValueError, match="non-negative integer"):
        validate_formatted_relationship_signal_records([record])


def test_build_output_payload_returns_exact_envelope() -> None:
    records = [
        _make_record(),
        _make_record(
            output_record_id="formattedsignal:relationshipsignal:beta.com",
            signal_id="relationshipsignal:beta.com",
            domain="beta.com",
        ),
    ]

    payload = build_output_payload(records)

    assert set(payload.keys()) == {
        "output_type",
        "output_schema_version",
        "record_count",
        "records",
    }
    assert payload["output_type"] == "relationship_signal_output"
    assert payload["output_schema_version"] == "1.0"
    assert payload["record_count"] == 2
    assert payload["records"] == records


def test_build_output_payload_does_not_mutate_input() -> None:
    records = [
        _make_record(
            organization_node_ids=["node-1", "node-2"],
            organization_node_pairs=["node-1|node-2"],
            supporting_unified_organization_ids=["u1", "u2"],
            supporting_source_ids=["s1", "s2"],
        )
    ]
    snapshot = [
        {key: list(value) if isinstance(value, list) else value for key, value in record.items()}
        for record in records
    ]

    _ = build_output_payload(records)

    assert records == snapshot


def test_serialize_relationship_signal_output_is_deterministic() -> None:
    payload = build_output_payload([_make_record()])

    serialized = serialize_relationship_signal_output(payload)
    expected = json.dumps(payload, indent=2, sort_keys=False, ensure_ascii=True)

    assert serialized == expected


def test_write_relationship_signal_output_writes_target_path(tmp_path) -> None:
    records = [_make_record()]
    output_path = tmp_path / "custom_output.json"

    returned_path = write_relationship_signal_output(records, str(output_path))

    assert returned_path == str(output_path)
    assert output_path.exists()
    assert output_path.read_text(encoding="utf-8") == serialize_relationship_signal_output(
        build_output_payload(records)
    )


def test_write_relationship_signal_output_uses_default_filename(monkeypatch, tmp_path) -> None:
    monkeypatch.chdir(tmp_path)

    returned_path = write_relationship_signal_output([_make_record()])

    assert returned_path == "relationship_signals_output.json"
    assert (tmp_path / "relationship_signals_output.json").exists()