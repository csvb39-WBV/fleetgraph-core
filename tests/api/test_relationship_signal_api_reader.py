import json

import pytest

from src.fleetgraph_core.api.relationship_signal_api_reader import (
    get_relationship_signal_output_summary,
    get_relationship_signal_records,
    load_relationship_signal_output,
    validate_relationship_signal_output_payload,
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


def _make_payload(records=None):
    if records is None:
        records = [_make_record()]

    return {
        "output_type": "relationship_signal_output",
        "output_schema_version": "1.0",
        "record_count": len(records),
        "records": records,
    }


def test_validate_relationship_signal_output_payload_accepts_valid_payload() -> None:
    validate_relationship_signal_output_payload(_make_payload())


def test_validate_relationship_signal_output_payload_rejects_missing_top_level_field() -> None:
    payload = _make_payload()
    payload.pop("record_count")

    with pytest.raises(ValueError, match="top-level fields"):
        validate_relationship_signal_output_payload(payload)


def test_validate_relationship_signal_output_payload_rejects_extra_top_level_field() -> None:
    payload = _make_payload()
    payload["extra"] = True

    with pytest.raises(ValueError, match="top-level fields"):
        validate_relationship_signal_output_payload(payload)


def test_validate_relationship_signal_output_payload_rejects_record_count_mismatch() -> None:
    payload = _make_payload()
    payload["record_count"] = 99

    with pytest.raises(ValueError, match="record_count must equal len\(records\)"):
        validate_relationship_signal_output_payload(payload)


def test_validate_relationship_signal_output_payload_rejects_invalid_record() -> None:
    payload = _make_payload()
    payload["records"][0]["candidate_state"] = "bad"

    with pytest.raises(ValueError, match="relationship_signal_formatted"):
        validate_relationship_signal_output_payload(payload)


def test_validate_relationship_signal_output_payload_rejects_extra_record_field() -> None:
    payload = _make_payload()
    payload["records"][0]["extra"] = "x"

    with pytest.raises(ValueError, match="record has missing or extra fields"):
        validate_relationship_signal_output_payload(payload)


def test_load_relationship_signal_output_loads_and_validates_file(tmp_path) -> None:
    payload = _make_payload(
        records=[
            _make_record(),
            _make_record(
                output_record_id="formattedsignal:relationshipsignal:beta.com",
                signal_id="relationshipsignal:beta.com",
                domain="beta.com",
            ),
        ]
    )
    path = tmp_path / "relationship_signals_output.json"
    path.write_text(json.dumps(payload), encoding="utf-8")

    loaded = load_relationship_signal_output(str(path))

    assert loaded == payload


def test_get_relationship_signal_records_returns_ordered_copy() -> None:
    payload = _make_payload(
        records=[
            _make_record(signal_id="relationshipsignal:alpha.com", domain="alpha.com"),
            _make_record(signal_id="relationshipsignal:beta.com", domain="beta.com"),
        ]
    )

    records = get_relationship_signal_records(payload)

    assert records == payload["records"]
    assert records[0]["signal_id"] == "relationshipsignal:alpha.com"
    assert records[1]["signal_id"] == "relationshipsignal:beta.com"
    assert records is not payload["records"]


def test_get_relationship_signal_output_summary_returns_exact_summary() -> None:
    payload = _make_payload()

    summary = get_relationship_signal_output_summary(payload)

    assert summary == {
        "output_type": "relationship_signal_output",
        "output_schema_version": "1.0",
        "record_count": 1,
    }


def test_reader_functions_do_not_mutate_payload() -> None:
    payload = _make_payload(
        records=[
            _make_record(
                organization_node_ids=["node-1", "node-2"],
                organization_node_pairs=["node-1|node-2"],
                supporting_unified_organization_ids=["u1", "u2"],
                supporting_source_ids=["s1", "s2"],
            )
        ]
    )
    snapshot = {
        "output_type": payload["output_type"],
        "output_schema_version": payload["output_schema_version"],
        "record_count": payload["record_count"],
        "records": [
            {
                key: list(value) if isinstance(value, list) else value
                for key, value in record.items()
            }
            for record in payload["records"]
        ],
    }

    validate_relationship_signal_output_payload(payload)
    _ = get_relationship_signal_records(payload)
    _ = get_relationship_signal_output_summary(payload)

    assert payload == snapshot