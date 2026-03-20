import pytest

from src.fleetgraph_core.output.relationship_signal_formatter import (
    assemble_formatted_relationship_signals,
    build_formatted_relationship_signal_record,
    build_output_record_id,
    validate_relationship_signal_records,
)


def _make_record(
    signal_id: str = "relationshipsignal:example.com",
    signal_type: str = "shared_domain_relationship_detected",
    shared_domain_candidate: str = "example.com",
    shared_domain_classification: str = "corporate",
    organization_node_ids=None,
    organization_node_pairs=None,
    link_count: int = 1,
    organization_count: int = 2,
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
        "relationship_signal_id": signal_id,
        "signal_type": signal_type,
        "shared_domain_aggregate_id": "shareddomainaggregate:" + shared_domain_candidate,
        "shared_domain_id": "unifieddomain:" + shared_domain_candidate,
        "shared_domain_node_id": "domainnode:" + shared_domain_candidate,
        "shared_domain_candidate": shared_domain_candidate,
        "shared_domain_classification": shared_domain_classification,
        "organization_node_ids": organization_node_ids,
        "organization_node_pairs": organization_node_pairs,
        "link_count": link_count,
        "organization_count": organization_count,
        "supporting_unified_organization_ids": supporting_unified_organization_ids,
        "supporting_source_ids": supporting_source_ids,
        "candidate_state": "relationship_signal_extracted",
    }


def test_validate_relationship_signal_records_accepts_valid_input() -> None:
    validate_relationship_signal_records([_make_record()])


def test_validate_relationship_signal_records_rejects_missing_fields() -> None:
    record = _make_record()
    record.pop("shared_domain_id")
    with pytest.raises(ValueError, match="missing or extra fields"):
        validate_relationship_signal_records([record])


def test_validate_relationship_signal_records_rejects_extra_fields() -> None:
    record = _make_record()
    record["extra"] = "x"
    with pytest.raises(ValueError, match="missing or extra fields"):
        validate_relationship_signal_records([record])


def test_validate_relationship_signal_records_rejects_negative_counts() -> None:
    record = _make_record(link_count=-1)
    with pytest.raises(ValueError, match="non-negative integer"):
        validate_relationship_signal_records([record])


def test_validate_relationship_signal_records_rejects_bad_classification() -> None:
    record = _make_record(shared_domain_classification="invalid")
    with pytest.raises(ValueError, match="shared_domain_classification"):
        validate_relationship_signal_records([record])


def test_validate_relationship_signal_records_rejects_bad_candidate_state() -> None:
    record = _make_record()
    record["candidate_state"] = "bad"
    with pytest.raises(ValueError, match="relationship_signal_extracted"):
        validate_relationship_signal_records([record])


def test_build_output_record_id() -> None:
    result = build_output_record_id("relationshipsignal:example.com")
    assert result == "formattedsignal:relationshipsignal:example.com"


def test_build_formatted_relationship_signal_record_mapping_and_contract() -> None:
    record = _make_record(
        organization_node_ids=["node-1", "node-2"],
        organization_node_pairs=["node-1|node-2", "node-1|node-3"],
        link_count=2,
        organization_count=3,
        supporting_unified_organization_ids=["u1", "u2", "u2", "u3"],
        supporting_source_ids=["s1", "s2", "s2", "s3"],
    )

    result = build_formatted_relationship_signal_record(record)

    assert set(result.keys()) == {
        "output_record_id",
        "output_schema_version",
        "signal_id",
        "signal_type",
        "domain",
        "domain_classification",
        "organization_count",
        "link_count",
        "organization_node_ids",
        "organization_node_pairs",
        "supporting_unified_organization_ids",
        "supporting_source_ids",
        "candidate_state",
    }
    assert result["output_record_id"] == "formattedsignal:relationshipsignal:example.com"
    assert result["output_schema_version"] == "1.0"
    assert result["signal_id"] == "relationshipsignal:example.com"
    assert result["signal_type"] == "shared_domain_relationship_detected"
    assert result["domain"] == "example.com"
    assert result["domain_classification"] == "corporate"
    assert result["organization_count"] == 3
    assert result["link_count"] == 2
    assert result["organization_node_ids"] == ["node-1", "node-2"]
    assert result["organization_node_pairs"] == ["node-1|node-2", "node-1|node-3"]
    assert result["supporting_unified_organization_ids"] == ["u1", "u2", "u3"]
    assert result["supporting_source_ids"] == ["s1", "s2", "s3"]
    assert result["candidate_state"] == "relationship_signal_formatted"


def test_assemble_formatted_relationship_signals_one_to_one_and_ordered() -> None:
    records = [
        _make_record(signal_id="relationshipsignal:alpha.com", shared_domain_candidate="alpha.com"),
        _make_record(signal_id="relationshipsignal:beta.com", shared_domain_candidate="beta.com"),
    ]

    result = assemble_formatted_relationship_signals(records)

    assert len(result) == 2
    assert result[0]["signal_id"] == "relationshipsignal:alpha.com"
    assert result[1]["signal_id"] == "relationshipsignal:beta.com"


def test_assemble_formatted_relationship_signals_does_not_mutate_input() -> None:
    records = [
        _make_record(
            supporting_unified_organization_ids=["u1", "u1", "u2"],
            supporting_source_ids=["s1", "s1", "s2"],
        )
    ]
    snapshot = [
        {key: list(value) if isinstance(value, list) else value for key, value in record.items()}
        for record in records
    ]

    _ = assemble_formatted_relationship_signals(records)

    assert records == snapshot
