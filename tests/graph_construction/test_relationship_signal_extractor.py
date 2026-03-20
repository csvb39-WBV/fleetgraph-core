import pytest

from src.fleetgraph_core.graph_construction.relationship_signal_extractor import (
    assemble_relationship_signals,
    build_relationship_signal_id,
    build_relationship_signal_record,
    validate_shared_domain_aggregate_records,
)


def _make_record(
    shared_domain_candidate: str = "example.com",
    shared_domain_classification: str = "corporate",
    shared_domain_link_ids=None,
    organization_node_pairs=None,
    organization_node_ids=None,
    supporting_unified_organization_ids=None,
    supporting_source_ids=None,
):
    if shared_domain_link_ids is None:
        shared_domain_link_ids = ["shareddomainlink:example.com:node-1:node-2"]
    if organization_node_pairs is None:
        organization_node_pairs = ["node-1|node-2"]
    if organization_node_ids is None:
        organization_node_ids = ["node-1", "node-2"]
    if supporting_unified_organization_ids is None:
        supporting_unified_organization_ids = ["u1", "u2"]
    if supporting_source_ids is None:
        supporting_source_ids = ["s1", "s2"]

    return {
        "shared_domain_aggregate_id": "shareddomainaggregate:" + shared_domain_candidate,
        "shared_domain_id": "unifieddomain:" + shared_domain_candidate,
        "shared_domain_node_id": "domainnode:" + shared_domain_candidate,
        "shared_domain_candidate": shared_domain_candidate,
        "shared_domain_classification": shared_domain_classification,
        "shared_domain_link_ids": shared_domain_link_ids,
        "organization_node_pairs": organization_node_pairs,
        "organization_node_ids": organization_node_ids,
        "supporting_unified_organization_ids": supporting_unified_organization_ids,
        "supporting_source_ids": supporting_source_ids,
        "candidate_state": "shared_domain_link_aggregated",
    }


def test_validate_shared_domain_aggregate_records_accepts_valid_input() -> None:
    validate_shared_domain_aggregate_records([_make_record()])


def test_validate_shared_domain_aggregate_records_rejects_missing_field() -> None:
    record = _make_record()
    record.pop("shared_domain_id")
    with pytest.raises(ValueError, match="missing or extra fields"):
        validate_shared_domain_aggregate_records([record])


def test_validate_shared_domain_aggregate_records_rejects_extra_field() -> None:
    record = _make_record()
    record["extra_field"] = "x"
    with pytest.raises(ValueError, match="missing or extra fields"):
        validate_shared_domain_aggregate_records([record])


def test_validate_shared_domain_aggregate_records_rejects_invalid_classification() -> None:
    record = _make_record(shared_domain_classification="invalid")
    with pytest.raises(ValueError, match="shared_domain_classification"):
        validate_shared_domain_aggregate_records([record])


def test_validate_shared_domain_aggregate_records_rejects_invalid_candidate_state() -> None:
    record = _make_record()
    record["candidate_state"] = "wrong"
    with pytest.raises(ValueError, match="shared_domain_link_aggregated"):
        validate_shared_domain_aggregate_records([record])


def test_build_relationship_signal_id_is_deterministic() -> None:
    assert build_relationship_signal_id("example.com") == "relationshipsignal:example.com"


def test_build_relationship_signal_record_fields_and_counts() -> None:
    record = _make_record(
        shared_domain_link_ids=["l1", "l2"],
        organization_node_pairs=["node-1|node-2", "node-2|node-3"],
        organization_node_ids=["node-1", "node-2", "node-2", "node-3"],
        supporting_unified_organization_ids=["u1", "u2", "u2", "u3"],
        supporting_source_ids=["s1", "s2", "s2", "s3"],
    )

    result = build_relationship_signal_record(record)

    assert result["relationship_signal_id"] == "relationshipsignal:example.com"
    assert result["signal_type"] == "shared_domain_relationship_detected"
    assert result["shared_domain_aggregate_id"] == record["shared_domain_aggregate_id"]
    assert result["shared_domain_id"] == record["shared_domain_id"]
    assert result["shared_domain_node_id"] == record["shared_domain_node_id"]
    assert result["shared_domain_candidate"] == record["shared_domain_candidate"]
    assert result["shared_domain_classification"] == record["shared_domain_classification"]
    assert result["organization_node_ids"] == ["node-1", "node-2", "node-3"]
    assert result["organization_node_pairs"] == ["node-1|node-2", "node-2|node-3"]
    assert result["link_count"] == 2
    assert result["organization_count"] == 3
    assert result["supporting_unified_organization_ids"] == ["u1", "u2", "u3"]
    assert result["supporting_source_ids"] == ["s1", "s2", "s3"]
    assert result["candidate_state"] == "relationship_signal_extracted"


def test_build_relationship_signal_record_output_contract_exact_keys() -> None:
    result = build_relationship_signal_record(_make_record())
    assert set(result.keys()) == {
        "relationship_signal_id",
        "signal_type",
        "shared_domain_aggregate_id",
        "shared_domain_id",
        "shared_domain_node_id",
        "shared_domain_candidate",
        "shared_domain_classification",
        "organization_node_ids",
        "organization_node_pairs",
        "link_count",
        "organization_count",
        "supporting_unified_organization_ids",
        "supporting_source_ids",
        "candidate_state",
    }


def test_assemble_relationship_signals_preserves_input_order() -> None:
    records = [
        _make_record(shared_domain_candidate="alpha.com"),
        _make_record(shared_domain_candidate="beta.com"),
    ]
    result = assemble_relationship_signals(records)

    assert len(result) == 2
    assert result[0]["relationship_signal_id"] == "relationshipsignal:alpha.com"
    assert result[1]["relationship_signal_id"] == "relationshipsignal:beta.com"


def test_assemble_relationship_signals_emits_one_per_input() -> None:
    records = [
        _make_record(shared_domain_candidate="alpha.com"),
        _make_record(shared_domain_candidate="beta.com"),
        _make_record(shared_domain_candidate="gamma.com"),
    ]
    result = assemble_relationship_signals(records)
    assert len(result) == 3


def test_assemble_relationship_signals_does_not_mutate_input() -> None:
    records = [
        _make_record(
            organization_node_ids=["node-1", "node-2", "node-2"],
            supporting_unified_organization_ids=["u1", "u1", "u2"],
            supporting_source_ids=["s1", "s1", "s2"],
        )
    ]
    snapshot = [
        {k: list(v) if isinstance(v, list) else v for k, v in record.items()}
        for record in records
    ]

    _ = assemble_relationship_signals(records)

    assert records == snapshot
