import pytest

from src.fleetgraph_core.graph_construction.shared_domain_link_aggregator import (
    assemble_shared_domain_aggregates,
    build_shared_domain_aggregate,
    build_shared_domain_aggregate_key,
    merge_shared_domain_link_group,
    validate_shared_domain_link_records,
)


def _make_record(
    link_id: str,
    left_id: str,
    right_id: str,
    shared_domain_id: str,
    shared_domain_node_id: str,
    shared_domain_candidate: str,
    shared_domain_classification: str = "corporate",
    supporting_unified_organization_ids=None,
    supporting_source_ids=None,
):
    if supporting_unified_organization_ids is None:
        supporting_unified_organization_ids = ["u1"]
    if supporting_source_ids is None:
        supporting_source_ids = ["s1"]

    return {
        "shared_domain_link_id": link_id,
        "left_organization_node_id": left_id,
        "right_organization_node_id": right_id,
        "shared_domain_id": shared_domain_id,
        "shared_domain_node_id": shared_domain_node_id,
        "shared_domain_candidate": shared_domain_candidate,
        "shared_domain_classification": shared_domain_classification,
        "supporting_unified_organization_ids": supporting_unified_organization_ids,
        "supporting_source_ids": supporting_source_ids,
        "candidate_state": "shared_domain_link_built",
    }


def test_validate_shared_domain_link_records_accepts_valid_input() -> None:
    record = _make_record(
        link_id="shareddomainlink:example.com:node-1:node-2",
        left_id="node-1",
        right_id="node-2",
        shared_domain_id="unifieddomain:example.com",
        shared_domain_node_id="domainnode:example.com",
        shared_domain_candidate="example.com",
    )
    validate_shared_domain_link_records([record])


def test_validate_shared_domain_link_records_rejects_missing_field() -> None:
    record = _make_record(
        link_id="shareddomainlink:example.com:node-1:node-2",
        left_id="node-1",
        right_id="node-2",
        shared_domain_id="unifieddomain:example.com",
        shared_domain_node_id="domainnode:example.com",
        shared_domain_candidate="example.com",
    )
    record.pop("shared_domain_id")

    with pytest.raises(ValueError, match="missing or extra fields"):
        validate_shared_domain_link_records([record])


def test_validate_shared_domain_link_records_rejects_extra_field() -> None:
    record = _make_record(
        link_id="shareddomainlink:example.com:node-1:node-2",
        left_id="node-1",
        right_id="node-2",
        shared_domain_id="unifieddomain:example.com",
        shared_domain_node_id="domainnode:example.com",
        shared_domain_candidate="example.com",
    )
    record["extra"] = "x"

    with pytest.raises(ValueError, match="missing or extra fields"):
        validate_shared_domain_link_records([record])


def test_validate_shared_domain_link_records_rejects_equal_left_right() -> None:
    record = _make_record(
        link_id="shareddomainlink:example.com:node-1:node-1",
        left_id="node-1",
        right_id="node-1",
        shared_domain_id="unifieddomain:example.com",
        shared_domain_node_id="domainnode:example.com",
        shared_domain_candidate="example.com",
    )

    with pytest.raises(ValueError, match="must not equal"):
        validate_shared_domain_link_records([record])


def test_validate_shared_domain_link_records_rejects_invalid_candidate_state() -> None:
    record = _make_record(
        link_id="shareddomainlink:example.com:node-1:node-2",
        left_id="node-1",
        right_id="node-2",
        shared_domain_id="unifieddomain:example.com",
        shared_domain_node_id="domainnode:example.com",
        shared_domain_candidate="example.com",
    )
    record["candidate_state"] = "wrong"

    with pytest.raises(ValueError, match="shared_domain_link_built"):
        validate_shared_domain_link_records([record])


def test_build_shared_domain_aggregate_key() -> None:
    record = _make_record(
        link_id="shareddomainlink:example.com:node-1:node-2",
        left_id="node-1",
        right_id="node-2",
        shared_domain_id="unifieddomain:example.com",
        shared_domain_node_id="domainnode:example.com",
        shared_domain_candidate="example.com",
    )
    assert build_shared_domain_aggregate_key(record) == "unifieddomain:example.com"


def test_merge_shared_domain_link_group_builds_expected_aggregate() -> None:
    group = [
        _make_record(
            link_id="shareddomainlink:example.com:node-1:node-2",
            left_id="node-1",
            right_id="node-2",
            shared_domain_id="unifieddomain:example.com",
            shared_domain_node_id="domainnode:example.com",
            shared_domain_candidate="example.com",
            supporting_unified_organization_ids=["u1", "u2"],
            supporting_source_ids=["s1", "s2"],
        ),
        _make_record(
            link_id="shareddomainlink:example.com:node-2:node-3",
            left_id="node-2",
            right_id="node-3",
            shared_domain_id="unifieddomain:example.com",
            shared_domain_node_id="domainnode:example.com",
            shared_domain_candidate="example.com",
            supporting_unified_organization_ids=["u2", "u3"],
            supporting_source_ids=["s2", "s3"],
        ),
    ]

    result = merge_shared_domain_link_group(group)

    assert result == {
        "shared_domain_aggregate_id": "shareddomainaggregate:example.com",
        "shared_domain_id": "unifieddomain:example.com",
        "shared_domain_node_id": "domainnode:example.com",
        "shared_domain_candidate": "example.com",
        "shared_domain_classification": "corporate",
        "shared_domain_link_ids": [
            "shareddomainlink:example.com:node-1:node-2",
            "shareddomainlink:example.com:node-2:node-3",
        ],
        "organization_node_pairs": ["node-1|node-2", "node-2|node-3"],
        "organization_node_ids": ["node-1", "node-2", "node-3"],
        "supporting_unified_organization_ids": ["u1", "u2", "u3"],
        "supporting_source_ids": ["s1", "s2", "s3"],
        "candidate_state": "shared_domain_link_aggregated",
    }


def test_build_shared_domain_aggregate_groups_by_shared_domain_id_and_preserves_group_order() -> None:
    records = [
        _make_record(
            link_id="shareddomainlink:alpha.com:node-1:node-2",
            left_id="node-1",
            right_id="node-2",
            shared_domain_id="unifieddomain:alpha.com",
            shared_domain_node_id="domainnode:alpha.com",
            shared_domain_candidate="alpha.com",
        ),
        _make_record(
            link_id="shareddomainlink:beta.com:node-3:node-4",
            left_id="node-3",
            right_id="node-4",
            shared_domain_id="unifieddomain:beta.com",
            shared_domain_node_id="domainnode:beta.com",
            shared_domain_candidate="beta.com",
            shared_domain_classification="generic",
        ),
        _make_record(
            link_id="shareddomainlink:alpha.com:node-2:node-5",
            left_id="node-2",
            right_id="node-5",
            shared_domain_id="unifieddomain:alpha.com",
            shared_domain_node_id="domainnode:alpha.com",
            shared_domain_candidate="alpha.com",
        ),
    ]

    result = build_shared_domain_aggregate(records)

    assert len(result) == 2
    assert result[0]["shared_domain_id"] == "unifieddomain:alpha.com"
    assert result[1]["shared_domain_id"] == "unifieddomain:beta.com"

    assert result[0]["shared_domain_link_ids"] == [
        "shareddomainlink:alpha.com:node-1:node-2",
        "shareddomainlink:alpha.com:node-2:node-5",
    ]


def test_assemble_shared_domain_aggregates_state_transition_and_schema() -> None:
    records = [
        _make_record(
            link_id="shareddomainlink:example.com:node-1:node-2",
            left_id="node-1",
            right_id="node-2",
            shared_domain_id="unifieddomain:example.com",
            shared_domain_node_id="domainnode:example.com",
            shared_domain_candidate="example.com",
        )
    ]

    result = assemble_shared_domain_aggregates(records)

    assert len(result) == 1
    assert result[0]["candidate_state"] == "shared_domain_link_aggregated"
    assert set(result[0].keys()) == {
        "shared_domain_aggregate_id",
        "shared_domain_id",
        "shared_domain_node_id",
        "shared_domain_candidate",
        "shared_domain_classification",
        "shared_domain_link_ids",
        "organization_node_pairs",
        "organization_node_ids",
        "supporting_unified_organization_ids",
        "supporting_source_ids",
        "candidate_state",
    }


def test_assemble_shared_domain_aggregates_does_not_mutate_input() -> None:
    records = [
        _make_record(
            link_id="shareddomainlink:example.com:node-1:node-2",
            left_id="node-1",
            right_id="node-2",
            shared_domain_id="unifieddomain:example.com",
            shared_domain_node_id="domainnode:example.com",
            shared_domain_candidate="example.com",
            supporting_unified_organization_ids=["u1", "u2"],
            supporting_source_ids=["s1", "s2"],
        )
    ]

    original = [
        {
            key: list(value) if isinstance(value, list) else value
            for key, value in record.items()
        }
        for record in records
    ]

    _ = assemble_shared_domain_aggregates(records)

    assert records == original
