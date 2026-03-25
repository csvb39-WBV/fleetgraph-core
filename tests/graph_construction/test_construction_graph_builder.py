from __future__ import annotations

import copy
import pathlib
import sys

import pytest


PROJECT_ROOT = pathlib.Path(__file__).resolve().parents[2]
SRC_ROOT = PROJECT_ROOT / "src"

if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))


import fleetgraph_core.graph_construction.construction_graph_builder as graph_builder_module
from fleetgraph_core.graph_construction.construction_graph_builder import (
    build_construction_graph,
    build_construction_graph_batch,
    get_supported_construction_graph_sections,
)
from fleetgraph_core.intelligence.unified_event_schema import build_unified_event_record


def _event_details_for(event_type: str) -> dict[str, str]:
    if event_type == "litigation":
        return {
            "case_id": "CASE-1",
            "case_type": "civil",
            "filing_date": "2026-03-01",
            "plaintiff_role": "plaintiff",
            "defendant_role": "defendant",
        }
    if event_type == "audit":
        return {
            "audit_id": "AUD-1",
            "issue_type": "compliance",
            "opened_date": "2026-03-01",
            "agency": "Inspector",
        }
    if event_type == "enforcement":
        return {
            "action_id": "ENF-1",
            "issue_type": "safety",
            "opened_date": "2026-03-01",
            "agency": "Regulator",
        }
    if event_type == "lien":
        return {
            "lien_id": "LIEN-1",
            "filing_date": "2026-03-01",
            "claimant_role": "claimant",
        }
    return {
        "bond_claim_id": "BOND-1",
        "filing_date": "2026-03-01",
        "claimant_role": "claimant",
    }


def _make_record(
    event_type: str = "litigation",
    event_id: str = "EVT-001",
    company_name: str = "Harbor Construction LLC",
    project_name: str | None = "Port Expansion",
    agency_or_court: str | None = "Travis County Court",
) -> dict[str, object]:
    return build_unified_event_record(
        {
            "event_id": event_id,
            "event_type": event_type,
            "company_name": company_name,
            "source_name": "construction_events",
            "status": "open",
            "event_date": "2026-03-24",
            "jurisdiction": "Texas",
            "project_name": project_name,
            "agency_or_court": agency_or_court,
            "severity": "high",
            "amount": 100000.0,
            "currency": "USD",
            "service_fit": ["litigation_response"],
            "trigger_tags": ["high_risk"],
            "evidence": {
                "summary": "Documented filing in district court",
                "source_record_id": "SRC-101",
            },
            "event_details": _event_details_for(event_type),
        }
    )


def test_supported_section_tuple_returned_correctly() -> None:
    assert get_supported_construction_graph_sections() == (
        "nodes",
        "edges",
        "metadata",
    )


def test_litigation_graph_contains_expected_nodes_and_edges() -> None:
    graph = build_construction_graph(
        _make_record(event_type="litigation", agency_or_court="Travis County Court")
    )

    assert [node["node_type"] for node in graph["nodes"]] == [
        "case",
        "company",
        "project",
        "court",
    ]
    assert [edge["edge_type"] for edge in graph["edges"]] == [
        "SUBJECT_OF_CASE",
        "RELATES_TO_PROJECT",
        "ISSUED_BY",
        "ADJUDICATED_BY",
    ]


def test_audit_graph_contains_expected_nodes_and_edges() -> None:
    graph = build_construction_graph(
        _make_record(event_type="audit", agency_or_court="State Auditor")
    )

    assert [node["node_type"] for node in graph["nodes"]] == [
        "audit",
        "company",
        "project",
        "agency",
    ]
    assert [edge["edge_id"] for edge in graph["edges"]] == [
        "audit:EVT-001->SUBJECT_OF_AUDIT->company:Harbor Construction LLC",
        "audit:EVT-001->RELATES_TO_PROJECT->project:Port Expansion",
        "audit:EVT-001->ISSUED_BY->agency:State Auditor",
        "project:Port Expansion->OVERSEEN_BY->agency:State Auditor",
    ]


def test_enforcement_graph_contains_expected_nodes_and_edges() -> None:
    graph = build_construction_graph(
        _make_record(event_type="enforcement", agency_or_court="Labor Authority")
    )

    assert [node["node_type"] for node in graph["nodes"]] == [
        "enforcement_action",
        "company",
        "project",
        "agency",
    ]
    assert [edge["edge_id"] for edge in graph["edges"]] == [
        "enforcement_action:EVT-001->SUBJECT_OF_ENFORCEMENT->company:Harbor Construction LLC",
        "enforcement_action:EVT-001->RELATES_TO_PROJECT->project:Port Expansion",
        "enforcement_action:EVT-001->ISSUED_BY->agency:Labor Authority",
        "project:Port Expansion->OVERSEEN_BY->agency:Labor Authority",
    ]


def test_lien_graph_contains_no_supporting_entity_relationship_edges() -> None:
    graph = build_construction_graph(
        _make_record(event_type="lien", agency_or_court="County Clerk")
    )

    assert [node["node_type"] for node in graph["nodes"]] == [
        "lien",
        "company",
        "project",
    ]
    assert [edge["edge_type"] for edge in graph["edges"]] == [
        "SUBJECT_OF_CASE",
        "RELATES_TO_PROJECT",
    ]


def test_bond_claim_graph_contains_no_supporting_entity_relationship_edges() -> None:
    graph = build_construction_graph(
        _make_record(event_type="bond_claim", agency_or_court="Bond Board")
    )

    assert [node["node_type"] for node in graph["nodes"]] == [
        "bond_claim",
        "company",
        "project",
    ]
    assert [edge["edge_type"] for edge in graph["edges"]] == [
        "SUBJECT_OF_CASE",
        "RELATES_TO_PROJECT",
    ]


def test_graph_top_level_and_metadata_keys_are_exact() -> None:
    graph = build_construction_graph(_make_record())

    assert set(graph.keys()) == {"nodes", "edges", "metadata"}
    assert set(graph["metadata"].keys()) == {
        "source_event_id",
        "source_event_type",
        "node_count",
        "edge_count",
    }


def test_node_order_is_preserved() -> None:
    graph = build_construction_graph(
        _make_record(event_type="audit", agency_or_court="State Auditor")
    )

    assert [node["node_id"] for node in graph["nodes"]] == [
        "audit:EVT-001",
        "company:Harbor Construction LLC",
        "project:Port Expansion",
        "agency:State Auditor",
    ]


def test_edge_order_is_preserved() -> None:
    graph = build_construction_graph(
        _make_record(event_type="litigation", agency_or_court="Travis County Court")
    )

    assert [edge["edge_id"] for edge in graph["edges"]] == [
        "case:EVT-001->SUBJECT_OF_CASE->company:Harbor Construction LLC",
        "case:EVT-001->RELATES_TO_PROJECT->project:Port Expansion",
        "case:EVT-001->ISSUED_BY->court:Travis County Court",
        "project:Port Expansion->ADJUDICATED_BY->court:Travis County Court",
    ]


def test_duplicate_node_ids_collapse_to_one_preserving_first_occurrence(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_build_event_node(record: dict[str, object]) -> dict[str, object]:
        del record
        return {
            "node_id": "case:EVT-001",
            "node_type": "case",
            "label": "Harbor Construction LLC case",
            "properties": {
                "event_id": "EVT-001",
                "event_type": "litigation",
                "company_name": "Harbor Construction LLC",
                "source_name": "construction_events",
                "status": "open",
                "event_date": "2026-03-24",
                "jurisdiction": "Texas",
                "project_name": "Port Expansion",
                "agency_or_court": "Travis County Court",
                "severity": "high",
                "amount": 100000.0,
                "currency": "USD",
                "service_fit": ["litigation_response"],
                "trigger_tags": ["high_risk"],
                "evidence_summary": "Documented filing in district court",
                "source_record_id": "SRC-101",
                "event_details": {"case_id": "CASE-1"},
            },
        }

    def fake_build_supporting_entity_nodes(
        record: dict[str, object],
    ) -> list[dict[str, object]]:
        del record
        return [
            {
                "node_id": "company:Harbor Construction LLC",
                "node_type": "company",
                "label": "Harbor Construction LLC",
                "properties": {
                    "name": "Harbor Construction LLC",
                    "source_event_id": "EVT-001",
                    "source_event_type": "litigation",
                },
            },
            {
                "node_id": "company:Harbor Construction LLC",
                "node_type": "company",
                "label": "Duplicate Company",
                "properties": {
                    "name": "Duplicate Company",
                    "source_event_id": "EVT-001",
                    "source_event_type": "litigation",
                },
            },
        ]

    monkeypatch.setattr(graph_builder_module, "build_event_node", fake_build_event_node)
    monkeypatch.setattr(
        graph_builder_module,
        "build_supporting_entity_nodes",
        fake_build_supporting_entity_nodes,
    )
    monkeypatch.setattr(
        graph_builder_module,
        "build_event_relationship_edges",
        lambda record: [],
    )
    monkeypatch.setattr(
        graph_builder_module,
        "build_supporting_entity_relationship_edges",
        lambda record: [],
    )

    graph = build_construction_graph(_make_record())

    assert [node["node_id"] for node in graph["nodes"]] == [
        "case:EVT-001",
        "company:Harbor Construction LLC",
    ]
    assert graph["nodes"][1]["label"] == "Harbor Construction LLC"


def test_duplicate_edge_ids_collapse_to_one_preserving_first_occurrence(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        graph_builder_module,
        "build_event_relationship_edges",
        lambda record: [
            {
                "edge_id": "case:EVT-001->SUBJECT_OF_CASE->company:Harbor Construction LLC",
                "from_node": "case:EVT-001",
                "to_node": "company:Harbor Construction LLC",
                "edge_type": "SUBJECT_OF_CASE",
                "properties": {
                    "source_event_id": "EVT-001",
                    "source_event_type": "litigation",
                },
            }
        ],
    )
    monkeypatch.setattr(
        graph_builder_module,
        "build_supporting_entity_relationship_edges",
        lambda record: [
            {
                "edge_id": "case:EVT-001->SUBJECT_OF_CASE->company:Harbor Construction LLC",
                "from_node": "duplicate",
                "to_node": "duplicate",
                "edge_type": "duplicate",
                "properties": {
                    "source_event_id": "EVT-001",
                    "source_event_type": "litigation",
                },
            }
        ],
    )

    graph = build_construction_graph(_make_record())

    assert [edge["edge_id"] for edge in graph["edges"]] == [
        "case:EVT-001->SUBJECT_OF_CASE->company:Harbor Construction LLC"
    ]
    assert graph["edges"][0]["edge_type"] == "SUBJECT_OF_CASE"


def test_malformed_node_id_in_dedupe_raises_exact_message(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        graph_builder_module,
        "build_supporting_entity_nodes",
        lambda record: [
            {
                "node_type": "company",
                "label": "Harbor Construction LLC",
                "properties": {
                    "name": "Harbor Construction LLC",
                    "source_event_id": "EVT-001",
                    "source_event_type": "litigation",
                },
            }
        ],
    )

    with pytest.raises(
        ValueError,
        match="graph assembly could not be completed from the provided record.",
    ):
        build_construction_graph(_make_record())


def test_malformed_edge_id_in_dedupe_raises_exact_message(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        graph_builder_module,
        "build_supporting_entity_relationship_edges",
        lambda record: [
            {
                "from_node": "project:Port Expansion",
                "to_node": "court:Travis County Court",
                "edge_type": "ADJUDICATED_BY",
                "properties": {
                    "source_event_id": "EVT-001",
                    "source_event_type": "litigation",
                },
            }
        ],
    )

    with pytest.raises(
        ValueError,
        match="graph assembly could not be completed from the provided record.",
    ):
        build_construction_graph(_make_record())


def test_all_valid_batch_success() -> None:
    result = build_construction_graph_batch(
        [
            _make_record(event_type="litigation", event_id="EVT-001", agency_or_court="Court A"),
            _make_record(event_type="audit", event_id="EVT-002", agency_or_court="Agency A"),
        ]
    )

    assert result["ok"] is True
    assert result["total_records"] == 2
    assert result["valid_records"] == 2
    assert result["invalid_records"] == 0
    assert result["results"] == [
        {
            "index": 0,
            "ok": True,
            "node_count": 4,
            "edge_count": 4,
            "error": None,
        },
        {
            "index": 1,
            "ok": True,
            "node_count": 4,
            "edge_count": 4,
            "error": None,
        },
    ]
    assert len(result["graphs"]) == 2


def test_mixed_batch_indexed_failure_reporting() -> None:
    invalid_record = _make_record(event_type="audit", event_id="EVT-002")
    invalid_record["status"] = "invalid-status"

    result = build_construction_graph_batch(
        [
            _make_record(event_type="litigation", event_id="EVT-001", agency_or_court="Court A"),
            invalid_record,
        ]
    )

    assert result["ok"] is False
    assert result["total_records"] == 2
    assert result["valid_records"] == 1
    assert result["invalid_records"] == 1
    assert result["results"][0] == {
        "index": 0,
        "ok": True,
        "node_count": 4,
        "edge_count": 4,
        "error": None,
    }
    assert result["results"][1] == {
        "index": 1,
        "ok": False,
        "node_count": None,
        "edge_count": None,
        "error": "status must be one of the supported statuses.",
    }


def test_empty_batch_allowed() -> None:
    result = build_construction_graph_batch([])

    assert result == {
        "ok": True,
        "total_records": 0,
        "valid_records": 0,
        "invalid_records": 0,
        "results": [],
        "graphs": [],
    }


def test_non_list_batch_raises() -> None:
    with pytest.raises(ValueError, match=r"^records must be a list\.$"):
        build_construction_graph_batch({})


def test_non_dict_batch_item_raises() -> None:
    with pytest.raises(ValueError, match=r"^records\[1\] must be a dictionary\.$"):
        build_construction_graph_batch([_make_record(), "bad-item"])


def test_graph_order_preserved_across_batch() -> None:
    result = build_construction_graph_batch(
        [
            _make_record(event_type="bond_claim", event_id="EVT-010", agency_or_court=None),
            _make_record(event_type="audit", event_id="EVT-020", agency_or_court="Agency B"),
        ]
    )

    assert [graph["metadata"]["source_event_id"] for graph in result["graphs"]] == [
        "EVT-010",
        "EVT-020",
    ]


def test_input_record_not_mutated() -> None:
    record = _make_record(event_type="audit", agency_or_court="Agency A")
    snapshot = copy.deepcopy(record)

    _ = build_construction_graph(record)

    assert record == snapshot


def test_output_mutation_does_not_mutate_original_input() -> None:
    record = _make_record(event_type="audit", agency_or_court="Agency A")
    graph = build_construction_graph(record)

    graph["nodes"][0]["properties"]["company_name"] = "Mutated"
    graph["edges"][0]["properties"]["source_event_id"] = "Mutated"

    assert record["company_name"] == "Harbor Construction LLC"
    assert record["event_id"] == "EVT-001"


def test_repeated_runs_deterministic() -> None:
    record = _make_record(event_type="litigation", agency_or_court="Court A")

    first = build_construction_graph(record)
    second = build_construction_graph(record)
    first_batch = build_construction_graph_batch([record])
    second_batch = build_construction_graph_batch([record])

    assert first == second
    assert first_batch == second_batch
