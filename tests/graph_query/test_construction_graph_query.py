from __future__ import annotations

import copy
import pathlib
import sys

import pytest


PROJECT_ROOT = pathlib.Path(__file__).resolve().parents[2]
SRC_ROOT = PROJECT_ROOT / "src"

if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))


import fleetgraph_core.graph_query.construction_graph_query as graph_query_module
from fleetgraph_core.graph_query.construction_graph_query import (
    get_supported_construction_query_sections,
    query_construction_graph,
    query_construction_graph_batch,
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


def test_supported_query_section_tuple_returned_correctly() -> None:
    assert get_supported_construction_query_sections() == (
        "node_matches",
        "edge_matches",
        "summary",
    )


def test_litigation_node_query_returns_only_court_node() -> None:
    result = query_construction_graph(
        _make_record(event_type="litigation", agency_or_court="Travis County Court"),
        node_type="court",
    )

    assert result["node_matches"] == [
        {
            "node_id": "court:Travis County Court",
            "node_type": "court",
            "label": "Travis County Court",
            "properties": {
                "name": "Travis County Court",
                "source_event_id": "EVT-001",
                "source_event_type": "litigation",
            },
        }
    ]


def test_audit_node_query_returns_only_agency_node() -> None:
    result = query_construction_graph(
        _make_record(event_type="audit", agency_or_court="State Auditor"),
        node_type="agency",
    )

    assert [node["node_id"] for node in result["node_matches"]] == [
        "agency:State Auditor"
    ]


def test_lien_node_query_for_agency_returns_empty_matches() -> None:
    result = query_construction_graph(
        _make_record(event_type="lien", agency_or_court="County Clerk"),
        node_type="agency",
    )

    assert result["node_matches"] == []


def test_node_type_none_returns_all_nodes_in_original_order() -> None:
    result = query_construction_graph(
        _make_record(event_type="audit", agency_or_court="State Auditor"),
        node_type=None,
    )

    assert [node["node_id"] for node in result["node_matches"]] == [
        "audit:EVT-001",
        "company:Harbor Construction LLC",
        "project:Port Expansion",
        "agency:State Auditor",
    ]


def test_litigation_edge_query_returns_only_adjudicated_by_edge() -> None:
    result = query_construction_graph(
        _make_record(event_type="litigation", agency_or_court="Travis County Court"),
        edge_type="ADJUDICATED_BY",
    )

    assert result["edge_matches"] == [
        {
            "edge_id": "project:Port Expansion->ADJUDICATED_BY->court:Travis County Court",
            "from_node": "project:Port Expansion",
            "to_node": "court:Travis County Court",
            "edge_type": "ADJUDICATED_BY",
            "properties": {
                "source_event_id": "EVT-001",
                "source_event_type": "litigation",
            },
        }
    ]


def test_audit_edge_query_returns_only_overseen_by_edge() -> None:
    result = query_construction_graph(
        _make_record(event_type="audit", agency_or_court="State Auditor"),
        edge_type="OVERSEEN_BY",
    )

    assert [edge["edge_id"] for edge in result["edge_matches"]] == [
        "project:Port Expansion->OVERSEEN_BY->agency:State Auditor"
    ]


def test_bond_claim_edge_query_for_overseen_by_returns_empty_matches() -> None:
    result = query_construction_graph(
        _make_record(event_type="bond_claim", agency_or_court="Bond Board"),
        edge_type="OVERSEEN_BY",
    )

    assert result["edge_matches"] == []


def test_edge_type_none_returns_all_edges_in_original_order() -> None:
    result = query_construction_graph(
        _make_record(event_type="litigation", agency_or_court="Travis County Court"),
        edge_type=None,
    )

    assert [edge["edge_id"] for edge in result["edge_matches"]] == [
        "case:EVT-001->SUBJECT_OF_CASE->company:Harbor Construction LLC",
        "case:EVT-001->RELATES_TO_PROJECT->project:Port Expansion",
        "case:EVT-001->ISSUED_BY->court:Travis County Court",
        "project:Port Expansion->ADJUDICATED_BY->court:Travis County Court",
    ]


def test_summary_keys_and_counts_are_exact() -> None:
    result = query_construction_graph(
        _make_record(event_type="audit", agency_or_court="State Auditor"),
        node_type="agency",
        edge_type="OVERSEEN_BY",
    )

    assert result["summary"] == {
        "source_event_id": "EVT-001",
        "source_event_type": "audit",
        "total_nodes": 4,
        "total_edges": 4,
        "matched_nodes": 1,
        "matched_edges": 1,
    }


def test_non_string_node_type_raises_exact_message() -> None:
    with pytest.raises(ValueError, match=r"^node_type must be a string or None\.$"):
        query_construction_graph(_make_record(), node_type=1)


def test_non_string_edge_type_raises_exact_message() -> None:
    with pytest.raises(ValueError, match=r"^edge_type must be a string or None\.$"):
        query_construction_graph(_make_record(), edge_type=1)


def test_malformed_graph_metadata_raises_exact_message(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        graph_query_module,
        "build_construction_graph",
        lambda record: {
            "nodes": [],
            "edges": [],
            "metadata": {
                "source_event_id": "EVT-001",
                "node_count": 0,
                "edge_count": 0,
            },
        },
    )

    with pytest.raises(
        ValueError,
        match="construction graph query could not be completed from the provided record.",
    ):
        query_construction_graph(_make_record())


def test_all_valid_batch_success() -> None:
    result = query_construction_graph_batch(
        [
            _make_record(event_type="litigation", event_id="EVT-001", agency_or_court="Court A"),
            _make_record(event_type="audit", event_id="EVT-002", agency_or_court="Agency A"),
        ],
        node_type="company",
        edge_type="RELATES_TO_PROJECT",
    )

    assert result["ok"] is True
    assert result["total_records"] == 2
    assert result["valid_records"] == 2
    assert result["invalid_records"] == 0
    assert result["results"] == [
        {
            "index": 0,
            "ok": True,
            "matched_nodes": 1,
            "matched_edges": 1,
            "error": None,
        },
        {
            "index": 1,
            "ok": True,
            "matched_nodes": 1,
            "matched_edges": 1,
            "error": None,
        },
    ]
    assert len(result["queries"]) == 2


def test_mixed_batch_indexed_failure_reporting() -> None:
    invalid_record = _make_record(event_type="audit", event_id="EVT-002")
    invalid_record["status"] = "invalid-status"

    result = query_construction_graph_batch(
        [
            _make_record(event_type="litigation", event_id="EVT-001", agency_or_court="Court A"),
            invalid_record,
        ],
        node_type="company",
        edge_type="RELATES_TO_PROJECT",
    )

    assert result["ok"] is False
    assert result["total_records"] == 2
    assert result["valid_records"] == 1
    assert result["invalid_records"] == 1
    assert result["results"][0] == {
        "index": 0,
        "ok": True,
        "matched_nodes": 1,
        "matched_edges": 1,
        "error": None,
    }
    assert result["results"][1] == {
        "index": 1,
        "ok": False,
        "matched_nodes": None,
        "matched_edges": None,
        "error": "status must be one of the supported statuses.",
    }


def test_empty_batch_allowed() -> None:
    result = query_construction_graph_batch([])

    assert result == {
        "ok": True,
        "total_records": 0,
        "valid_records": 0,
        "invalid_records": 0,
        "results": [],
        "queries": [],
    }


def test_non_list_batch_raises() -> None:
    with pytest.raises(ValueError, match=r"^records must be a list\.$"):
        query_construction_graph_batch({})


def test_non_dict_batch_item_raises() -> None:
    with pytest.raises(ValueError, match=r"^records\[1\] must be a dictionary\.$"):
        query_construction_graph_batch([_make_record(), "bad-item"])


def test_query_order_preserved_across_batch() -> None:
    result = query_construction_graph_batch(
        [
            _make_record(event_type="bond_claim", event_id="EVT-010", agency_or_court=None),
            _make_record(event_type="audit", event_id="EVT-020", agency_or_court="Agency B"),
        ],
        node_type="company",
        edge_type="RELATES_TO_PROJECT",
    )

    assert [query["summary"]["source_event_id"] for query in result["queries"]] == [
        "EVT-010",
        "EVT-020",
    ]


def test_input_record_not_mutated() -> None:
    record = _make_record(event_type="audit", agency_or_court="Agency A")
    snapshot = copy.deepcopy(record)

    _ = query_construction_graph(record)

    assert record == snapshot


def test_output_mutation_does_not_mutate_original_input() -> None:
    record = _make_record(event_type="audit", agency_or_court="Agency A")
    result = query_construction_graph(record)

    result["node_matches"][0]["node_id"] = "mutated"
    result["edge_matches"][0]["edge_id"] = "mutated"

    assert record["event_id"] == "EVT-001"
    assert record["company_name"] == "Harbor Construction LLC"


def test_repeated_runs_deterministic() -> None:
    record = _make_record(event_type="litigation", agency_or_court="Court A")

    first = query_construction_graph(record, node_type="company", edge_type="ISSUED_BY")
    second = query_construction_graph(record, node_type="company", edge_type="ISSUED_BY")
    first_batch = query_construction_graph_batch(
        [record],
        node_type="company",
        edge_type="ISSUED_BY",
    )
    second_batch = query_construction_graph_batch(
        [record],
        node_type="company",
        edge_type="ISSUED_BY",
    )

    assert first == second
    assert first_batch == second_batch
