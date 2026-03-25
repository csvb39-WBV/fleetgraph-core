from __future__ import annotations

import copy
import pathlib
import sys

import pytest


PROJECT_ROOT = pathlib.Path(__file__).resolve().parents[2]
SRC_ROOT = PROJECT_ROOT / "src"

if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))


from fleetgraph_core.graph_construction.event_relationship_builder import (
    build_event_relationship_edge_batch,
    build_event_relationship_edges,
    get_supported_event_relationship_types,
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


def test_get_supported_event_relationship_types_returns_required_tuple() -> None:
    assert get_supported_event_relationship_types() == (
        "SUBJECT_OF_CASE",
        "SUBJECT_OF_AUDIT",
        "SUBJECT_OF_ENFORCEMENT",
        "RELATES_TO_PROJECT",
        "ISSUED_BY",
    )


def test_litigation_record_produces_company_project_and_court_edges() -> None:
    result = build_event_relationship_edges(
        _make_record(event_type="litigation", agency_or_court="Travis County Court")
    )

    assert [edge["edge_type"] for edge in result] == [
        "SUBJECT_OF_CASE",
        "RELATES_TO_PROJECT",
        "ISSUED_BY",
    ]
    assert result[0]["to_node"] == "company:Harbor Construction LLC"
    assert result[1]["to_node"] == "project:Port Expansion"
    assert result[2]["to_node"] == "court:Travis County Court"


def test_audit_record_produces_company_project_and_agency_edges() -> None:
    result = build_event_relationship_edges(
        _make_record(event_type="audit", agency_or_court="State Auditor")
    )

    assert [edge["edge_type"] for edge in result] == [
        "SUBJECT_OF_AUDIT",
        "RELATES_TO_PROJECT",
        "ISSUED_BY",
    ]
    assert result[2]["to_node"] == "agency:State Auditor"


def test_enforcement_record_produces_company_project_and_agency_edges() -> None:
    result = build_event_relationship_edges(
        _make_record(event_type="enforcement", agency_or_court="Labor Authority")
    )

    assert [edge["edge_type"] for edge in result] == [
        "SUBJECT_OF_ENFORCEMENT",
        "RELATES_TO_PROJECT",
        "ISSUED_BY",
    ]
    assert result[2]["to_node"] == "agency:Labor Authority"


def test_lien_record_produces_company_and_project_edges_only() -> None:
    result = build_event_relationship_edges(
        _make_record(event_type="lien", agency_or_court="County Clerk")
    )

    assert [edge["edge_type"] for edge in result] == [
        "SUBJECT_OF_CASE",
        "RELATES_TO_PROJECT",
    ]


def test_bond_claim_record_produces_company_and_project_edges_only() -> None:
    result = build_event_relationship_edges(
        _make_record(event_type="bond_claim", agency_or_court="Bond Board")
    )

    assert [edge["edge_type"] for edge in result] == [
        "SUBJECT_OF_CASE",
        "RELATES_TO_PROJECT",
    ]


def test_no_project_edge_when_project_node_absent() -> None:
    result = build_event_relationship_edges(
        _make_record(event_type="audit", project_name=None, agency_or_court="State Auditor")
    )

    assert [edge["edge_type"] for edge in result] == [
        "SUBJECT_OF_AUDIT",
        "ISSUED_BY",
    ]


def test_no_agency_or_court_edge_when_supporting_node_absent() -> None:
    result = build_event_relationship_edges(
        _make_record(event_type="litigation", agency_or_court=None)
    )

    assert [edge["edge_type"] for edge in result] == [
        "SUBJECT_OF_CASE",
        "RELATES_TO_PROJECT",
    ]


def test_edge_shape_is_exact() -> None:
    result = build_event_relationship_edges(_make_record(event_type="litigation"))

    assert result[0] == {
        "edge_id": "case:EVT-001->SUBJECT_OF_CASE->company:Harbor Construction LLC",
        "from_node": "case:EVT-001",
        "to_node": "company:Harbor Construction LLC",
        "edge_type": "SUBJECT_OF_CASE",
        "properties": {
            "source_event_id": "EVT-001",
            "source_event_type": "litigation",
        },
    }


def test_edge_order_is_preserved_per_record() -> None:
    result = build_event_relationship_edges(
        _make_record(event_type="audit", agency_or_court="State Auditor")
    )

    assert [edge["to_node"] for edge in result] == [
        "company:Harbor Construction LLC",
        "project:Port Expansion",
        "agency:State Auditor",
    ]


def test_all_valid_batch_success() -> None:
    result = build_event_relationship_edge_batch(
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
        {"index": 0, "ok": True, "edge_count": 3, "error": None},
        {"index": 1, "ok": True, "edge_count": 3, "error": None},
    ]
    assert len(result["edges"]) == 6


def test_mixed_batch_indexed_failure_reporting() -> None:
    invalid_record = _make_record(event_type="audit", event_id="EVT-002")
    invalid_record["status"] = "not-valid"

    result = build_event_relationship_edge_batch(
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
        "edge_count": 3,
        "error": None,
    }
    assert result["results"][1]["index"] == 1
    assert result["results"][1]["ok"] is False
    assert result["results"][1]["edge_count"] is None
    assert result["results"][1]["error"] == "status must be one of the supported statuses."


def test_empty_batch_allowed() -> None:
    result = build_event_relationship_edge_batch([])

    assert result == {
        "ok": True,
        "total_records": 0,
        "valid_records": 0,
        "invalid_records": 0,
        "results": [],
        "edges": [],
    }


def test_non_list_batch_raises_exact_message() -> None:
    with pytest.raises(ValueError, match=r"^records must be a list\.$"):
        build_event_relationship_edge_batch({})


def test_non_dict_item_raises_exact_message() -> None:
    with pytest.raises(ValueError, match=r"^records\[1\] must be a dictionary\.$"):
        build_event_relationship_edge_batch([_make_record(), "bad-item"])


def test_aggregate_edge_order_preserved_across_records() -> None:
    result = build_event_relationship_edge_batch(
        [
            _make_record(event_type="bond_claim", event_id="EVT-010", agency_or_court=None),
            _make_record(event_type="audit", event_id="EVT-020", agency_or_court="Agency B"),
        ]
    )

    assert [edge["edge_id"] for edge in result["edges"]] == [
        "bond_claim:EVT-010->SUBJECT_OF_CASE->company:Harbor Construction LLC",
        "bond_claim:EVT-010->RELATES_TO_PROJECT->project:Port Expansion",
        "audit:EVT-020->SUBJECT_OF_AUDIT->company:Harbor Construction LLC",
        "audit:EVT-020->RELATES_TO_PROJECT->project:Port Expansion",
        "audit:EVT-020->ISSUED_BY->agency:Agency B",
    ]


def test_input_record_not_mutated() -> None:
    record = _make_record(event_type="audit", agency_or_court="Agency A")
    snapshot = copy.deepcopy(record)

    _ = build_event_relationship_edges(record)

    assert record == snapshot


def test_output_edge_mutation_does_not_mutate_original_input() -> None:
    record = _make_record(event_type="audit", agency_or_court="Agency A")
    result = build_event_relationship_edges(record)

    result[0]["properties"]["source_event_id"] = "MUTATED"
    result[0]["to_node"] = "company:Mutated"

    assert record["event_id"] == "EVT-001"
    assert record["company_name"] == "Harbor Construction LLC"


def test_repeated_runs_are_deterministic() -> None:
    record = _make_record(event_type="litigation", agency_or_court="Court A")

    first = build_event_relationship_edges(record)
    second = build_event_relationship_edges(record)
    first_batch = build_event_relationship_edge_batch([record])
    second_batch = build_event_relationship_edge_batch([record])

    assert first == second
    assert first_batch == second_batch
