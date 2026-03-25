import copy
import pathlib
import sys

import pytest


PROJECT_ROOT = pathlib.Path(__file__).resolve().parents[2]
SRC_ROOT = PROJECT_ROOT / "src"

if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from fleetgraph_core.graph_construction.supporting_entity_relationship_builder import (
    build_supporting_entity_relationship_edge_batch,
    build_supporting_entity_relationship_edges,
    get_supported_supporting_entity_relationship_types,
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
    event_id: str = "evt-1",
    company_name: str = "Acme Builders",
    project_name: str | None = "Port Expansion",
    agency_or_court: str | None = "Superior Court",
) -> dict[str, object]:
    return build_unified_event_record(
        {
            "event_id": event_id,
            "event_type": event_type,
            "company_name": company_name,
            "source_name": "Daily Ledger",
            "status": "open",
            "event_date": "2026-03-01",
            "jurisdiction": "CA",
            "project_name": project_name,
            "agency_or_court": agency_or_court,
            "severity": "medium",
            "amount": 1000,
            "currency": "USD",
            "service_fit": ["legal_monitoring"],
            "trigger_tags": ["construction"],
            "evidence": {
                "summary": "summary",
                "source_record_id": "src-1",
            },
            "event_details": _event_details_for(event_type),
        }
    )


def test_supported_relationship_edge_types_returned_correctly():
    assert get_supported_supporting_entity_relationship_types() == (
        "OVERSEEN_BY",
        "ADJUDICATED_BY",
    )


def test_litigation_with_project_and_court_produces_adjudicated_by():
    result = build_supporting_entity_relationship_edges(
        _make_record(event_type="litigation", agency_or_court="Superior Court")
    )

    assert result == [
        {
            "edge_id": "project:Port Expansion->ADJUDICATED_BY->court:Superior Court",
            "from_node": "project:Port Expansion",
            "to_node": "court:Superior Court",
            "edge_type": "ADJUDICATED_BY",
            "properties": {
                "source_event_id": "evt-1",
                "source_event_type": "litigation",
            },
        }
    ]


def test_audit_with_project_and_agency_produces_overseen_by():
    result = build_supporting_entity_relationship_edges(
        _make_record(event_type="audit", agency_or_court="State Auditor")
    )

    assert result == [
        {
            "edge_id": "project:Port Expansion->OVERSEEN_BY->agency:State Auditor",
            "from_node": "project:Port Expansion",
            "to_node": "agency:State Auditor",
            "edge_type": "OVERSEEN_BY",
            "properties": {
                "source_event_id": "evt-1",
                "source_event_type": "audit",
            },
        }
    ]


def test_enforcement_with_project_and_agency_produces_overseen_by():
    result = build_supporting_entity_relationship_edges(
        _make_record(event_type="enforcement", agency_or_court="State Regulator")
    )

    assert result == [
        {
            "edge_id": "project:Port Expansion->OVERSEEN_BY->agency:State Regulator",
            "from_node": "project:Port Expansion",
            "to_node": "agency:State Regulator",
            "edge_type": "OVERSEEN_BY",
            "properties": {
                "source_event_id": "evt-1",
                "source_event_type": "enforcement",
            },
        }
    ]


def test_lien_record_produces_no_supporting_entity_relationship_edges():
    assert (
        build_supporting_entity_relationship_edges(
            _make_record(event_type="lien", agency_or_court="County Clerk")
        )
        == []
    )


def test_bond_claim_record_produces_no_supporting_entity_relationship_edges():
    assert (
        build_supporting_entity_relationship_edges(
            _make_record(event_type="bond_claim", agency_or_court="Bond Board")
        )
        == []
    )


def test_no_project_edge_when_project_absent():
    assert (
        build_supporting_entity_relationship_edges(
            _make_record(event_type="audit", project_name=None, agency_or_court="Inspector")
        )
        == []
    )


def test_no_project_to_agency_when_agency_absent():
    assert (
        build_supporting_entity_relationship_edges(
            _make_record(event_type="audit", agency_or_court=None)
        )
        == []
    )


def test_no_project_to_court_when_court_absent():
    assert (
        build_supporting_entity_relationship_edges(
            _make_record(event_type="litigation", agency_or_court=None)
        )
        == []
    )


def test_exact_top_level_keys():
    result = build_supporting_entity_relationship_edges(
        _make_record(event_type="audit", agency_or_court="State Auditor")
    )[0]

    assert set(result.keys()) == {
        "edge_id",
        "from_node",
        "to_node",
        "edge_type",
        "properties",
    }


def test_exact_edge_id_format():
    result = build_supporting_entity_relationship_edges(
        _make_record(event_type="audit", agency_or_court="State Auditor")
    )[0]

    assert result["edge_id"] == "project:Port Expansion->OVERSEEN_BY->agency:State Auditor"


def test_exact_properties_shape():
    result = build_supporting_entity_relationship_edges(
        _make_record(event_type="litigation", agency_or_court="Superior Court")
    )[0]

    assert result["properties"] == {
        "source_event_id": "evt-1",
        "source_event_type": "litigation",
    }


def test_per_record_edge_order_preserved():
    record = _make_record(event_type="audit", agency_or_court="State Auditor")
    result = build_supporting_entity_relationship_edges(record)

    assert [edge["edge_type"] for edge in result] == ["OVERSEEN_BY"]


def test_all_valid_batch_success():
    result = build_supporting_entity_relationship_edge_batch(
        [
            _make_record(event_type="audit", event_id="evt-1", agency_or_court="Agency A"),
            _make_record(event_type="litigation", event_id="evt-2", agency_or_court="Court A"),
        ]
    )

    assert result["ok"] is True
    assert result["total_records"] == 2
    assert result["valid_records"] == 2
    assert result["invalid_records"] == 0
    assert result["results"] == [
        {"index": 0, "ok": True, "edge_count": 1, "error": None},
        {"index": 1, "ok": True, "edge_count": 1, "error": None},
    ]
    assert [edge["edge_type"] for edge in result["edges"]] == [
        "OVERSEEN_BY",
        "ADJUDICATED_BY",
    ]


def test_mixed_batch_indexed_failure_reporting():
    invalid_record = _make_record(event_type="audit", event_id="evt-2", agency_or_court="Agency A")
    invalid_record["status"] = "invalid-status"

    result = build_supporting_entity_relationship_edge_batch(
        [
            _make_record(event_type="litigation", event_id="evt-1", agency_or_court="Court A"),
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
        "edge_count": 1,
        "error": None,
    }
    assert result["results"][1]["index"] == 1
    assert result["results"][1]["ok"] is False
    assert result["results"][1]["edge_count"] is None
    assert result["results"][1]["error"] == "status must be one of the supported statuses."


def test_empty_batch_allowed():
    result = build_supporting_entity_relationship_edge_batch([])

    assert result == {
        "ok": True,
        "total_records": 0,
        "valid_records": 0,
        "invalid_records": 0,
        "results": [],
        "edges": [],
    }


def test_non_list_batch_raises():
    with pytest.raises(ValueError, match=r"^records must be a list\.$"):
        build_supporting_entity_relationship_edge_batch({})


def test_non_dict_batch_item_raises():
    with pytest.raises(ValueError, match=r"^records\[1\] must be a dictionary\.$"):
        build_supporting_entity_relationship_edge_batch([_make_record(), "bad-item"])


def test_aggregate_edges_order_preserved_across_records():
    result = build_supporting_entity_relationship_edge_batch(
        [
            _make_record(event_type="audit", event_id="evt-1", agency_or_court="Agency A"),
            _make_record(event_type="litigation", event_id="evt-2", agency_or_court="Court A"),
            _make_record(event_type="lien", event_id="evt-3", agency_or_court="County Clerk"),
        ]
    )

    assert [edge["edge_id"] for edge in result["edges"]] == [
        "project:Port Expansion->OVERSEEN_BY->agency:Agency A",
        "project:Port Expansion->ADJUDICATED_BY->court:Court A",
    ]


def test_input_record_not_mutated():
    record = _make_record(event_type="audit", agency_or_court="Agency A")
    snapshot = copy.deepcopy(record)

    _ = build_supporting_entity_relationship_edges(record)

    assert record == snapshot


def test_output_edge_mutation_does_not_mutate_original_input():
    record = _make_record(event_type="audit", agency_or_court="Agency A")
    result = build_supporting_entity_relationship_edges(record)

    result[0]["properties"]["source_event_id"] = "mutated"
    result[0]["from_node"] = "mutated"

    assert record["event_id"] == "evt-1"
    assert record["project_name"] == "Port Expansion"


def test_repeated_runs_deterministic():
    record = _make_record(event_type="litigation", agency_or_court="Superior Court")

    first = build_supporting_entity_relationship_edges(record)
    second = build_supporting_entity_relationship_edges(record)

    assert first == second
