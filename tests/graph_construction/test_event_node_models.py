from __future__ import annotations

import copy
import pathlib
import sys

import pytest


PROJECT_ROOT = pathlib.Path(__file__).resolve().parents[2]
SRC_ROOT = PROJECT_ROOT / "src"

if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))


from fleetgraph_core.graph_construction import event_node_models
from fleetgraph_core.graph_construction.event_node_models import (
    build_event_node,
    build_event_node_batch,
    get_supported_event_node_types,
)
from fleetgraph_core.intelligence.unified_event_schema import build_unified_event_record


def _base_record(event_type: str, event_details: dict[str, object]) -> dict[str, object]:
    return {
        "event_id": "EVT-001",
        "event_type": event_type,
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
        "evidence": {
            "summary": "Documented filing in district court",
            "source_record_id": "SRC-101",
        },
        "event_details": event_details,
    }


def _litigation_record() -> dict[str, object]:
    return _base_record(
        "litigation",
        {
            "case_id": "CASE-1",
            "case_type": "Breach of contract",
            "filing_date": "2026-03-01",
            "plaintiff_role": "plaintiff",
            "defendant_role": "defendant",
        },
    )


def _audit_record() -> dict[str, object]:
    return _base_record(
        "audit",
        {
            "audit_id": "AUD-1",
            "issue_type": "safety",
            "opened_date": "2026-02-01",
            "agency": "State Auditor",
        },
    )


def _enforcement_record() -> dict[str, object]:
    return _base_record(
        "enforcement",
        {
            "action_id": "ACT-1",
            "issue_type": "labor",
            "opened_date": "2026-01-15",
            "agency": "Labor Authority",
        },
    )


def _lien_record() -> dict[str, object]:
    return _base_record(
        "lien",
        {
            "lien_id": "LIEN-1",
            "filing_date": "2026-01-20",
            "claimant_role": "supplier",
        },
    )


def _bond_claim_record() -> dict[str, object]:
    return _base_record(
        "bond_claim",
        {
            "bond_claim_id": "BOND-1",
            "filing_date": "2026-01-22",
            "claimant_role": "subcontractor",
        },
    )


def test_get_supported_event_node_types_returns_required_tuple() -> None:
    assert get_supported_event_node_types() == (
        "case",
        "audit",
        "enforcement_action",
        "lien",
        "bond_claim",
    )


@pytest.mark.parametrize(
    ("record_factory", "expected_node_type"),
    [
        (_litigation_record, "case"),
        (_audit_record, "audit"),
        (_enforcement_record, "enforcement_action"),
        (_lien_record, "lien"),
        (_bond_claim_record, "bond_claim"),
    ],
)
def test_build_event_node_maps_event_types(record_factory, expected_node_type: str) -> None:
    result = build_event_node(build_unified_event_record(record_factory()))
    assert result["node_type"] == expected_node_type


def test_build_event_node_returns_exact_top_level_shape() -> None:
    result = build_event_node(build_unified_event_record(_litigation_record()))
    assert set(result.keys()) == {"node_id", "node_type", "label", "properties"}


def test_build_event_node_returns_exact_properties_shape() -> None:
    result = build_event_node(build_unified_event_record(_litigation_record()))
    assert set(result["properties"].keys()) == {
        "event_id",
        "event_type",
        "company_name",
        "source_name",
        "status",
        "event_date",
        "jurisdiction",
        "project_name",
        "agency_or_court",
        "severity",
        "amount",
        "currency",
        "service_fit",
        "trigger_tags",
        "evidence_summary",
        "source_record_id",
        "event_details",
    }


def test_build_event_node_uses_exact_node_id_and_label_formats() -> None:
    result = build_event_node(build_unified_event_record(_litigation_record()))
    assert result["node_id"] == "case:EVT-001"
    assert result["label"] == "Harbor Construction LLC case"


def test_build_event_node_flattens_evidence_and_omits_nested_evidence() -> None:
    result = build_event_node(build_unified_event_record(_audit_record()))
    assert result["properties"]["evidence_summary"] == "Documented filing in district court"
    assert result["properties"]["source_record_id"] == "SRC-101"
    assert "evidence" not in result["properties"]


def test_build_event_node_preserves_event_details_as_dict() -> None:
    result = build_event_node(build_unified_event_record(_enforcement_record()))
    assert isinstance(result["properties"]["event_details"], dict)
    assert result["properties"]["event_details"]["action_id"] == "ACT-1"


def test_build_event_node_rejects_unmappable_event_type_with_exact_message(monkeypatch) -> None:
    monkeypatch.delitem(event_node_models._EVENT_TYPE_TO_NODE_TYPE, "litigation", raising=False)

    with pytest.raises(
        ValueError,
        match="event_type cannot be mapped to a supported event node type.",
    ):
        build_event_node(build_unified_event_record(_litigation_record()))


def test_build_event_node_batch_all_valid_success() -> None:
    records = [
        build_unified_event_record(_litigation_record()),
        build_unified_event_record(_audit_record()),
    ]

    result = build_event_node_batch(records)

    assert result["ok"] is True
    assert result["total_records"] == 2
    assert result["valid_records"] == 2
    assert result["invalid_records"] == 0
    assert len(result["results"]) == 2
    assert len(result["nodes"]) == 2


def test_build_event_node_batch_mixed_batch_preserves_indexed_failures_and_errors() -> None:
    invalid_record = build_unified_event_record(_audit_record())
    del invalid_record["event_id"]

    result = build_event_node_batch(
        [
            build_unified_event_record(_litigation_record()),
            invalid_record,
            build_unified_event_record(_bond_claim_record()),
        ]
    )

    assert result["ok"] is False
    assert result["total_records"] == 3
    assert result["valid_records"] == 2
    assert result["invalid_records"] == 1
    assert result["results"] == [
        {
            "index": 0,
            "ok": True,
            "node_id": "case:EVT-001",
            "node_type": "case",
            "error": None,
        },
        {
            "index": 1,
            "ok": False,
            "node_id": None,
            "node_type": None,
            "error": "record must contain exactly the canonical top-level keys.",
        },
        {
            "index": 2,
            "ok": True,
            "node_id": "bond_claim:EVT-001",
            "node_type": "bond_claim",
            "error": None,
        },
    ]


def test_build_event_node_batch_allows_empty_batch() -> None:
    result = build_event_node_batch([])
    assert result == {
        "ok": True,
        "total_records": 0,
        "valid_records": 0,
        "invalid_records": 0,
        "results": [],
        "nodes": [],
    }


def test_build_event_node_batch_rejects_non_list_with_exact_message() -> None:
    with pytest.raises(ValueError, match=r"records must be a list\."):
        build_event_node_batch({})  # type: ignore[arg-type]


def test_build_event_node_batch_rejects_non_dict_item_with_exact_message() -> None:
    with pytest.raises(ValueError, match=r"records\[1\] must be a dictionary\."):
        build_event_node_batch([build_unified_event_record(_lien_record()), "bad"])  # type: ignore[list-item]


def test_build_event_node_batch_preserves_successful_node_order() -> None:
    records = [
        build_unified_event_record(_bond_claim_record()),
        build_unified_event_record(_litigation_record()),
        build_unified_event_record(_audit_record()),
    ]

    result = build_event_node_batch(records)

    assert [node["node_type"] for node in result["nodes"]] == [
        "bond_claim",
        "case",
        "audit",
    ]


def test_build_event_node_does_not_mutate_input() -> None:
    record = build_unified_event_record(_litigation_record())
    snapshot = copy.deepcopy(record)

    _ = build_event_node(record)

    assert record == snapshot


def test_output_mutation_does_not_mutate_original_input() -> None:
    record = build_unified_event_record(_litigation_record())
    result = build_event_node(record)

    result["properties"]["service_fit"].append("mutated")
    result["properties"]["trigger_tags"].append("mutated")
    result["properties"]["event_details"]["case_id"] = "CHANGED"

    assert record["service_fit"] == ["litigation_response"]
    assert record["trigger_tags"] == ["high_risk"]
    assert record["event_details"]["case_id"] == "CASE-1"


def test_build_event_node_and_batch_are_deterministic() -> None:
    record = build_unified_event_record(_enforcement_record())

    first_node = build_event_node(record)
    second_node = build_event_node(record)
    first_batch = build_event_node_batch([record])
    second_batch = build_event_node_batch([record])

    assert first_node == second_node
    assert first_batch == second_batch
