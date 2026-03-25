from __future__ import annotations

import copy
import pathlib
import sys

import pytest


PROJECT_ROOT = pathlib.Path(__file__).resolve().parents[2]
SRC_ROOT = PROJECT_ROOT / "src"

if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))


import fleetgraph_core.intelligence.construction_signal_extractor as extractor_module
from fleetgraph_core.intelligence.construction_signal_extractor import (
    extract_construction_signal_batch,
    extract_construction_signals,
    get_supported_construction_signal_types,
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


def test_supported_signal_types_returned_correctly() -> None:
    assert get_supported_construction_signal_types() == (
        "litigation_risk",
        "audit_risk",
        "enforcement_risk",
        "payment_risk",
    )


def test_litigation_record_maps_to_litigation_risk() -> None:
    result = extract_construction_signals(
        _make_record(event_type="litigation", agency_or_court="Travis County Court")
    )
    assert result["signals"][0]["signal_type"] == "litigation_risk"


def test_audit_record_maps_to_audit_risk() -> None:
    result = extract_construction_signals(
        _make_record(event_type="audit", agency_or_court="State Auditor")
    )
    assert result["signals"][0]["signal_type"] == "audit_risk"


def test_enforcement_record_maps_to_enforcement_risk() -> None:
    result = extract_construction_signals(
        _make_record(event_type="enforcement", agency_or_court="State Regulator")
    )
    assert result["signals"][0]["signal_type"] == "enforcement_risk"


def test_lien_record_maps_to_payment_risk() -> None:
    result = extract_construction_signals(
        _make_record(event_type="lien", agency_or_court="County Clerk")
    )
    assert result["signals"][0]["signal_type"] == "payment_risk"


def test_bond_claim_record_maps_to_payment_risk() -> None:
    result = extract_construction_signals(
        _make_record(event_type="bond_claim", agency_or_court="Bond Board")
    )
    assert result["signals"][0]["signal_type"] == "payment_risk"


def test_primary_entity_is_always_company_node_id() -> None:
    result = extract_construction_signals(
        _make_record(event_type="audit", agency_or_court="State Auditor")
    )
    assert result["signals"][0]["primary_entity"] == "company:Harbor Construction LLC"


def test_litigation_related_entities_include_project_and_court() -> None:
    result = extract_construction_signals(
        _make_record(event_type="litigation", agency_or_court="Travis County Court")
    )
    assert result["signals"][0]["related_entities"] == [
        "project:Port Expansion",
        "court:Travis County Court",
    ]


def test_audit_related_entities_include_project_and_agency() -> None:
    result = extract_construction_signals(
        _make_record(event_type="audit", agency_or_court="State Auditor")
    )
    assert result["signals"][0]["related_entities"] == [
        "project:Port Expansion",
        "agency:State Auditor",
    ]


def test_enforcement_related_entities_include_project_and_agency() -> None:
    result = extract_construction_signals(
        _make_record(event_type="enforcement", agency_or_court="State Regulator")
    )
    assert result["signals"][0]["related_entities"] == [
        "project:Port Expansion",
        "agency:State Regulator",
    ]


def test_payment_risk_related_entities_include_project_only() -> None:
    result = extract_construction_signals(
        _make_record(event_type="bond_claim", agency_or_court="Bond Board")
    )
    assert result["signals"][0]["related_entities"] == ["project:Port Expansion"]


def test_exact_top_level_keys() -> None:
    result = extract_construction_signals(_make_record())
    assert set(result.keys()) == {
        "source_event_id",
        "source_event_type",
        "signals",
        "signal_count",
    }


def test_exact_signal_keys() -> None:
    result = extract_construction_signals(_make_record())
    assert set(result["signals"][0].keys()) == {
        "signal_id",
        "signal_type",
        "primary_entity",
        "related_entities",
        "source_event_id",
        "source_event_type",
    }


def test_exact_signal_id_format() -> None:
    result = extract_construction_signals(
        _make_record(event_type="litigation", event_id="EVT-001")
    )
    assert result["signals"][0]["signal_id"] == "litigation_risk:EVT-001"


def test_deterministic_signal_ordering() -> None:
    result = extract_construction_signals(
        _make_record(event_type="audit", agency_or_court="State Auditor")
    )
    assert len(result["signals"]) == 1
    assert result["signal_count"] == 1


def test_deterministic_related_entity_ordering() -> None:
    result = extract_construction_signals(
        _make_record(event_type="audit", agency_or_court="State Auditor")
    )
    assert result["signals"][0]["related_entities"] == [
        "project:Port Expansion",
        "agency:State Auditor",
    ]


def test_malformed_graph_raises_exact_message(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        extractor_module,
        "build_construction_graph",
        lambda record: {
            "nodes": "bad",
            "edges": [],
            "metadata": {
                "source_event_id": "EVT-001",
                "source_event_type": "audit",
                "node_count": 0,
                "edge_count": 0,
            },
        },
    )

    with pytest.raises(
        ValueError,
        match="construction signals could not be extracted from the provided record.",
    ):
        extract_construction_signals(_make_record())


def test_malformed_identities_raise_exact_message(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        extractor_module,
        "resolve_construction_graph_identities",
        lambda record: {
            "source_event_id": "EVT-001",
            "source_event_type": "audit",
            "identities": "bad",
            "identity_count": 0,
        },
    )

    with pytest.raises(
        ValueError,
        match="construction signals could not be extracted from the provided record.",
    ):
        extract_construction_signals(_make_record())


def test_missing_company_node_raises_exact_message(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        extractor_module,
        "build_construction_graph",
        lambda record: {
            "nodes": [
                {
                    "node_id": "audit:EVT-001",
                    "node_type": "audit",
                    "label": "Audit",
                    "properties": {},
                }
            ],
            "edges": [],
            "metadata": {
                "source_event_id": "EVT-001",
                "source_event_type": "audit",
                "node_count": 1,
                "edge_count": 0,
            },
        },
    )
    monkeypatch.setattr(
        extractor_module,
        "resolve_construction_graph_identities",
        lambda record: {
            "source_event_id": "EVT-001",
            "source_event_type": "audit",
            "identities": [
                {
                    "identity_type": "event",
                    "node_id": "audit:EVT-001",
                    "canonical_name": "Audit",
                    "source_event_id": "EVT-001",
                    "source_event_type": "audit",
                }
            ],
            "identity_count": 1,
        },
    )

    with pytest.raises(
        ValueError,
        match="construction signals could not be extracted from the provided record.",
    ):
        extract_construction_signals(_make_record(event_type="audit"))


def test_unsupported_event_type_raises_exact_message(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        extractor_module,
        "build_construction_graph",
        lambda record: {
            "nodes": [
                {
                    "node_id": "company:Harbor Construction LLC",
                    "node_type": "company",
                    "label": "Harbor Construction LLC",
                    "properties": {},
                }
            ],
            "edges": [],
            "metadata": {
                "source_event_id": "EVT-001",
                "source_event_type": "mystery",
                "node_count": 1,
                "edge_count": 0,
            },
        },
    )
    monkeypatch.setattr(
        extractor_module,
        "resolve_construction_graph_identities",
        lambda record: {
            "source_event_id": "EVT-001",
            "source_event_type": "mystery",
            "identities": [
                {
                    "identity_type": "company",
                    "node_id": "company:Harbor Construction LLC",
                    "canonical_name": "Harbor Construction LLC",
                    "source_event_id": "EVT-001",
                    "source_event_type": "mystery",
                }
            ],
            "identity_count": 1,
        },
    )

    with pytest.raises(
        ValueError,
        match="construction signals could not be extracted from the provided record.",
    ):
        extract_construction_signals(_make_record())


def test_all_valid_batch_success() -> None:
    result = extract_construction_signal_batch(
        [
            _make_record(event_type="audit", event_id="EVT-001", agency_or_court="Agency A"),
            _make_record(event_type="litigation", event_id="EVT-002", agency_or_court="Court A"),
        ]
    )

    assert result["ok"] is True
    assert result["total_records"] == 2
    assert result["valid_records"] == 2
    assert result["invalid_records"] == 0
    assert result["results"] == [
        {"index": 0, "ok": True, "signal_count": 1, "error": None},
        {"index": 1, "ok": True, "signal_count": 1, "error": None},
    ]
    assert [signal["signal_id"] for signal in result["signals"]] == [
        "audit_risk:EVT-001",
        "litigation_risk:EVT-002",
    ]


def test_mixed_failure_batch_with_indexed_reporting() -> None:
    invalid_record = _make_record(event_type="audit", event_id="EVT-002")
    invalid_record["status"] = "invalid-status"

    result = extract_construction_signal_batch(
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
        "signal_count": 1,
        "error": None,
    }
    assert result["results"][1] == {
        "index": 1,
        "ok": False,
        "signal_count": None,
        "error": "status must be one of the supported statuses.",
    }


def test_empty_batch_allowed() -> None:
    result = extract_construction_signal_batch([])

    assert result == {
        "ok": True,
        "total_records": 0,
        "valid_records": 0,
        "invalid_records": 0,
        "results": [],
        "signals": [],
    }


def test_non_list_input_raises() -> None:
    with pytest.raises(ValueError, match=r"^records must be a list\.$"):
        extract_construction_signal_batch({})


def test_non_dict_item_raises() -> None:
    with pytest.raises(ValueError, match=r"^records\[1\] must be a dictionary\.$"):
        extract_construction_signal_batch([_make_record(), "bad-item"])


def test_flattened_signal_order_preserved_across_records() -> None:
    result = extract_construction_signal_batch(
        [
            _make_record(event_type="bond_claim", event_id="EVT-010", agency_or_court=None),
            _make_record(event_type="audit", event_id="EVT-020", agency_or_court="Agency B"),
        ]
    )

    assert [signal["signal_id"] for signal in result["signals"]] == [
        "payment_risk:EVT-010",
        "audit_risk:EVT-020",
    ]


def test_input_record_not_mutated() -> None:
    record = _make_record(event_type="audit", agency_or_court="Agency A")
    snapshot = copy.deepcopy(record)

    _ = extract_construction_signals(record)

    assert record == snapshot


def test_output_mutation_does_not_mutate_original_input() -> None:
    record = _make_record(event_type="audit", agency_or_court="Agency A")
    result = extract_construction_signals(record)

    result["signals"][0]["primary_entity"] = "mutated"
    result["signals"][0]["related_entities"].append("mutated")

    assert record["event_id"] == "EVT-001"
    assert record["company_name"] == "Harbor Construction LLC"


def test_repeated_runs_deterministic() -> None:
    record = _make_record(event_type="litigation", agency_or_court="Court A")

    first = extract_construction_signals(record)
    second = extract_construction_signals(record)
    first_batch = extract_construction_signal_batch([record])
    second_batch = extract_construction_signal_batch([record])

    assert first == second
    assert first_batch == second_batch
