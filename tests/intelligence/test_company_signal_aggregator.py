from __future__ import annotations

import copy
import pathlib
import sys

import pytest


PROJECT_ROOT = pathlib.Path(__file__).resolve().parents[2]
SRC_ROOT = PROJECT_ROOT / "src"

if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))


import fleetgraph_core.intelligence.company_signal_aggregator as aggregator_module
from fleetgraph_core.intelligence.company_signal_aggregator import (
    aggregate_company_signal_batch,
    aggregate_company_signals,
    get_supported_company_signal_types,
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


def test_supported_company_signal_types_returned_correctly() -> None:
    assert get_supported_company_signal_types() == (
        "litigation_risk",
        "audit_risk",
        "enforcement_risk",
        "payment_risk",
    )


def test_litigation_company_gets_litigation_risk() -> None:
    result = aggregate_company_signals(
        _make_record(event_type="litigation", agency_or_court="Travis County Court")
    )
    assert result["companies"][0]["signal_types"] == ["litigation_risk"]


def test_audit_company_gets_audit_risk() -> None:
    result = aggregate_company_signals(
        _make_record(event_type="audit", agency_or_court="State Auditor")
    )
    assert result["companies"][0]["signal_types"] == ["audit_risk"]


def test_enforcement_company_gets_enforcement_risk() -> None:
    result = aggregate_company_signals(
        _make_record(event_type="enforcement", agency_or_court="State Regulator")
    )
    assert result["companies"][0]["signal_types"] == ["enforcement_risk"]


def test_lien_company_gets_payment_risk() -> None:
    result = aggregate_company_signals(
        _make_record(event_type="lien", agency_or_court="County Clerk")
    )
    assert result["companies"][0]["signal_types"] == ["payment_risk"]


def test_bond_claim_company_gets_payment_risk() -> None:
    result = aggregate_company_signals(
        _make_record(event_type="bond_claim", agency_or_court="Bond Board")
    )
    assert result["companies"][0]["signal_types"] == ["payment_risk"]


def test_signal_types_deduped_and_counts_correct() -> None:
    record = _make_record(event_type="audit", agency_or_court="State Auditor")

    original_extract = aggregator_module.extract_construction_signal_batch

    def _fake_extract(records: list[dict[str, object]]) -> dict[str, object]:
        original = original_extract(records)
        return {
            "ok": True,
            "total_records": 1,
            "valid_records": 1,
            "invalid_records": 0,
            "results": [{"index": 0, "ok": True, "signal_count": 2, "error": None}],
            "signals": [
                original["signals"][0],
                copy.deepcopy(original["signals"][0]),
            ],
        }

    aggregator_module.extract_construction_signal_batch = _fake_extract
    try:
        result = aggregate_company_signals(record)
    finally:
        aggregator_module.extract_construction_signal_batch = original_extract

    assert result["companies"] == [
        {
            "company_node_id": "company:Harbor Construction LLC",
            "signal_types": ["audit_risk"],
            "signal_count": 2,
            "related_entities": [
                "project:Port Expansion",
                "agency:State Auditor",
            ],
        }
    ]


def test_related_entities_flattened_and_deduped() -> None:
    original_extract = aggregator_module.extract_construction_signal_batch

    def _fake_extract(records: list[dict[str, object]]) -> dict[str, object]:
        return {
            "ok": True,
            "total_records": 1,
            "valid_records": 1,
            "invalid_records": 0,
            "results": [{"index": 0, "ok": True, "signal_count": 2, "error": None}],
            "signals": [
                {
                    "signal_id": "audit_risk:EVT-001",
                    "signal_type": "audit_risk",
                    "primary_entity": "company:Harbor Construction LLC",
                    "related_entities": [
                        "project:Port Expansion",
                        "agency:State Auditor",
                    ],
                    "source_event_id": "EVT-001",
                    "source_event_type": "audit",
                },
                {
                    "signal_id": "enforcement_risk:EVT-001",
                    "signal_type": "enforcement_risk",
                    "primary_entity": "company:Harbor Construction LLC",
                    "related_entities": [
                        "project:Port Expansion",
                        "agency:State Auditor",
                        "agency:State Regulator",
                    ],
                    "source_event_id": "EVT-001",
                    "source_event_type": "enforcement",
                },
            ],
        }

    aggregator_module.extract_construction_signal_batch = _fake_extract
    try:
        result = aggregate_company_signals(_make_record(event_type="audit"))
    finally:
        aggregator_module.extract_construction_signal_batch = original_extract

    assert result["companies"][0]["related_entities"] == [
        "project:Port Expansion",
        "agency:State Auditor",
        "agency:State Regulator",
    ]


def test_exact_top_level_keys() -> None:
    result = aggregate_company_signals(_make_record())
    assert set(result.keys()) == {
        "source_event_id",
        "companies",
        "company_count",
    }


def test_exact_company_keys() -> None:
    result = aggregate_company_signals(_make_record())
    assert set(result["companies"][0].keys()) == {
        "company_node_id",
        "signal_types",
        "signal_count",
        "related_entities",
    }


def test_company_order_preserved() -> None:
    original_extract = aggregator_module.extract_construction_signal_batch

    def _fake_extract(records: list[dict[str, object]]) -> dict[str, object]:
        return {
            "ok": True,
            "total_records": 1,
            "valid_records": 1,
            "invalid_records": 0,
            "results": [{"index": 0, "ok": True, "signal_count": 2, "error": None}],
            "signals": [
                {
                    "signal_id": "audit_risk:EVT-001",
                    "signal_type": "audit_risk",
                    "primary_entity": "company:First Company",
                    "related_entities": ["project:Port Expansion"],
                    "source_event_id": "EVT-001",
                    "source_event_type": "audit",
                },
                {
                    "signal_id": "litigation_risk:EVT-001",
                    "signal_type": "litigation_risk",
                    "primary_entity": "company:Second Company",
                    "related_entities": ["court:Travis County Court"],
                    "source_event_id": "EVT-001",
                    "source_event_type": "litigation",
                },
            ],
        }

    aggregator_module.extract_construction_signal_batch = _fake_extract
    try:
        result = aggregate_company_signals(_make_record())
    finally:
        aggregator_module.extract_construction_signal_batch = original_extract

    assert [company["company_node_id"] for company in result["companies"]] == [
        "company:First Company",
        "company:Second Company",
    ]


def test_related_entities_order_preserved() -> None:
    result = aggregate_company_signals(
        _make_record(event_type="litigation", agency_or_court="Travis County Court")
    )
    assert result["companies"][0]["related_entities"] == [
        "project:Port Expansion",
        "court:Travis County Court",
    ]


def test_malformed_signal_batch_raises_exact_message(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        aggregator_module,
        "extract_construction_signal_batch",
        lambda records: {"signals": []},
    )

    with pytest.raises(
        ValueError,
        match="company signal aggregation could not be completed from the provided record.",
    ):
        aggregate_company_signals(_make_record())


def test_missing_signal_fields_raise_exact_message(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        aggregator_module,
        "extract_construction_signal_batch",
        lambda records: {
            "ok": True,
            "total_records": 1,
            "valid_records": 1,
            "invalid_records": 0,
            "results": [{"index": 0, "ok": True, "signal_count": 1, "error": None}],
            "signals": [
                {
                    "signal_id": "audit_risk:EVT-001",
                    "signal_type": "audit_risk",
                    "primary_entity": "company:Harbor Construction LLC",
                    "source_event_id": "EVT-001",
                    "source_event_type": "audit",
                }
            ],
        },
    )

    with pytest.raises(
        ValueError,
        match="company signal aggregation could not be completed from the provided record.",
    ):
        aggregate_company_signals(_make_record())


def test_all_valid_batch() -> None:
    result = aggregate_company_signal_batch(
        [
            _make_record(event_type="audit", event_id="EVT-001", agency_or_court="Agency A"),
            _make_record(event_type="litigation", event_id="EVT-002", agency_or_court="Court A"),
        ]
    )

    assert result == {
        "ok": True,
        "total_records": 2,
        "valid_records": 2,
        "invalid_records": 0,
        "results": [
            {"index": 0, "ok": True, "company_count": 1, "error": None},
            {"index": 1, "ok": True, "company_count": 1, "error": None},
        ],
        "companies": [
            {
                "company_node_id": "company:Harbor Construction LLC",
                "signal_types": ["audit_risk"],
                "signal_count": 1,
                "related_entities": [
                    "project:Port Expansion",
                    "agency:Agency A",
                ],
            },
            {
                "company_node_id": "company:Harbor Construction LLC",
                "signal_types": ["litigation_risk"],
                "signal_count": 1,
                "related_entities": [
                    "project:Port Expansion",
                    "court:Court A",
                ],
            },
        ],
    }


def test_mixed_failure_batch() -> None:
    invalid_record = _make_record(event_type="audit", event_id="EVT-002")
    invalid_record["status"] = "invalid-status"

    result = aggregate_company_signal_batch(
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
        "company_count": 1,
        "error": None,
    }
    assert result["results"][1] == {
        "index": 1,
        "ok": False,
        "company_count": None,
        "error": "status must be one of the supported statuses.",
    }


def test_empty_batch() -> None:
    result = aggregate_company_signal_batch([])

    assert result == {
        "ok": True,
        "total_records": 0,
        "valid_records": 0,
        "invalid_records": 0,
        "results": [],
        "companies": [],
    }


def test_non_list_input() -> None:
    with pytest.raises(ValueError, match=r"^records must be a list\.$"):
        aggregate_company_signal_batch({})


def test_non_dict_item() -> None:
    with pytest.raises(ValueError, match=r"^records\[1\] must be a dictionary\.$"):
        aggregate_company_signal_batch([_make_record(), "bad-item"])


def test_input_not_mutated() -> None:
    record = _make_record(event_type="audit", agency_or_court="Agency A")
    snapshot = copy.deepcopy(record)

    _ = aggregate_company_signals(record)

    assert record == snapshot


def test_output_mutation_safe() -> None:
    record = _make_record(event_type="audit", agency_or_court="Agency A")
    result = aggregate_company_signals(record)

    result["companies"][0]["company_node_id"] = "mutated"
    result["companies"][0]["signal_types"].append("mutated")
    result["companies"][0]["related_entities"].append("mutated")

    assert record["event_id"] == "EVT-001"
    assert record["company_name"] == "Harbor Construction LLC"


def test_deterministic_runs() -> None:
    record = _make_record(event_type="litigation", agency_or_court="Court A")

    first = aggregate_company_signals(record)
    second = aggregate_company_signals(record)
    first_batch = aggregate_company_signal_batch([record])
    second_batch = aggregate_company_signal_batch([record])

    assert first == second
    assert first_batch == second_batch
