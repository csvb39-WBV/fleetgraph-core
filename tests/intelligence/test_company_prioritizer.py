from __future__ import annotations

import copy
import pathlib
import sys

import pytest


PROJECT_ROOT = pathlib.Path(__file__).resolve().parents[2]
SRC_ROOT = PROJECT_ROOT / "src"

if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))


import fleetgraph_core.intelligence.company_prioritizer as prioritizer_module
from fleetgraph_core.intelligence.company_prioritizer import (
    get_supported_priority_levels,
    prioritize_companies,
    prioritize_company_batch,
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


def test_supported_priority_level_tuple_returned_correctly() -> None:
    assert get_supported_priority_levels() == (
        "critical",
        "high",
        "medium",
        "low",
    )


def test_litigation_only_company_score_correct() -> None:
    result = prioritize_companies(
        _make_record(event_type="litigation", agency_or_court=None)
    )
    assert result["companies"][0]["priority_score"] == 40


def test_audit_only_company_score_correct() -> None:
    result = prioritize_companies(
        _make_record(event_type="audit", project_name=None, agency_or_court=None)
    )
    assert result["companies"][0]["priority_score"] == 30


def test_enforcement_only_company_score_correct() -> None:
    result = prioritize_companies(
        _make_record(event_type="enforcement", project_name=None, agency_or_court=None)
    )
    assert result["companies"][0]["priority_score"] == 35


def test_payment_only_company_score_correct() -> None:
    result = prioritize_companies(
        _make_record(event_type="lien", project_name=None, agency_or_court=None)
    )
    assert result["companies"][0]["priority_score"] == 25


def test_multi_signal_company_score_includes_additive_signal_scoring() -> None:
    original_aggregate = prioritizer_module.aggregate_company_signal_batch

    def _fake_aggregate(records: list[dict[str, object]]) -> dict[str, object]:
        return {
            "ok": True,
            "total_records": 1,
            "valid_records": 1,
            "invalid_records": 0,
            "results": [{"index": 0, "ok": True, "company_count": 1, "error": None}],
            "companies": [
                {
                    "company_node_id": "company:Harbor Construction LLC",
                    "signal_types": ["audit_risk", "enforcement_risk"],
                    "signal_count": 2,
                    "related_entities": [
                        "project:Port Expansion",
                        "agency:State Regulator",
                    ],
                }
            ],
        }

    prioritizer_module.aggregate_company_signal_batch = _fake_aggregate
    try:
        result = prioritize_companies(_make_record(event_type="audit"))
    finally:
        prioritizer_module.aggregate_company_signal_batch = original_aggregate

    assert result["companies"][0]["priority_score"] == 80


def test_signal_count_modifier_applied_correctly() -> None:
    original_aggregate = prioritizer_module.aggregate_company_signal_batch

    def _fake_aggregate(records: list[dict[str, object]]) -> dict[str, object]:
        return {
            "ok": True,
            "total_records": 1,
            "valid_records": 1,
            "invalid_records": 0,
            "results": [{"index": 0, "ok": True, "company_count": 1, "error": None}],
            "companies": [
                {
                    "company_node_id": "company:Harbor Construction LLC",
                    "signal_types": ["payment_risk"],
                    "signal_count": 2,
                    "related_entities": ["project:Port Expansion"],
                }
            ],
        }

    prioritizer_module.aggregate_company_signal_batch = _fake_aggregate
    try:
        result = prioritize_companies(_make_record(event_type="lien"))
    finally:
        prioritizer_module.aggregate_company_signal_batch = original_aggregate

    assert result["companies"][0]["priority_score"] == 35


def test_related_entity_modifier_applied_correctly() -> None:
    result = prioritize_companies(
        _make_record(event_type="audit", agency_or_court="State Auditor")
    )
    assert result["companies"][0]["priority_score"] == 35


def test_critical_threshold_correct() -> None:
    original_aggregate = prioritizer_module.aggregate_company_signal_batch

    def _fake_aggregate(records: list[dict[str, object]]) -> dict[str, object]:
        return {
            "ok": True,
            "total_records": 1,
            "valid_records": 1,
            "invalid_records": 0,
            "results": [{"index": 0, "ok": True, "company_count": 1, "error": None}],
            "companies": [
                {
                    "company_node_id": "company:Harbor Construction LLC",
                    "signal_types": ["litigation_risk", "audit_risk"],
                    "signal_count": 2,
                    "related_entities": ["project:Port Expansion", "court:Court A"],
                }
            ],
        }

    prioritizer_module.aggregate_company_signal_batch = _fake_aggregate
    try:
        result = prioritize_companies(_make_record())
    finally:
        prioritizer_module.aggregate_company_signal_batch = original_aggregate

    assert result["companies"][0]["priority_level"] == "critical"


def test_high_threshold_correct() -> None:
    result = prioritize_companies(
        _make_record(event_type="litigation", agency_or_court="Court A")
    )
    assert result["companies"][0]["priority_level"] == "high"


def test_medium_threshold_correct() -> None:
    result = prioritize_companies(
        _make_record(event_type="audit", project_name=None, agency_or_court=None)
    )
    assert result["companies"][0]["priority_level"] == "medium"


def test_low_threshold_correct() -> None:
    original_aggregate = prioritizer_module.aggregate_company_signal_batch

    def _fake_aggregate(records: list[dict[str, object]]) -> dict[str, object]:
        return {
            "ok": True,
            "total_records": 1,
            "valid_records": 1,
            "invalid_records": 0,
            "results": [{"index": 0, "ok": True, "company_count": 1, "error": None}],
            "companies": [
                {
                    "company_node_id": "company:Harbor Construction LLC",
                    "signal_types": [],
                    "signal_count": 0,
                    "related_entities": [],
                }
            ],
        }

    prioritizer_module.aggregate_company_signal_batch = _fake_aggregate
    try:
        result = prioritize_companies(_make_record())
    finally:
        prioritizer_module.aggregate_company_signal_batch = original_aggregate

    assert result["companies"][0]["priority_level"] == "low"


def test_exact_top_level_keys() -> None:
    result = prioritize_companies(_make_record())
    assert set(result.keys()) == {
        "source_event_id",
        "companies",
        "company_count",
    }


def test_exact_company_priority_keys() -> None:
    result = prioritize_companies(_make_record())
    assert set(result["companies"][0].keys()) == {
        "company_node_id",
        "priority_level",
        "priority_score",
        "signal_types",
        "signal_count",
        "related_entities",
    }


def test_descending_score_order_enforced() -> None:
    original_aggregate = prioritizer_module.aggregate_company_signal_batch

    def _fake_aggregate(records: list[dict[str, object]]) -> dict[str, object]:
        return {
            "ok": True,
            "total_records": 1,
            "valid_records": 1,
            "invalid_records": 0,
            "results": [{"index": 0, "ok": True, "company_count": 2, "error": None}],
            "companies": [
                {
                    "company_node_id": "company:Low",
                    "signal_types": ["payment_risk"],
                    "signal_count": 1,
                    "related_entities": [],
                },
                {
                    "company_node_id": "company:High",
                    "signal_types": ["litigation_risk"],
                    "signal_count": 1,
                    "related_entities": ["project:Port Expansion", "court:Court A"],
                },
            ],
        }

    prioritizer_module.aggregate_company_signal_batch = _fake_aggregate
    try:
        result = prioritize_companies(_make_record())
    finally:
        prioritizer_module.aggregate_company_signal_batch = original_aggregate

    assert [company["company_node_id"] for company in result["companies"]] == [
        "company:High",
        "company:Low",
    ]


def test_tie_case_preserves_first_seen_order() -> None:
    original_aggregate = prioritizer_module.aggregate_company_signal_batch

    def _fake_aggregate(records: list[dict[str, object]]) -> dict[str, object]:
        return {
            "ok": True,
            "total_records": 1,
            "valid_records": 1,
            "invalid_records": 0,
            "results": [{"index": 0, "ok": True, "company_count": 2, "error": None}],
            "companies": [
                {
                    "company_node_id": "company:First",
                    "signal_types": ["audit_risk"],
                    "signal_count": 1,
                    "related_entities": [],
                },
                {
                    "company_node_id": "company:Second",
                    "signal_types": ["audit_risk"],
                    "signal_count": 1,
                    "related_entities": [],
                },
            ],
        }

    prioritizer_module.aggregate_company_signal_batch = _fake_aggregate
    try:
        result = prioritize_companies(_make_record())
    finally:
        prioritizer_module.aggregate_company_signal_batch = original_aggregate

    assert [company["company_node_id"] for company in result["companies"]] == [
        "company:First",
        "company:Second",
    ]


def test_malformed_aggregator_output_raises_exact_message(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        prioritizer_module,
        "aggregate_company_signal_batch",
        lambda records: {"companies": []},
    )

    with pytest.raises(
        ValueError,
        match="company prioritization could not be completed from the provided record.",
    ):
        prioritize_companies(_make_record())


def test_missing_required_fields_raise_exact_message(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        prioritizer_module,
        "aggregate_company_signal_batch",
        lambda records: {
            "ok": True,
            "total_records": 1,
            "valid_records": 1,
            "invalid_records": 0,
            "results": [{"index": 0, "ok": True, "company_count": 1, "error": None}],
            "companies": [
                {
                    "company_node_id": "company:Harbor Construction LLC",
                    "signal_types": ["audit_risk"],
                    "signal_count": 1,
                }
            ],
        },
    )

    with pytest.raises(
        ValueError,
        match="company prioritization could not be completed from the provided record.",
    ):
        prioritize_companies(_make_record())


def test_unsupported_signal_type_raises_exact_message(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        prioritizer_module,
        "aggregate_company_signal_batch",
        lambda records: {
            "ok": True,
            "total_records": 1,
            "valid_records": 1,
            "invalid_records": 0,
            "results": [{"index": 0, "ok": True, "company_count": 1, "error": None}],
            "companies": [
                {
                    "company_node_id": "company:Harbor Construction LLC",
                    "signal_types": ["mystery_risk"],
                    "signal_count": 1,
                    "related_entities": [],
                }
            ],
        },
    )

    with pytest.raises(
        ValueError,
        match="company prioritization could not be completed from the provided record.",
    ):
        prioritize_companies(_make_record())


def test_all_valid_batch_success() -> None:
    result = prioritize_company_batch(
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
                "priority_level": "medium",
                "priority_score": 35,
                "signal_types": ["audit_risk"],
                "signal_count": 1,
                "related_entities": [
                    "project:Port Expansion",
                    "agency:Agency A",
                ],
            },
            {
                "company_node_id": "company:Harbor Construction LLC",
                "priority_level": "high",
                "priority_score": 45,
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

    result = prioritize_company_batch(
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
    result = prioritize_company_batch([])

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
        prioritize_company_batch({})


def test_non_dict_item() -> None:
    with pytest.raises(ValueError, match=r"^records\[1\] must be a dictionary\.$"):
        prioritize_company_batch([_make_record(), "bad-item"])


def test_flattened_prioritized_company_order_preserved_across_records() -> None:
    result = prioritize_company_batch(
        [
            _make_record(event_type="audit", event_id="EVT-001", agency_or_court="Agency A"),
            _make_record(event_type="lien", event_id="EVT-002", agency_or_court=None),
        ]
    )

    assert [company["priority_score"] for company in result["companies"]] == [35, 25]


def test_input_not_mutated() -> None:
    record = _make_record(event_type="audit", agency_or_court="Agency A")
    snapshot = copy.deepcopy(record)

    _ = prioritize_companies(record)

    assert record == snapshot


def test_output_mutation_does_not_mutate_original_input() -> None:
    record = _make_record(event_type="audit", agency_or_court="Agency A")
    result = prioritize_companies(record)

    result["companies"][0]["company_node_id"] = "mutated"
    result["companies"][0]["signal_types"].append("mutated")
    result["companies"][0]["related_entities"].append("mutated")

    assert record["event_id"] == "EVT-001"
    assert record["company_name"] == "Harbor Construction LLC"


def test_deterministic_repeated_runs() -> None:
    record = _make_record(event_type="litigation", agency_or_court="Court A")

    first = prioritize_companies(record)
    second = prioritize_companies(record)
    first_batch = prioritize_company_batch([record])
    second_batch = prioritize_company_batch([record])

    assert first == second
    assert first_batch == second_batch
