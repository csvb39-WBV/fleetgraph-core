from __future__ import annotations

import copy
import pathlib
import sys

import pytest


PROJECT_ROOT = pathlib.Path(__file__).resolve().parents[2]
SRC_ROOT = PROJECT_ROOT / "src"

if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))


import fleetgraph_core.output.opportunity_summary as opportunity_summary_module
from fleetgraph_core.output.opportunity_summary import (
    build_opportunity_summary,
    build_opportunity_summary_batch,
    get_supported_summary_priority_levels,
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


def _multi_view_batch_output(source_event_id: str = "EVT-001") -> dict[str, object]:
    return {
        "ok": True,
        "total_records": 1,
        "valid_records": 1,
        "invalid_records": 0,
        "results": [{"index": 0, "ok": True, "opportunity_count": 4, "error": None}],
        "views": [
            {
                "source_event_id": source_event_id,
                "opportunities": [
                    {
                        "company_node_id": "company:Critical Co",
                        "priority_level": "critical",
                        "priority_score": 80,
                        "signal_types": ["litigation_risk", "audit_risk"],
                        "signal_count": 2,
                        "related_entities": ["project:Alpha", "court:Court A"],
                    },
                    {
                        "company_node_id": "company:High Co",
                        "priority_level": "high",
                        "priority_score": 45,
                        "signal_types": ["litigation_risk"],
                        "signal_count": 1,
                        "related_entities": ["project:Beta", "court:Court B"],
                    },
                    {
                        "company_node_id": "company:Medium Co",
                        "priority_level": "medium",
                        "priority_score": 30,
                        "signal_types": ["audit_risk"],
                        "signal_count": 1,
                        "related_entities": ["project:Gamma"],
                    },
                    {
                        "company_node_id": "company:Low Co",
                        "priority_level": "low",
                        "priority_score": 10,
                        "signal_types": [],
                        "signal_count": 0,
                        "related_entities": [],
                    },
                ],
                "opportunity_count": 4,
            }
        ],
    }


def test_supported_summary_priority_tuple_returned_correctly() -> None:
    assert get_supported_summary_priority_levels() == (
        "critical",
        "high",
        "medium",
        "low",
    )


def test_non_empty_view_produces_exact_counts_by_priority(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        opportunity_summary_module,
        "build_top_opportunities_view_batch",
        lambda records, limit=None, minimum_priority=None: _multi_view_batch_output(),
    )

    result = build_opportunity_summary(_make_record())

    assert result == {
        "source_event_id": "EVT-001",
        "summary": {
            "opportunity_count": 4,
            "critical_count": 1,
            "high_count": 1,
            "medium_count": 1,
            "low_count": 1,
            "top_company_node_id": "company:Critical Co",
            "top_priority_level": "critical",
            "top_priority_score": 80,
        },
    }


def test_top_company_fields_come_from_first_opportunity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        opportunity_summary_module,
        "build_top_opportunities_view_batch",
        lambda records, limit=None, minimum_priority=None: _multi_view_batch_output(),
    )

    result = build_opportunity_summary(_make_record())

    assert result["summary"]["top_company_node_id"] == "company:Critical Co"
    assert result["summary"]["top_priority_level"] == "critical"
    assert result["summary"]["top_priority_score"] == 80


def test_empty_view_produces_zero_counts_and_none_top_fields(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        opportunity_summary_module,
        "build_top_opportunities_view_batch",
        lambda records, limit=None, minimum_priority=None: {
            "ok": True,
            "total_records": 1,
            "valid_records": 1,
            "invalid_records": 0,
            "results": [{"index": 0, "ok": True, "opportunity_count": 0, "error": None}],
            "views": [
                {
                    "source_event_id": "EVT-001",
                    "opportunities": [],
                    "opportunity_count": 0,
                }
            ],
        },
    )

    result = build_opportunity_summary(_make_record())

    assert result == {
        "source_event_id": "EVT-001",
        "summary": {
            "opportunity_count": 0,
            "critical_count": 0,
            "high_count": 0,
            "medium_count": 0,
            "low_count": 0,
            "top_company_node_id": None,
            "top_priority_level": None,
            "top_priority_score": None,
        },
    }


def test_minimum_priority_affects_counts_through_upstream_filtering() -> None:
    result = build_opportunity_summary(
        _make_record(event_type="litigation", agency_or_court="Court A"),
        minimum_priority="high",
    )

    assert result["summary"]["opportunity_count"] == 1
    assert result["summary"]["critical_count"] == 0
    assert result["summary"]["high_count"] == 1
    assert result["summary"]["medium_count"] == 0
    assert result["summary"]["low_count"] == 0


def test_limit_affects_counts_through_upstream_truncation() -> None:
    original_builder = opportunity_summary_module.build_top_opportunities_view_batch
    captured: dict[str, object] = {}

    def _capturing_builder(
        records,
        limit=None,
        minimum_priority=None,
    ) -> dict[str, object]:
        captured["limit"] = limit
        captured["minimum_priority"] = minimum_priority
        return original_builder(records, limit=limit, minimum_priority=minimum_priority)

    opportunity_summary_module.build_top_opportunities_view_batch = _capturing_builder
    try:
        result = build_opportunity_summary(
            _make_record(event_type="litigation", agency_or_court="Court A"),
            limit=1,
        )
    finally:
        opportunity_summary_module.build_top_opportunities_view_batch = original_builder

    assert captured == {"limit": 1, "minimum_priority": None}
    assert result["summary"]["opportunity_count"] == 1


def test_exact_top_level_keys() -> None:
    result = build_opportunity_summary(
        _make_record(event_type="litigation", agency_or_court="Court A")
    )

    assert set(result.keys()) == {
        "source_event_id",
        "summary",
    }


def test_exact_summary_keys() -> None:
    result = build_opportunity_summary(
        _make_record(event_type="litigation", agency_or_court="Court A")
    )

    assert set(result["summary"].keys()) == {
        "opportunity_count",
        "critical_count",
        "high_count",
        "medium_count",
        "low_count",
        "top_company_node_id",
        "top_priority_level",
        "top_priority_score",
    }


def test_first_incoming_opportunity_determines_top_company_fields(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        opportunity_summary_module,
        "build_top_opportunities_view_batch",
        lambda records, limit=None, minimum_priority=None: {
            "ok": True,
            "total_records": 1,
            "valid_records": 1,
            "invalid_records": 0,
            "results": [{"index": 0, "ok": True, "opportunity_count": 2, "error": None}],
            "views": [
                {
                    "source_event_id": "EVT-001",
                    "opportunities": [
                        {
                            "company_node_id": "company:First",
                            "priority_level": "medium",
                            "priority_score": 30,
                            "signal_types": ["audit_risk"],
                            "signal_count": 1,
                            "related_entities": [],
                        },
                        {
                            "company_node_id": "company:Second",
                            "priority_level": "critical",
                            "priority_score": 80,
                            "signal_types": ["litigation_risk", "audit_risk"],
                            "signal_count": 2,
                            "related_entities": ["project:Alpha", "court:Court A"],
                        },
                    ],
                    "opportunity_count": 2,
                }
            ],
        },
    )

    result = build_opportunity_summary(_make_record())

    assert result["summary"]["top_company_node_id"] == "company:First"
    assert result["summary"]["top_priority_level"] == "medium"
    assert result["summary"]["top_priority_score"] == 30


def test_no_reordering_inside_summary_logic(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        opportunity_summary_module,
        "build_top_opportunities_view_batch",
        lambda records, limit=None, minimum_priority=None: {
            "ok": True,
            "total_records": 1,
            "valid_records": 1,
            "invalid_records": 0,
            "results": [{"index": 0, "ok": True, "opportunity_count": 2, "error": None}],
            "views": [
                {
                    "source_event_id": "EVT-001",
                    "opportunities": [
                        {
                            "company_node_id": "company:Medium First",
                            "priority_level": "medium",
                            "priority_score": 30,
                            "signal_types": ["audit_risk"],
                            "signal_count": 1,
                            "related_entities": [],
                        },
                        {
                            "company_node_id": "company:Critical Second",
                            "priority_level": "critical",
                            "priority_score": 80,
                            "signal_types": ["litigation_risk", "audit_risk"],
                            "signal_count": 2,
                            "related_entities": ["project:Alpha", "court:Court A"],
                        },
                    ],
                    "opportunity_count": 2,
                }
            ],
        },
    )

    result = build_opportunity_summary(_make_record())

    assert result["summary"]["top_company_node_id"] == "company:Medium First"


def test_malformed_upstream_output_raises_exact_message(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        opportunity_summary_module,
        "build_top_opportunities_view_batch",
        lambda records, limit=None, minimum_priority=None: {"views": []},
    )

    with pytest.raises(
        ValueError,
        match="opportunity summary could not be built from the provided record.",
    ):
        build_opportunity_summary(_make_record())


def test_missing_required_view_fields_raise_exact_message(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        opportunity_summary_module,
        "build_top_opportunities_view_batch",
        lambda records, limit=None, minimum_priority=None: {
            "ok": True,
            "total_records": 1,
            "valid_records": 1,
            "invalid_records": 0,
            "results": [{"index": 0, "ok": True, "opportunity_count": 1, "error": None}],
            "views": [
                {
                    "source_event_id": "EVT-001",
                    "opportunities": [],
                }
            ],
        },
    )

    with pytest.raises(
        ValueError,
        match="opportunity summary could not be built from the provided record.",
    ):
        build_opportunity_summary(_make_record())


def test_all_valid_batch_success() -> None:
    result = build_opportunity_summary_batch(
        [
            _make_record(event_type="litigation", event_id="EVT-001", agency_or_court="Court A"),
            _make_record(event_type="audit", event_id="EVT-002", agency_or_court="Agency A"),
        ]
    )

    assert result == {
        "ok": True,
        "total_records": 2,
        "valid_records": 2,
        "invalid_records": 0,
        "results": [
            {"index": 0, "ok": True, "opportunity_count": 1, "error": None},
            {"index": 1, "ok": True, "opportunity_count": 1, "error": None},
        ],
        "summaries": [
            {
                "source_event_id": "EVT-001",
                "summary": {
                    "opportunity_count": 1,
                    "critical_count": 0,
                    "high_count": 1,
                    "medium_count": 0,
                    "low_count": 0,
                    "top_company_node_id": "company:Harbor Construction LLC",
                    "top_priority_level": "high",
                    "top_priority_score": 45,
                },
            },
            {
                "source_event_id": "EVT-002",
                "summary": {
                    "opportunity_count": 1,
                    "critical_count": 0,
                    "high_count": 0,
                    "medium_count": 1,
                    "low_count": 0,
                    "top_company_node_id": "company:Harbor Construction LLC",
                    "top_priority_level": "medium",
                    "top_priority_score": 35,
                },
            },
        ],
    }


def test_mixed_failure_batch() -> None:
    invalid_record = _make_record(event_type="audit", event_id="EVT-002")
    invalid_record["status"] = "invalid-status"

    result = build_opportunity_summary_batch(
        [
            _make_record(event_type="litigation", event_id="EVT-001", agency_or_court="Court A"),
            invalid_record,
        ]
    )

    assert result["ok"] is False
    assert result["total_records"] == 2
    assert result["valid_records"] == 1
    assert result["invalid_records"] == 1
    assert result["results"] == [
        {"index": 0, "ok": True, "opportunity_count": 1, "error": None},
        {
            "index": 1,
            "ok": False,
            "opportunity_count": None,
            "error": "status must be one of the supported statuses.",
        },
    ]


def test_empty_batch() -> None:
    result = build_opportunity_summary_batch([])

    assert result == {
        "ok": True,
        "total_records": 0,
        "valid_records": 0,
        "invalid_records": 0,
        "results": [],
        "summaries": [],
    }


def test_non_list_input() -> None:
    with pytest.raises(ValueError, match=r"^records must be a list\.$"):
        build_opportunity_summary_batch({})


def test_non_dict_item() -> None:
    with pytest.raises(ValueError, match=r"^records\[1\] must be a dictionary\.$"):
        build_opportunity_summary_batch([_make_record(), "bad-item"])


def test_summary_order_preserved_across_batch() -> None:
    result = build_opportunity_summary_batch(
        [
            _make_record(event_type="litigation", event_id="EVT-010", agency_or_court="Court A"),
            _make_record(event_type="audit", event_id="EVT-020", agency_or_court="Agency B"),
        ]
    )

    assert [summary["source_event_id"] for summary in result["summaries"]] == [
        "EVT-010",
        "EVT-020",
    ]


def test_input_not_mutated() -> None:
    record = _make_record(event_type="audit", agency_or_court="Agency A")
    snapshot = copy.deepcopy(record)

    _ = build_opportunity_summary(record)

    assert record == snapshot


def test_output_mutation_does_not_mutate_original_input() -> None:
    record = _make_record(event_type="audit", agency_or_court="Agency A")
    result = build_opportunity_summary(record)

    result["summary"]["opportunity_count"] = 999
    result["summary"]["top_company_node_id"] = "mutated"

    assert record["event_id"] == "EVT-001"
    assert record["company_name"] == "Harbor Construction LLC"


def test_deterministic_repeated_runs() -> None:
    record = _make_record(event_type="litigation", agency_or_court="Court A")

    first = build_opportunity_summary(record)
    second = build_opportunity_summary(record)
    first_batch = build_opportunity_summary_batch([record])
    second_batch = build_opportunity_summary_batch([record])

    assert first == second
    assert first_batch == second_batch
