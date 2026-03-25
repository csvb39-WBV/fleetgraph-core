from __future__ import annotations

import copy
import pathlib
import sys

import pytest


PROJECT_ROOT = pathlib.Path(__file__).resolve().parents[2]
SRC_ROOT = PROJECT_ROOT / "src"

if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))


import fleetgraph_core.output.top_opportunities_view as top_opportunities_module
from fleetgraph_core.output.top_opportunities_view import (
    build_top_opportunities_view,
    build_top_opportunities_view_batch,
    get_supported_opportunity_priority_levels,
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


def _multi_company_prioritizer_output() -> dict[str, object]:
    return {
        "ok": True,
        "total_records": 1,
        "valid_records": 1,
        "invalid_records": 0,
        "results": [
            {
                "index": 0,
                "ok": True,
                "company_count": 4,
                "error": None,
            }
        ],
        "companies": [
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
    }


def test_supported_priority_level_tuple_returned_correctly() -> None:
    assert get_supported_opportunity_priority_levels() == (
        "critical",
        "high",
        "medium",
        "low",
    )


def test_minimum_priority_none_returns_all_companies(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        top_opportunities_module,
        "prioritize_company_batch",
        lambda records: _multi_company_prioritizer_output(),
    )

    result = build_top_opportunities_view(_make_record(), minimum_priority=None)

    assert [company["company_node_id"] for company in result["opportunities"]] == [
        "company:Critical Co",
        "company:High Co",
        "company:Medium Co",
        "company:Low Co",
    ]


def test_minimum_priority_critical_returns_only_critical(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        top_opportunities_module,
        "prioritize_company_batch",
        lambda records: _multi_company_prioritizer_output(),
    )

    result = build_top_opportunities_view(_make_record(), minimum_priority="critical")

    assert [company["company_node_id"] for company in result["opportunities"]] == [
        "company:Critical Co"
    ]


def test_minimum_priority_high_returns_critical_and_high(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        top_opportunities_module,
        "prioritize_company_batch",
        lambda records: _multi_company_prioritizer_output(),
    )

    result = build_top_opportunities_view(_make_record(), minimum_priority="high")

    assert [company["company_node_id"] for company in result["opportunities"]] == [
        "company:Critical Co",
        "company:High Co",
    ]


def test_minimum_priority_medium_returns_critical_high_and_medium(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        top_opportunities_module,
        "prioritize_company_batch",
        lambda records: _multi_company_prioritizer_output(),
    )

    result = build_top_opportunities_view(_make_record(), minimum_priority="medium")

    assert [company["company_node_id"] for company in result["opportunities"]] == [
        "company:Critical Co",
        "company:High Co",
        "company:Medium Co",
    ]


def test_minimum_priority_low_returns_all_supported_levels(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        top_opportunities_module,
        "prioritize_company_batch",
        lambda records: _multi_company_prioritizer_output(),
    )

    result = build_top_opportunities_view(_make_record(), minimum_priority="low")

    assert [company["company_node_id"] for company in result["opportunities"]] == [
        "company:Critical Co",
        "company:High Co",
        "company:Medium Co",
        "company:Low Co",
    ]


def test_limit_none_returns_all_filtered_companies(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        top_opportunities_module,
        "prioritize_company_batch",
        lambda records: _multi_company_prioritizer_output(),
    )

    result = build_top_opportunities_view(
        _make_record(),
        minimum_priority="high",
        limit=None,
    )

    assert [company["company_node_id"] for company in result["opportunities"]] == [
        "company:Critical Co",
        "company:High Co",
    ]


def test_limit_one_truncates_to_first_filtered_company(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        top_opportunities_module,
        "prioritize_company_batch",
        lambda records: _multi_company_prioritizer_output(),
    )

    result = build_top_opportunities_view(
        _make_record(),
        minimum_priority="medium",
        limit=1,
    )

    assert [company["company_node_id"] for company in result["opportunities"]] == [
        "company:Critical Co"
    ]


def test_limit_two_truncates_preserving_order(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        top_opportunities_module,
        "prioritize_company_batch",
        lambda records: _multi_company_prioritizer_output(),
    )

    result = build_top_opportunities_view(
        _make_record(),
        minimum_priority="low",
        limit=2,
    )

    assert [company["company_node_id"] for company in result["opportunities"]] == [
        "company:Critical Co",
        "company:High Co",
    ]


def test_exact_top_level_keys() -> None:
    result = build_top_opportunities_view(
        _make_record(event_type="litigation", agency_or_court="Court A")
    )

    assert set(result.keys()) == {
        "source_event_id",
        "opportunities",
        "opportunity_count",
    }


def test_exact_opportunity_keys() -> None:
    result = build_top_opportunities_view(
        _make_record(event_type="litigation", agency_or_court="Court A")
    )

    assert set(result["opportunities"][0].keys()) == {
        "company_node_id",
        "priority_level",
        "priority_score",
        "signal_types",
        "signal_count",
        "related_entities",
    }


def test_incoming_prioritizer_order_preserved_after_filtering(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        top_opportunities_module,
        "prioritize_company_batch",
        lambda records: _multi_company_prioritizer_output(),
    )

    result = build_top_opportunities_view(
        _make_record(),
        minimum_priority="medium",
    )

    assert [company["company_node_id"] for company in result["opportunities"]] == [
        "company:Critical Co",
        "company:High Co",
        "company:Medium Co",
    ]


def test_incoming_prioritizer_order_preserved_after_filtering_and_limit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        top_opportunities_module,
        "prioritize_company_batch",
        lambda records: _multi_company_prioritizer_output(),
    )

    result = build_top_opportunities_view(
        _make_record(),
        minimum_priority="medium",
        limit=2,
    )

    assert [company["company_node_id"] for company in result["opportunities"]] == [
        "company:Critical Co",
        "company:High Co",
    ]


def test_non_string_minimum_priority_raises_exact_message() -> None:
    with pytest.raises(
        ValueError,
        match=r"^minimum_priority must be a string or None\.$",
    ):
        build_top_opportunities_view(_make_record(), minimum_priority=1)


def test_unsupported_minimum_priority_raises_exact_message() -> None:
    with pytest.raises(
        ValueError,
        match=r"^minimum_priority must be one of the supported priority levels\.$",
    ):
        build_top_opportunities_view(_make_record(), minimum_priority="urgent")


def test_non_integer_limit_raises_exact_message() -> None:
    with pytest.raises(ValueError, match=r"^limit must be an integer or None\.$"):
        build_top_opportunities_view(_make_record(), limit="2")


def test_zero_or_negative_limit_raises_exact_message() -> None:
    with pytest.raises(ValueError, match=r"^limit must be greater than zero\.$"):
        build_top_opportunities_view(_make_record(), limit=0)

    with pytest.raises(ValueError, match=r"^limit must be greater than zero\.$"):
        build_top_opportunities_view(_make_record(), limit=-1)


def test_malformed_prioritizer_output_raises_exact_message(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        top_opportunities_module,
        "prioritize_company_batch",
        lambda records: {"companies": []},
    )

    with pytest.raises(
        ValueError,
        match="top opportunities view could not be built from the provided record.",
    ):
        build_top_opportunities_view(_make_record())


def test_missing_required_company_fields_raise_exact_message(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        top_opportunities_module,
        "prioritize_company_batch",
        lambda records: {
            "ok": True,
            "total_records": 1,
            "valid_records": 1,
            "invalid_records": 0,
            "results": [{"index": 0, "ok": True, "company_count": 1, "error": None}],
            "companies": [
                {
                    "company_node_id": "company:Critical Co",
                    "priority_level": "critical",
                    "priority_score": 80,
                    "signal_types": ["litigation_risk"],
                    "signal_count": 1,
                }
            ],
        },
    )

    with pytest.raises(
        ValueError,
        match="top opportunities view could not be built from the provided record.",
    ):
        build_top_opportunities_view(_make_record())


def test_all_valid_batch_success(monkeypatch: pytest.MonkeyPatch) -> None:
    outputs = [
        {
            "ok": True,
            "total_records": 1,
            "valid_records": 1,
            "invalid_records": 0,
            "results": [{"index": 0, "ok": True, "company_count": 2, "error": None}],
            "companies": [
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
            ],
        },
        {
            "ok": True,
            "total_records": 1,
            "valid_records": 1,
            "invalid_records": 0,
            "results": [{"index": 0, "ok": True, "company_count": 1, "error": None}],
            "companies": [
                {
                    "company_node_id": "company:Medium Co",
                    "priority_level": "medium",
                    "priority_score": 30,
                    "signal_types": ["audit_risk"],
                    "signal_count": 1,
                    "related_entities": ["project:Gamma"],
                }
            ],
        },
    ]

    def _fake_prioritize_company_batch(records: list[dict[str, object]]) -> dict[str, object]:
        del records
        return outputs.pop(0)

    monkeypatch.setattr(
        top_opportunities_module,
        "prioritize_company_batch",
        _fake_prioritize_company_batch,
    )

    result = build_top_opportunities_view_batch(
        [
            _make_record(event_type="litigation", event_id="EVT-001", agency_or_court="Court A"),
            _make_record(event_type="audit", event_id="EVT-002", agency_or_court="Agency A"),
        ],
        minimum_priority="medium",
        limit=2,
    )

    assert result == {
        "ok": True,
        "total_records": 2,
        "valid_records": 2,
        "invalid_records": 0,
        "results": [
            {"index": 0, "ok": True, "opportunity_count": 2, "error": None},
            {"index": 1, "ok": True, "opportunity_count": 1, "error": None},
        ],
        "views": [
            {
                "source_event_id": "EVT-001",
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
                ],
                "opportunity_count": 2,
            },
            {
                "source_event_id": "EVT-002",
                "opportunities": [
                    {
                        "company_node_id": "company:Medium Co",
                        "priority_level": "medium",
                        "priority_score": 30,
                        "signal_types": ["audit_risk"],
                        "signal_count": 1,
                        "related_entities": ["project:Gamma"],
                    }
                ],
                "opportunity_count": 1,
            },
        ],
    }


def test_mixed_failure_batch(monkeypatch: pytest.MonkeyPatch) -> None:
    outputs = [
        {
            "ok": True,
            "total_records": 1,
            "valid_records": 1,
            "invalid_records": 0,
            "results": [{"index": 0, "ok": True, "company_count": 1, "error": None}],
            "companies": [
                {
                    "company_node_id": "company:High Co",
                    "priority_level": "high",
                    "priority_score": 45,
                    "signal_types": ["litigation_risk"],
                    "signal_count": 1,
                    "related_entities": ["project:Beta", "court:Court B"],
                }
            ],
        },
        {
            "ok": False,
            "total_records": 1,
            "valid_records": 0,
            "invalid_records": 1,
            "results": [{"index": 0, "ok": False, "company_count": None, "error": "status must be one of the supported statuses."}],
            "companies": [],
        },
    ]

    def _fake_prioritize_company_batch(records: list[dict[str, object]]) -> dict[str, object]:
        del records
        return outputs.pop(0)

    monkeypatch.setattr(
        top_opportunities_module,
        "prioritize_company_batch",
        _fake_prioritize_company_batch,
    )

    result = build_top_opportunities_view_batch(
        [
            _make_record(event_type="litigation", event_id="EVT-001", agency_or_court="Court A"),
            _make_record(event_type="audit", event_id="EVT-002", agency_or_court="Agency A"),
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
    result = build_top_opportunities_view_batch([])

    assert result == {
        "ok": True,
        "total_records": 0,
        "valid_records": 0,
        "invalid_records": 0,
        "results": [],
        "views": [],
    }


def test_non_list_input() -> None:
    with pytest.raises(ValueError, match=r"^records must be a list\.$"):
        build_top_opportunities_view_batch({})


def test_non_dict_item() -> None:
    with pytest.raises(ValueError, match=r"^records\[1\] must be a dictionary\.$"):
        build_top_opportunities_view_batch([_make_record(), "bad-item"])


def test_view_order_preserved_across_batch(monkeypatch: pytest.MonkeyPatch) -> None:
    def _fake_prioritize_company_batch(records: list[dict[str, object]]) -> dict[str, object]:
        event_id = records[0]["event_id"]
        return {
            "ok": True,
            "total_records": 1,
            "valid_records": 1,
            "invalid_records": 0,
            "results": [{"index": 0, "ok": True, "company_count": 1, "error": None}],
            "companies": [
                {
                    "company_node_id": f"company:{event_id}",
                    "priority_level": "high",
                    "priority_score": 45,
                    "signal_types": ["litigation_risk"],
                    "signal_count": 1,
                    "related_entities": ["project:Beta"],
                }
            ],
        }

    monkeypatch.setattr(
        top_opportunities_module,
        "prioritize_company_batch",
        _fake_prioritize_company_batch,
    )

    result = build_top_opportunities_view_batch(
        [
            _make_record(event_type="litigation", event_id="EVT-010", agency_or_court="Court A"),
            _make_record(event_type="audit", event_id="EVT-020", agency_or_court="Agency B"),
        ]
    )

    assert [view["source_event_id"] for view in result["views"]] == [
        "EVT-010",
        "EVT-020",
    ]


def test_input_not_mutated() -> None:
    record = _make_record(event_type="audit", agency_or_court="Agency A")
    snapshot = copy.deepcopy(record)

    _ = build_top_opportunities_view(record)

    assert record == snapshot


def test_output_mutation_does_not_mutate_original_input() -> None:
    record = _make_record(event_type="audit", agency_or_court="Agency A")
    result = build_top_opportunities_view(record)

    result["opportunities"][0]["company_node_id"] = "mutated"
    result["opportunities"][0]["signal_types"].append("mutated")
    result["opportunities"][0]["related_entities"].append("mutated")

    assert record["event_id"] == "EVT-001"
    assert record["company_name"] == "Harbor Construction LLC"


def test_deterministic_repeated_runs() -> None:
    record = _make_record(event_type="litigation", agency_or_court="Court A")

    first = build_top_opportunities_view(record)
    second = build_top_opportunities_view(record)
    first_batch = build_top_opportunities_view_batch([record])
    second_batch = build_top_opportunities_view_batch([record])

    assert first == second
    assert first_batch == second_batch

