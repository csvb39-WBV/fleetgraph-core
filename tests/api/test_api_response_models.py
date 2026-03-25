from __future__ import annotations

import copy
import pathlib
import sys

import pytest


PROJECT_ROOT = pathlib.Path(__file__).resolve().parents[2]
SRC_ROOT = PROJECT_ROOT / "src"

if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))


import fleetgraph_core.api.api_response_models as api_response_module
from fleetgraph_core.api.api_response_models import (
    build_analysis_response,
    build_summary_response,
    get_supported_api_response_types,
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


def test_supported_api_response_tuple_returned_correctly() -> None:
    assert get_supported_api_response_types() == (
        "analysis",
        "summary",
    )


def test_analysis_response_exact_top_level_keys() -> None:
    result = build_analysis_response(
        _make_record(event_type="litigation", agency_or_court="Court A")
    )

    assert set(result.keys()) == {
        "response_type",
        "source_event_id",
        "opportunity_count",
        "opportunities",
    }


def test_analysis_response_opportunity_count_copied_correctly() -> None:
    result = build_analysis_response(
        _make_record(event_type="litigation", agency_or_court="Court A")
    )

    assert result["response_type"] == "analysis"
    assert result["opportunity_count"] == 1


def test_analysis_response_opportunity_order_preserved(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        api_response_module,
        "build_top_opportunities_view",
        lambda record, limit=None, minimum_priority=None: {
            "source_event_id": "EVT-001",
            "opportunity_count": 2,
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
        },
    )

    result = build_analysis_response(_make_record())

    assert [opportunity["company_node_id"] for opportunity in result["opportunities"]] == [
        "company:First",
        "company:Second",
    ]


def test_analysis_response_limit_forwarding_works() -> None:
    original_builder = api_response_module.build_top_opportunities_view
    captured: dict[str, object] = {}

    def _capturing_builder(record, limit=None, minimum_priority=None):
        captured["limit"] = limit
        captured["minimum_priority"] = minimum_priority
        return original_builder(record, limit=limit, minimum_priority=minimum_priority)

    api_response_module.build_top_opportunities_view = _capturing_builder
    try:
        result = build_analysis_response(
            _make_record(event_type="litigation", agency_or_court="Court A"),
            limit=1,
        )
    finally:
        api_response_module.build_top_opportunities_view = original_builder

    assert captured == {"limit": 1, "minimum_priority": None}
    assert result["opportunity_count"] == 1


def test_analysis_response_minimum_priority_forwarding_works() -> None:
    original_builder = api_response_module.build_top_opportunities_view
    captured: dict[str, object] = {}

    def _capturing_builder(record, limit=None, minimum_priority=None):
        captured["limit"] = limit
        captured["minimum_priority"] = minimum_priority
        return original_builder(record, limit=limit, minimum_priority=minimum_priority)

    api_response_module.build_top_opportunities_view = _capturing_builder
    try:
        result = build_analysis_response(
            _make_record(event_type="litigation", agency_or_court="Court A"),
            minimum_priority="high",
        )
    finally:
        api_response_module.build_top_opportunities_view = original_builder

    assert captured == {"limit": None, "minimum_priority": "high"}
    assert result["opportunity_count"] == 1


def test_summary_response_exact_top_level_keys() -> None:
    result = build_summary_response(
        _make_record(event_type="audit", agency_or_court="Agency A")
    )

    assert set(result.keys()) == {
        "response_type",
        "source_event_id",
        "summary",
    }


def test_summary_response_exact_summary_keys() -> None:
    result = build_summary_response(
        _make_record(event_type="audit", agency_or_court="Agency A")
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


def test_summary_response_copied_correctly() -> None:
    result = build_summary_response(
        _make_record(event_type="audit", agency_or_court="Agency A")
    )

    assert result == {
        "response_type": "summary",
        "source_event_id": "EVT-001",
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
    }


def test_summary_response_limit_forwarding_works() -> None:
    original_builder = api_response_module.build_opportunity_summary
    captured: dict[str, object] = {}

    def _capturing_builder(record, limit=None, minimum_priority=None):
        captured["limit"] = limit
        captured["minimum_priority"] = minimum_priority
        return original_builder(record, limit=limit, minimum_priority=minimum_priority)

    api_response_module.build_opportunity_summary = _capturing_builder
    try:
        result = build_summary_response(
            _make_record(event_type="litigation", agency_or_court="Court A"),
            limit=1,
        )
    finally:
        api_response_module.build_opportunity_summary = original_builder

    assert captured == {"limit": 1, "minimum_priority": None}
    assert result["summary"]["opportunity_count"] == 1


def test_summary_response_minimum_priority_forwarding_works() -> None:
    original_builder = api_response_module.build_opportunity_summary
    captured: dict[str, object] = {}

    def _capturing_builder(record, limit=None, minimum_priority=None):
        captured["limit"] = limit
        captured["minimum_priority"] = minimum_priority
        return original_builder(record, limit=limit, minimum_priority=minimum_priority)

    api_response_module.build_opportunity_summary = _capturing_builder
    try:
        result = build_summary_response(
            _make_record(event_type="litigation", agency_or_court="Court A"),
            minimum_priority="high",
        )
    finally:
        api_response_module.build_opportunity_summary = original_builder

    assert captured == {"limit": None, "minimum_priority": "high"}
    assert result["summary"]["opportunity_count"] == 1


def test_malformed_analysis_upstream_output_raises_exact_message(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        api_response_module,
        "build_top_opportunities_view",
        lambda record, limit=None, minimum_priority=None: {"opportunities": []},
    )

    with pytest.raises(
        ValueError,
        match="api response could not be built from the provided record.",
    ):
        build_analysis_response(_make_record())


def test_malformed_summary_upstream_output_raises_exact_message(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        api_response_module,
        "build_opportunity_summary",
        lambda record, limit=None, minimum_priority=None: {"summary": {}},
    )

    with pytest.raises(
        ValueError,
        match="api response could not be built from the provided record.",
    ):
        build_summary_response(_make_record())


def test_missing_required_fields_raise_exact_message(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        api_response_module,
        "build_top_opportunities_view",
        lambda record, limit=None, minimum_priority=None: {
            "source_event_id": "EVT-001",
            "opportunity_count": 1,
            "opportunities": [
                {
                    "company_node_id": "company:Only",
                    "priority_level": "high",
                    "priority_score": 45,
                    "signal_types": ["litigation_risk"],
                    "signal_count": 1,
                }
            ],
        },
    )

    with pytest.raises(
        ValueError,
        match="api response could not be built from the provided record.",
    ):
        build_analysis_response(_make_record())


def test_input_not_mutated() -> None:
    record = _make_record(event_type="audit", agency_or_court="Agency A")
    snapshot = copy.deepcopy(record)

    _ = build_analysis_response(record)
    _ = build_summary_response(record)

    assert record == snapshot


def test_output_mutation_does_not_mutate_original_input() -> None:
    record = _make_record(event_type="audit", agency_or_court="Agency A")
    analysis_result = build_analysis_response(record)
    summary_result = build_summary_response(record)

    analysis_result["opportunities"][0]["company_node_id"] = "mutated"
    analysis_result["opportunities"][0]["signal_types"].append("mutated")
    summary_result["summary"]["top_company_node_id"] = "mutated"

    assert record["event_id"] == "EVT-001"
    assert record["company_name"] == "Harbor Construction LLC"


def test_deterministic_repeated_runs() -> None:
    record = _make_record(event_type="litigation", agency_or_court="Court A")

    first_analysis = build_analysis_response(record)
    second_analysis = build_analysis_response(record)
    first_summary = build_summary_response(record)
    second_summary = build_summary_response(record)

    assert first_analysis == second_analysis
    assert first_summary == second_summary
