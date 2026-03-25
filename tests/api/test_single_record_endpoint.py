from __future__ import annotations

import copy
import pathlib
import sys

import pytest


PROJECT_ROOT = pathlib.Path(__file__).resolve().parents[2]
SRC_ROOT = PROJECT_ROOT / "src"

if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))


import fleetgraph_core.api.single_record_endpoint as endpoint_module
from fleetgraph_core.api.single_record_endpoint import (
    get_supported_single_record_response_types,
    handle_single_record_request,
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


def _make_request_payload(
    *,
    response_type: str = "analysis",
    record: dict[str, object] | None = None,
    limit: int | None = None,
    minimum_priority: str | None = None,
) -> dict[str, object]:
    if record is None:
        record = _make_record()

    return {
        "response_type": response_type,
        "record": record,
        "limit": limit,
        "minimum_priority": minimum_priority,
    }


def test_supported_single_record_response_tuple_returned_correctly() -> None:
    assert get_supported_single_record_response_types() == (
        "analysis",
        "summary",
    )


def test_non_dict_request_payload_raises_exact_message() -> None:
    with pytest.raises(ValueError, match=r"^request_payload must be a dictionary\.$"):
        handle_single_record_request("bad-payload")


def test_missing_key_raises_exact_message() -> None:
    payload = _make_request_payload()
    del payload["limit"]

    with pytest.raises(
        ValueError,
        match=r"^request_payload must contain exactly the required keys\.$",
    ):
        handle_single_record_request(payload)


def test_extra_key_raises_exact_message() -> None:
    payload = _make_request_payload()
    payload["extra"] = "value"

    with pytest.raises(
        ValueError,
        match=r"^request_payload must contain exactly the required keys\.$",
    ):
        handle_single_record_request(payload)


def test_non_string_response_type_raises_exact_message() -> None:
    payload = _make_request_payload(response_type=1)

    with pytest.raises(ValueError, match=r"^response_type must be a string\.$"):
        handle_single_record_request(payload)


def test_unsupported_response_type_raises_exact_message() -> None:
    payload = _make_request_payload(response_type="detail")

    with pytest.raises(
        ValueError,
        match=r"^response_type must be one of the supported response types\.$",
    ):
        handle_single_record_request(payload)


def test_non_dict_record_raises_exact_message() -> None:
    payload = _make_request_payload(record="bad-record")

    with pytest.raises(ValueError, match=r"^record must be a dictionary\.$"):
        handle_single_record_request(payload)


def test_analysis_response_dispatches_correctly() -> None:
    result = handle_single_record_request(
        _make_request_payload(
            response_type="analysis",
            record=_make_record(event_type="litigation", agency_or_court="Court A"),
        )
    )

    assert result["ok"] is True
    assert result["response"]["response_type"] == "analysis"
    assert result["response"]["source_event_id"] == "EVT-001"


def test_summary_response_dispatches_correctly() -> None:
    result = handle_single_record_request(
        _make_request_payload(
            response_type="summary",
            record=_make_record(event_type="audit", agency_or_court="Agency A"),
        )
    )

    assert result["ok"] is True
    assert result["response"]["response_type"] == "summary"
    assert result["response"]["source_event_id"] == "EVT-001"


def test_limit_forwarded_correctly() -> None:
    original_builder = endpoint_module.build_analysis_response
    captured: dict[str, object] = {}

    def _capturing_builder(record, limit=None, minimum_priority=None):
        captured["limit"] = limit
        captured["minimum_priority"] = minimum_priority
        return original_builder(record, limit=limit, minimum_priority=minimum_priority)

    endpoint_module.build_analysis_response = _capturing_builder
    try:
        result = handle_single_record_request(
            _make_request_payload(
                response_type="analysis",
                record=_make_record(event_type="litigation", agency_or_court="Court A"),
                limit=1,
            )
        )
    finally:
        endpoint_module.build_analysis_response = original_builder

    assert captured == {"limit": 1, "minimum_priority": None}
    assert result["ok"] is True


def test_minimum_priority_forwarded_correctly() -> None:
    original_builder = endpoint_module.build_summary_response
    captured: dict[str, object] = {}

    def _capturing_builder(record, limit=None, minimum_priority=None):
        captured["limit"] = limit
        captured["minimum_priority"] = minimum_priority
        return original_builder(record, limit=limit, minimum_priority=minimum_priority)

    endpoint_module.build_summary_response = _capturing_builder
    try:
        result = handle_single_record_request(
            _make_request_payload(
                response_type="summary",
                record=_make_record(event_type="litigation", agency_or_court="Court A"),
                minimum_priority="high",
            )
        )
    finally:
        endpoint_module.build_summary_response = original_builder

    assert captured == {"limit": None, "minimum_priority": "high"}
    assert result["ok"] is True


def test_exact_top_level_keys() -> None:
    result = handle_single_record_request(
        _make_request_payload(
            response_type="analysis",
            record=_make_record(event_type="litigation", agency_or_court="Court A"),
        )
    )

    assert set(result.keys()) == {"ok", "response"}


def test_response_returned_exactly_from_upstream_adapter_analysis(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        endpoint_module,
        "build_analysis_response",
        lambda record, limit=None, minimum_priority=None: {
            "response_type": "analysis",
            "source_event_id": "EVT-001",
            "opportunity_count": 1,
            "opportunities": [
                {
                    "company_node_id": "company:Only",
                    "priority_level": "high",
                    "priority_score": 45,
                    "signal_types": ["litigation_risk"],
                    "signal_count": 1,
                    "related_entities": ["project:Alpha", "court:Court A"],
                }
            ],
        },
    )

    result = handle_single_record_request(_make_request_payload())

    assert result == {
        "ok": True,
        "response": {
            "response_type": "analysis",
            "source_event_id": "EVT-001",
            "opportunity_count": 1,
            "opportunities": [
                {
                    "company_node_id": "company:Only",
                    "priority_level": "high",
                    "priority_score": 45,
                    "signal_types": ["litigation_risk"],
                    "signal_count": 1,
                    "related_entities": ["project:Alpha", "court:Court A"],
                }
            ],
        },
    }


def test_response_returned_exactly_from_upstream_adapter_summary(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        endpoint_module,
        "build_summary_response",
        lambda record, limit=None, minimum_priority=None: {
            "response_type": "summary",
            "source_event_id": "EVT-001",
            "summary": {
                "opportunity_count": 1,
                "critical_count": 0,
                "high_count": 1,
                "medium_count": 0,
                "low_count": 0,
                "top_company_node_id": "company:Only",
                "top_priority_level": "high",
                "top_priority_score": 45,
            },
        },
    )

    result = handle_single_record_request(
        _make_request_payload(response_type="summary")
    )

    assert result == {
        "ok": True,
        "response": {
            "response_type": "summary",
            "source_event_id": "EVT-001",
            "summary": {
                "opportunity_count": 1,
                "critical_count": 0,
                "high_count": 1,
                "medium_count": 0,
                "low_count": 0,
                "top_company_node_id": "company:Only",
                "top_priority_level": "high",
                "top_priority_score": 45,
            },
        },
    }


def test_malformed_downstream_analysis_output_raises_exact_message(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        endpoint_module,
        "build_analysis_response",
        lambda record, limit=None, minimum_priority=None: "bad-output",
    )

    with pytest.raises(
        ValueError,
        match=(
            r"^single record endpoint could not be built from the provided request\.$"
        ),
    ):
        handle_single_record_request(_make_request_payload(response_type="analysis"))


def test_malformed_downstream_analysis_dict_output_raises_exact_message(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        endpoint_module,
        "build_analysis_response",
        lambda record, limit=None, minimum_priority=None: {
            "response_type": "analysis",
            "source_event_id": "EVT-001",
        },
    )

    with pytest.raises(
        ValueError,
        match=(
            r"^single record endpoint could not be built from the provided request\.$"
        ),
    ):
        handle_single_record_request(_make_request_payload(response_type="analysis"))


def test_malformed_downstream_summary_output_raises_exact_message(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        endpoint_module,
        "build_summary_response",
        lambda record, limit=None, minimum_priority=None: [],
    )

    with pytest.raises(
        ValueError,
        match=(
            r"^single record endpoint could not be built from the provided request\.$"
        ),
    ):
        handle_single_record_request(_make_request_payload(response_type="summary"))


def test_malformed_downstream_summary_dict_output_raises_exact_message(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        endpoint_module,
        "build_summary_response",
        lambda record, limit=None, minimum_priority=None: {
            "response_type": "summary",
            "source_event_id": "EVT-001",
            "summary": {
                "opportunity_count": 1,
            },
        },
    )

    with pytest.raises(
        ValueError,
        match=(
            r"^single record endpoint could not be built from the provided request\.$"
        ),
    ):
        handle_single_record_request(_make_request_payload(response_type="summary"))


def test_input_not_mutated() -> None:
    payload = _make_request_payload(
        response_type="analysis",
        record=_make_record(event_type="audit", agency_or_court="Agency A"),
    )
    snapshot = copy.deepcopy(payload)

    _ = handle_single_record_request(payload)

    assert payload == snapshot


def test_output_mutation_does_not_mutate_original_input() -> None:
    payload = _make_request_payload(
        response_type="analysis",
        record=_make_record(event_type="audit", agency_or_court="Agency A"),
    )
    result = handle_single_record_request(payload)

    result["response"]["source_event_id"] = "mutated"
    result["response"]["opportunities"][0]["company_node_id"] = "mutated"
    result["response"]["opportunities"][0]["signal_types"].append("mutated")

    assert payload["record"]["event_id"] == "EVT-001"
    assert payload["record"]["company_name"] == "Harbor Construction LLC"


def test_deterministic_repeated_runs() -> None:
    payload = _make_request_payload(
        response_type="summary",
        record=_make_record(event_type="litigation", agency_or_court="Court A"),
    )

    first = handle_single_record_request(payload)
    second = handle_single_record_request(payload)

    assert first == second
