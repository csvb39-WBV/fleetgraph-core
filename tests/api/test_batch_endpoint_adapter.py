from __future__ import annotations

import copy
import pathlib
import sys

import pytest


PROJECT_ROOT = pathlib.Path(__file__).resolve().parents[2]
SRC_ROOT = PROJECT_ROOT / "src"

if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))


import fleetgraph_core.api.batch_endpoint_adapter as batch_endpoint_module
from fleetgraph_core.api.batch_endpoint_adapter import apply_batch_endpoint_request
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


def _make_batch_request(
    *,
    request_id: str = "REQ-001",
    endpoint_id: str = "analysis",
    records: list[dict[str, object]] | None = None,
) -> dict[str, object]:
    if records is None:
        records = [_make_record()]

    return {
        "request_id": request_id,
        "endpoint_id": endpoint_id,
        "records": records,
    }


def test_valid_batch_request() -> None:
    result = apply_batch_endpoint_request(
        _make_batch_request(records=[_make_record(event_type="litigation", agency_or_court="Court A")])
    )

    assert result["request_id"] == "REQ-001"
    assert result["endpoint_id"] == "analysis"
    assert result["record_count"] == 1


def test_root_input_not_dict() -> None:
    with pytest.raises(ValueError, match=r"^batch_request must be a dictionary\.$"):
        apply_batch_endpoint_request("bad-request")


def test_missing_request_id() -> None:
    batch_request = _make_batch_request()
    del batch_request["request_id"]

    with pytest.raises(
        ValueError,
        match=r"^batch_request must contain exactly the required keys\.$",
    ):
        apply_batch_endpoint_request(batch_request)


def test_missing_endpoint_id() -> None:
    batch_request = _make_batch_request()
    del batch_request["endpoint_id"]

    with pytest.raises(
        ValueError,
        match=r"^batch_request must contain exactly the required keys\.$",
    ):
        apply_batch_endpoint_request(batch_request)


def test_missing_records() -> None:
    batch_request = _make_batch_request()
    del batch_request["records"]

    with pytest.raises(
        ValueError,
        match=r"^batch_request must contain exactly the required keys\.$",
    ):
        apply_batch_endpoint_request(batch_request)


def test_extra_root_field_rejection() -> None:
    batch_request = _make_batch_request()
    batch_request["extra"] = "value"

    with pytest.raises(
        ValueError,
        match=r"^batch_request must contain exactly the required keys\.$",
    ):
        apply_batch_endpoint_request(batch_request)


def test_empty_request_id() -> None:
    with pytest.raises(ValueError, match=r"^request_id must be a non-empty string\.$"):
        apply_batch_endpoint_request(_make_batch_request(request_id=""))


def test_whitespace_request_id() -> None:
    with pytest.raises(ValueError, match=r"^request_id must be a non-empty string\.$"):
        apply_batch_endpoint_request(_make_batch_request(request_id="   "))


def test_empty_endpoint_id() -> None:
    with pytest.raises(ValueError, match=r"^endpoint_id must be a non-empty string\.$"):
        apply_batch_endpoint_request(_make_batch_request(endpoint_id=""))


def test_whitespace_endpoint_id() -> None:
    with pytest.raises(ValueError, match=r"^endpoint_id must be a non-empty string\.$"):
        apply_batch_endpoint_request(_make_batch_request(endpoint_id="   "))


def test_request_id_and_endpoint_id_are_preserved_exactly(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        batch_endpoint_module,
        "handle_single_record_request",
        lambda request_payload: {"ok": True, "response": request_payload["response_type"]},
    )

    result = apply_batch_endpoint_request(
        _make_batch_request(
            request_id="  REQ-001  ",
            endpoint_id="  analysis  ",
            records=[_make_record(event_id="EVT-001")],
        )
    )

    assert result["request_id"] == "  REQ-001  "
    assert result["endpoint_id"] == "  analysis  "


def test_records_not_list() -> None:
    with pytest.raises(ValueError, match=r"^records must be a list\.$"):
        apply_batch_endpoint_request(_make_batch_request(records="bad-records"))


def test_empty_records() -> None:
    with pytest.raises(ValueError, match=r"^records must contain at least one item\.$"):
        apply_batch_endpoint_request(_make_batch_request(records=[]))


def test_record_item_not_dict() -> None:
    with pytest.raises(ValueError, match=r"^records\[1\] must be a dictionary\.$"):
        apply_batch_endpoint_request(_make_batch_request(records=[_make_record(), "bad-item"]))


def test_preserves_input_order_exactly(monkeypatch: pytest.MonkeyPatch) -> None:
    def _fake_handle_single_record_request(request_payload: dict[str, object]) -> dict[str, object]:
        event_id = request_payload["record"]["event_id"]
        return {"ok": True, "response": {"event_id": event_id}}

    monkeypatch.setattr(
        batch_endpoint_module,
        "handle_single_record_request",
        _fake_handle_single_record_request,
    )

    result = apply_batch_endpoint_request(
        _make_batch_request(
            records=[
                _make_record(event_id="EVT-010"),
                _make_record(event_id="EVT-020"),
                _make_record(event_id="EVT-030"),
            ]
        )
    )

    assert [item["response"]["event_id"] for item in result["results"]] == [
        "EVT-010",
        "EVT-020",
        "EVT-030",
    ]


def test_calls_single_record_adapter_once_per_record(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[str] = []

    def _fake_handle_single_record_request(request_payload: dict[str, object]) -> dict[str, object]:
        calls.append(request_payload["record"]["event_id"])
        return {"ok": True, "response": {"event_id": request_payload["record"]["event_id"]}}

    monkeypatch.setattr(
        batch_endpoint_module,
        "handle_single_record_request",
        _fake_handle_single_record_request,
    )

    apply_batch_endpoint_request(
        _make_batch_request(
            records=[
                _make_record(event_id="EVT-010"),
                _make_record(event_id="EVT-020"),
            ]
        )
    )

    assert calls == ["EVT-010", "EVT-020"]


def test_builds_per_record_request_correctly(monkeypatch: pytest.MonkeyPatch) -> None:
    captured_requests: list[dict[str, object]] = []

    def _fake_handle_single_record_request(request_payload: dict[str, object]) -> dict[str, object]:
        captured_requests.append(copy.deepcopy(request_payload))
        return {"ok": True, "response": {"event_id": request_payload["record"]["event_id"]}}

    monkeypatch.setattr(
        batch_endpoint_module,
        "handle_single_record_request",
        _fake_handle_single_record_request,
    )

    apply_batch_endpoint_request(
        _make_batch_request(
            endpoint_id="summary",
            records=[_make_record(event_id="EVT-123")],
        )
    )

    assert captured_requests == [
        {
            "response_type": "summary",
            "record": _make_record(event_id="EVT-123"),
            "limit": None,
            "minimum_priority": None,
        }
    ]


def test_record_count_exact(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        batch_endpoint_module,
        "handle_single_record_request",
        lambda request_payload: {"ok": True, "response": request_payload["record"]["event_id"]},
    )

    result = apply_batch_endpoint_request(
        _make_batch_request(records=[_make_record(event_id="EVT-1"), _make_record(event_id="EVT-2")])
    )

    assert result["record_count"] == 2


def test_success_count_exact(monkeypatch: pytest.MonkeyPatch) -> None:
    outputs = [
        {"ok": True, "response": {"id": "one"}},
        {"ok": False, "response": {"id": "two"}},
        {"ok": True, "response": {"id": "three"}},
    ]

    def _fake_handle_single_record_request(request_payload: dict[str, object]) -> dict[str, object]:
        del request_payload
        return copy.deepcopy(outputs.pop(0))

    monkeypatch.setattr(
        batch_endpoint_module,
        "handle_single_record_request",
        _fake_handle_single_record_request,
    )

    result = apply_batch_endpoint_request(
        _make_batch_request(
            records=[
                _make_record(event_id="EVT-1"),
                _make_record(event_id="EVT-2"),
                _make_record(event_id="EVT-3"),
            ]
        )
    )

    assert result["success_count"] == 2


def test_failure_count_exact(monkeypatch: pytest.MonkeyPatch) -> None:
    outputs = [
        {"ok": False, "response": {"id": "one"}},
        {"ok": False, "response": {"id": "two"}},
        {"ok": True, "response": {"id": "three"}},
    ]

    def _fake_handle_single_record_request(request_payload: dict[str, object]) -> dict[str, object]:
        del request_payload
        return copy.deepcopy(outputs.pop(0))

    monkeypatch.setattr(
        batch_endpoint_module,
        "handle_single_record_request",
        _fake_handle_single_record_request,
    )

    result = apply_batch_endpoint_request(
        _make_batch_request(
            records=[
                _make_record(event_id="EVT-1"),
                _make_record(event_id="EVT-2"),
                _make_record(event_id="EVT-3"),
            ]
        )
    )

    assert result["failure_count"] == 2


def test_batch_state_completed(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        batch_endpoint_module,
        "handle_single_record_request",
        lambda request_payload: {"ok": True, "response": request_payload["record"]["event_id"]},
    )

    result = apply_batch_endpoint_request(
        _make_batch_request(records=[_make_record(event_id="EVT-1"), _make_record(event_id="EVT-2")])
    )

    assert result["batch_state"] == "completed"


def test_batch_state_partial_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    outputs = [
        {"ok": True, "response": {"id": "one"}},
        {"ok": False, "response": {"id": "two"}},
    ]

    def _fake_handle_single_record_request(request_payload: dict[str, object]) -> dict[str, object]:
        del request_payload
        return copy.deepcopy(outputs.pop(0))

    monkeypatch.setattr(
        batch_endpoint_module,
        "handle_single_record_request",
        _fake_handle_single_record_request,
    )

    result = apply_batch_endpoint_request(
        _make_batch_request(records=[_make_record(event_id="EVT-1"), _make_record(event_id="EVT-2")])
    )

    assert result["batch_state"] == "partial_failure"


def test_batch_state_failed(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        batch_endpoint_module,
        "handle_single_record_request",
        lambda request_payload: {"ok": False, "response": request_payload["record"]["event_id"]},
    )

    result = apply_batch_endpoint_request(
        _make_batch_request(records=[_make_record(event_id="EVT-1"), _make_record(event_id="EVT-2")])
    )

    assert result["batch_state"] == "failed"


def test_repeated_call_with_same_input_and_mocked_upstream_returns_identical_output(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def _fake_handle_single_record_request(request_payload: dict[str, object]) -> dict[str, object]:
        return {
            "ok": True,
            "response": {
                "response_type": request_payload["response_type"],
                "source_event_id": request_payload["record"]["event_id"],
            },
        }

    monkeypatch.setattr(
        batch_endpoint_module,
        "handle_single_record_request",
        _fake_handle_single_record_request,
    )

    batch_request = _make_batch_request(
        records=[_make_record(event_id="EVT-1"), _make_record(event_id="EVT-2")]
    )

    first = apply_batch_endpoint_request(batch_request)
    second = apply_batch_endpoint_request(batch_request)

    assert first == second


def test_input_object_not_mutated() -> None:
    batch_request = _make_batch_request(
        records=[_make_record(event_type="audit", agency_or_court="Agency A")]
    )
    snapshot = copy.deepcopy(batch_request)

    _ = apply_batch_endpoint_request(batch_request)

    assert batch_request == snapshot


def test_upstream_returned_item_payloads_are_passed_through_unchanged(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    upstream_payload = {
        "ok": False,
        "response": {
            "response_type": "analysis",
            "source_event_id": "EVT-001",
            "detail": {"reason": "upstream-failure"},
        },
    }

    monkeypatch.setattr(
        batch_endpoint_module,
        "handle_single_record_request",
        lambda request_payload: copy.deepcopy(upstream_payload),
    )

    result = apply_batch_endpoint_request(
        _make_batch_request(records=[_make_record(event_id="EVT-001")])
    )

    assert result["results"] == [upstream_payload]


def test_upstream_raised_exception_propagates_unchanged(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def _raising_handle_single_record_request(request_payload: dict[str, object]) -> dict[str, object]:
        del request_payload
        raise RuntimeError("upstream boom")

    monkeypatch.setattr(
        batch_endpoint_module,
        "handle_single_record_request",
        _raising_handle_single_record_request,
    )

    with pytest.raises(RuntimeError, match="upstream boom"):
        apply_batch_endpoint_request(
            _make_batch_request(records=[_make_record(event_id="EVT-001")])
        )

