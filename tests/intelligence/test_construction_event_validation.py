from __future__ import annotations

import copy
import pathlib
import sys

import pytest


PROJECT_ROOT = pathlib.Path(__file__).resolve().parents[2]
SRC_ROOT = PROJECT_ROOT / "src"

if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))


from fleetgraph_core.intelligence.construction_event_validation import (
    get_supported_construction_event_types,
    validate_construction_event_batch,
    validate_construction_event_record,
)
from fleetgraph_core.intelligence.unified_event_schema import get_supported_event_types


def _base_record(event_type: str, event_details: dict[str, object]) -> dict[str, object]:
    return {
        "event_id": f"{event_type}-001",
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
            "summary": "Documented record",
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


def test_supported_construction_event_types_are_returned_correctly() -> None:
    canonical_types = get_supported_event_types()

    assert get_supported_construction_event_types() == tuple(
        event_type
        for event_type in canonical_types
        if event_type in ("litigation", "audit", "enforcement", "lien", "bond_claim")
    )


def test_valid_litigation_record_returns_success_payload() -> None:
    result = validate_construction_event_record(_litigation_record())

    assert result == {
        "ok": True,
        "event_type": "litigation",
        "event_id": "litigation-001",
    }


def test_valid_audit_record_returns_success_payload() -> None:
    result = validate_construction_event_record(_audit_record())

    assert result == {
        "ok": True,
        "event_type": "audit",
        "event_id": "audit-001",
    }


def test_valid_record_preserves_original_event_type_and_event_id() -> None:
    record = _enforcement_record()
    result = validate_construction_event_record(record)

    assert result["event_type"] == record["event_type"]
    assert result["event_id"] == record["event_id"]


def test_invalid_record_raises_value_error() -> None:
    record = _audit_record()
    record["status"] = "invalid"

    with pytest.raises(ValueError):
        validate_construction_event_record(record)


def test_canonical_schema_error_text_is_preserved() -> None:
    record = _lien_record()
    record["status"] = "invalid"

    with pytest.raises(ValueError, match="status must be one of the supported statuses."):
        validate_construction_event_record(record)


def test_construction_validator_rejects_canonical_but_non_construction_type(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "fleetgraph_core.intelligence.construction_event_validation.validate_unified_event_record",
        lambda record: True,
    )
    monkeypatch.setattr(
        "fleetgraph_core.intelligence.construction_event_validation.get_supported_event_types",
        lambda: (
            "litigation",
            "audit",
            "enforcement",
            "lien",
            "bond_claim",
            "fleet_signal",
        ),
    )

    record = _audit_record()
    record["event_type"] = "fleet_signal"

    with pytest.raises(
        ValueError,
        match="event_type must be one of the supported construction event types.",
    ):
        validate_construction_event_record(record)


def test_all_valid_batch_returns_correct_totals_and_ordered_results() -> None:
    records = [
        _litigation_record(),
        _audit_record(),
        _bond_claim_record(),
    ]

    result = validate_construction_event_batch(records)

    assert result == {
        "ok": True,
        "total_records": 3,
        "valid_records": 3,
        "invalid_records": 0,
        "results": [
            {
                "index": 0,
                "ok": True,
                "event_type": "litigation",
                "event_id": "litigation-001",
                "error": None,
            },
            {
                "index": 1,
                "ok": True,
                "event_type": "audit",
                "event_id": "audit-001",
                "error": None,
            },
            {
                "index": 2,
                "ok": True,
                "event_type": "bond_claim",
                "event_id": "bond_claim-001",
                "error": None,
            },
        ],
    }


def test_mixed_batch_returns_correct_indexed_failures() -> None:
    invalid_record = _audit_record()
    invalid_record["status"] = "invalid"
    records = [
        _litigation_record(),
        invalid_record,
        _lien_record(),
    ]

    result = validate_construction_event_batch(records)

    assert result == {
        "ok": False,
        "total_records": 3,
        "valid_records": 2,
        "invalid_records": 1,
        "results": [
            {
                "index": 0,
                "ok": True,
                "event_type": "litigation",
                "event_id": "litigation-001",
                "error": None,
            },
            {
                "index": 1,
                "ok": False,
                "event_type": None,
                "event_id": None,
                "error": "status must be one of the supported statuses.",
            },
            {
                "index": 2,
                "ok": True,
                "event_type": "lien",
                "event_id": "lien-001",
                "error": None,
            },
        ],
    }


def test_empty_batch_is_allowed() -> None:
    assert validate_construction_event_batch([]) == {
        "ok": True,
        "total_records": 0,
        "valid_records": 0,
        "invalid_records": 0,
        "results": [],
    }


def test_non_list_batch_raises() -> None:
    with pytest.raises(ValueError, match="records must be a list\\."):
        validate_construction_event_batch({"records": []})  # type: ignore[arg-type]


def test_non_dict_batch_item_raises() -> None:
    with pytest.raises(ValueError, match=r"records\[1\] must be a dictionary\."):
        validate_construction_event_batch([_audit_record(), "bad-item"])  # type: ignore[list-item]


def test_record_validation_does_not_mutate_input() -> None:
    record = _litigation_record()
    before = copy.deepcopy(record)

    validate_construction_event_record(record)

    assert record == before


def test_batch_validation_does_not_mutate_input() -> None:
    records = [_litigation_record(), _audit_record()]
    before = copy.deepcopy(records)

    validate_construction_event_batch(records)

    assert records == before


def test_repeated_batch_runs_are_deterministic() -> None:
    invalid_record = _audit_record()
    invalid_record["status"] = "invalid"
    records = [_enforcement_record(), invalid_record, _bond_claim_record()]

    first = validate_construction_event_batch(records)
    second = validate_construction_event_batch(records)

    assert first == second
