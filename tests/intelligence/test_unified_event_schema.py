from __future__ import annotations

import pathlib
import sys

import pytest


PROJECT_ROOT = pathlib.Path(__file__).resolve().parents[2]
SRC_ROOT = PROJECT_ROOT / "src"

if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))


from fleetgraph_core.intelligence.unified_event_schema import (
    build_unified_event_record,
    get_supported_event_types,
    validate_unified_event_record,
)


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


def test_get_supported_event_types_is_deterministic() -> None:
    assert get_supported_event_types() == (
        "litigation",
        "audit",
        "enforcement",
        "lien",
        "bond_claim",
    )


def test_build_unified_event_record_valid_litigation() -> None:
    result = build_unified_event_record(_litigation_record())
    assert result["event_type"] == "litigation"
    assert result["event_details"]["case_id"] == "CASE-1"


def test_build_unified_event_record_valid_audit() -> None:
    result = build_unified_event_record(_audit_record())
    assert result["event_type"] == "audit"
    assert result["event_details"]["audit_id"] == "AUD-1"


def test_build_unified_event_record_valid_enforcement() -> None:
    result = build_unified_event_record(_enforcement_record())
    assert result["event_type"] == "enforcement"
    assert result["event_details"]["action_id"] == "ACT-1"


def test_build_unified_event_record_valid_lien() -> None:
    result = build_unified_event_record(_lien_record())
    assert result["event_type"] == "lien"
    assert result["event_details"]["lien_id"] == "LIEN-1"


def test_build_unified_event_record_valid_bond_claim() -> None:
    result = build_unified_event_record(_bond_claim_record())
    assert result["event_type"] == "bond_claim"
    assert result["event_details"]["bond_claim_id"] == "BOND-1"


def test_validate_unified_event_record_returns_true_for_valid_record() -> None:
    record = build_unified_event_record(_litigation_record())
    assert validate_unified_event_record(record) is True


def test_build_unified_event_record_invalid_event_type() -> None:
    record = _litigation_record()
    record["event_type"] = "unknown"

    with pytest.raises(ValueError, match="event_type must be one of the supported event types"):
        build_unified_event_record(record)


def test_build_unified_event_record_invalid_status() -> None:
    record = _litigation_record()
    record["status"] = "in_progress"

    with pytest.raises(ValueError, match="status must be one of the supported statuses"):
        build_unified_event_record(record)


def test_build_unified_event_record_invalid_severity() -> None:
    record = _litigation_record()
    record["severity"] = "urgent"

    with pytest.raises(ValueError, match="severity must be one of the supported severity levels"):
        build_unified_event_record(record)


@pytest.mark.parametrize("field_name", ["event_id", "company_name", "source_name"])
def test_build_unified_event_record_missing_required_string(field_name: str) -> None:
    record = _litigation_record()
    record.pop(field_name)

    with pytest.raises(ValueError, match="record must contain exactly the canonical top-level keys"):
        build_unified_event_record(record)


@pytest.mark.parametrize("amount", [-1, True, "100"])
def test_build_unified_event_record_invalid_amount(amount: object) -> None:
    record = _litigation_record()
    record["amount"] = amount

    with pytest.raises(ValueError, match="amount must be a non-negative number"):
        build_unified_event_record(record)


def test_build_unified_event_record_invalid_list_member() -> None:
    record = _litigation_record()
    record["service_fit"] = ["valid", "   "]

    with pytest.raises(ValueError, match=r"service_fit\[1\] must be a non-empty string"):
        build_unified_event_record(record)


def test_build_unified_event_record_invalid_evidence_shape() -> None:
    record = _litigation_record()
    record["evidence"] = {"summary": "x"}

    with pytest.raises(ValueError, match="evidence must contain exactly"):
        build_unified_event_record(record)


def test_build_unified_event_record_invalid_event_details_type() -> None:
    record = _litigation_record()
    record["event_details"] = []

    with pytest.raises(ValueError, match="event_details must be a dictionary"):
        build_unified_event_record(record)


@pytest.mark.parametrize(
    "record_factory,missing_key,expected_message",
    [
        (_litigation_record, "case_id", "event_details.case_id is required"),
        (_audit_record, "audit_id", "event_details.audit_id is required"),
        (_enforcement_record, "action_id", "event_details.action_id is required"),
        (_lien_record, "lien_id", "event_details.lien_id is required"),
        (_bond_claim_record, "bond_claim_id", "event_details.bond_claim_id is required"),
    ],
)
def test_build_unified_event_record_missing_required_event_detail_key(
    record_factory,
    missing_key: str,
    expected_message: str,
) -> None:
    record = record_factory()
    del record["event_details"][missing_key]

    with pytest.raises(ValueError, match=expected_message):
        build_unified_event_record(record)


@pytest.mark.parametrize(
    "record_factory,detail_key,detail_value,expected_message",
    [
        (_litigation_record, "case_type", "", "event_details.case_type must be a non-empty string"),
        (_audit_record, "agency", 1, "event_details.agency must be a non-empty string"),
        (_enforcement_record, "issue_type", "   ", "event_details.issue_type must be a non-empty string"),
        (_lien_record, "claimant_role", [], "event_details.claimant_role must be a non-empty string"),
        (_bond_claim_record, "filing_date", None, "event_details.filing_date must be a non-empty string"),
    ],
)
def test_build_unified_event_record_invalid_event_detail_values(
    record_factory,
    detail_key: str,
    detail_value: object,
    expected_message: str,
) -> None:
    record = record_factory()
    record["event_details"][detail_key] = detail_value

    with pytest.raises(ValueError, match=expected_message):
        build_unified_event_record(record)


def test_build_unified_event_record_defensive_copy_for_nested_structures() -> None:
    record = _litigation_record()
    built = build_unified_event_record(record)

    built["event_details"]["case_id"] = "MUTATED"
    built["evidence"]["summary"] = "changed"

    assert record["event_details"]["case_id"] == "CASE-1"
    assert record["evidence"]["summary"] == "Documented filing in district court"


def test_input_mutation_after_build_does_not_mutate_output() -> None:
    record = _litigation_record()
    built = build_unified_event_record(record)

    record["event_details"]["case_id"] = "CHANGED"
    record["service_fit"].append("new_tag")

    assert built["event_details"]["case_id"] == "CASE-1"
    assert built["service_fit"] == ["litigation_response"]


def test_output_mutation_does_not_retroactively_alter_original_input() -> None:
    record = _litigation_record()
    built = build_unified_event_record(record)

    built["trigger_tags"].append("mutated")
    built["event_details"]["plaintiff_role"] = "changed"

    assert record["trigger_tags"] == ["high_risk"]
    assert record["event_details"]["plaintiff_role"] == "plaintiff"


def test_build_unified_event_record_rejects_extra_top_level_key() -> None:
    record = _litigation_record()
    record["unauthorized_field"] = "should_not_be_here"

    with pytest.raises(ValueError, match="record must contain exactly the canonical top-level keys"):
        build_unified_event_record(record)


def test_build_unified_event_record_rejects_missing_top_level_key() -> None:
    record = _litigation_record()
    record.pop("jurisdiction")

    with pytest.raises(ValueError, match="record must contain exactly the canonical top-level keys"):
        build_unified_event_record(record)
