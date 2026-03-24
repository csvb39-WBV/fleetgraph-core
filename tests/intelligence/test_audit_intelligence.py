from __future__ import annotations

import pathlib
import sys

import pytest


PROJECT_ROOT = pathlib.Path(__file__).resolve().parents[2]
SRC_ROOT = PROJECT_ROOT / "src"

if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))


from fleetgraph_core.intelligence.audit_intelligence import parse_audit_signal


VALID_PAYLOAD = {
    "audit_id": " AUD-001 ",
    "company_name": " Acme Construction ",
    "agency": " OSHA ",
    "issue_type": " Safety Violation ",
    "severity": " High ",
    "opened_date": " 2026-03-20 ",
    "status": " Open ",
    "penalty_amount": 12500.5,
    "source_name": " regulatory_enforcement ",
}

EXPECTED_NORMALIZED_PAYLOAD = {
    "audit_id": "AUD-001",
    "company_name": "Acme Construction",
    "agency": "OSHA",
    "issue_type": "safety violation",
    "severity": "high",
    "opened_date": "2026-03-20",
    "status": "open",
    "penalty_amount": 12500.5,
    "source_name": "regulatory_enforcement",
}


def test_parse_audit_signal_returns_exact_normalized_output_for_valid_payload() -> None:
    assert parse_audit_signal(VALID_PAYLOAD) == EXPECTED_NORMALIZED_PAYLOAD


def test_parse_audit_signal_accepts_regulatory_enforcement_source() -> None:
    result = parse_audit_signal(VALID_PAYLOAD)

    assert result["source_name"] == "regulatory_enforcement"


def test_parse_audit_signal_accepts_osha_citations_source() -> None:
    payload = dict(VALID_PAYLOAD)
    payload["source_name"] = " osha_citations "

    result = parse_audit_signal(payload)

    assert result["source_name"] == "osha_citations"


def test_parse_audit_signal_trims_surrounding_whitespace() -> None:
    result = parse_audit_signal(VALID_PAYLOAD)

    assert result["audit_id"] == "AUD-001"
    assert result["company_name"] == "Acme Construction"
    assert result["agency"] == "OSHA"
    assert result["opened_date"] == "2026-03-20"
    assert result["source_name"] == "regulatory_enforcement"


def test_parse_audit_signal_lowercases_only_required_fields() -> None:
    result = parse_audit_signal(VALID_PAYLOAD)

    assert result["issue_type"] == "safety violation"
    assert result["severity"] == "high"
    assert result["status"] == "open"
    assert result["audit_id"] == "AUD-001"
    assert result["company_name"] == "Acme Construction"
    assert result["agency"] == "OSHA"
    assert result["opened_date"] == "2026-03-20"
    assert result["source_name"] == "regulatory_enforcement"


@pytest.mark.parametrize("source_name", ["permit", "court_dockets", "REGULATORY_ENFORCEMENT"])
def test_parse_audit_signal_rejects_unsupported_sources(source_name: str) -> None:
    payload = dict(VALID_PAYLOAD)
    payload["source_name"] = source_name

    with pytest.raises(ValueError, match=rf"source_name '{source_name.strip()}' is not supported"):
        parse_audit_signal(payload)


@pytest.mark.parametrize(
    "field_name",
    [
        "audit_id",
        "company_name",
        "agency",
        "issue_type",
        "severity",
        "opened_date",
        "status",
        "penalty_amount",
        "source_name",
    ],
)
def test_parse_audit_signal_rejects_missing_required_fields(field_name: str) -> None:
    payload = dict(VALID_PAYLOAD)
    del payload[field_name]

    with pytest.raises(ValueError, match=rf"{field_name} is required"):
        parse_audit_signal(payload)


@pytest.mark.parametrize(
    ("field_name", "value"),
    [
        ("audit_id", 123),
        ("company_name", 123),
        ("agency", 123),
        ("issue_type", 123),
        ("severity", 123),
        ("opened_date", 123),
        ("status", 123),
        ("source_name", 123),
    ],
)
def test_parse_audit_signal_rejects_non_string_required_string_fields(
    field_name: str,
    value: object,
) -> None:
    payload = dict(VALID_PAYLOAD)
    payload[field_name] = value

    with pytest.raises(ValueError, match=rf"{field_name} must be a string"):
        parse_audit_signal(payload)


@pytest.mark.parametrize(
    ("field_name", "value"),
    [
        ("audit_id", ""),
        ("company_name", "   "),
        ("agency", ""),
        ("issue_type", "   "),
        ("severity", ""),
        ("opened_date", "   "),
        ("status", ""),
        ("source_name", "   "),
    ],
)
def test_parse_audit_signal_rejects_empty_or_whitespace_only_string_fields(
    field_name: str,
    value: str,
) -> None:
    payload = dict(VALID_PAYLOAD)
    payload[field_name] = value

    with pytest.raises(ValueError, match=rf"{field_name} cannot be empty or whitespace-only"):
        parse_audit_signal(payload)


@pytest.mark.parametrize("penalty_amount", ["100", None, {}, []])
def test_parse_audit_signal_rejects_non_numeric_penalty_amount(penalty_amount: object) -> None:
    payload = dict(VALID_PAYLOAD)
    payload["penalty_amount"] = penalty_amount

    with pytest.raises(ValueError, match="penalty_amount must be an int or float"):
        parse_audit_signal(payload)


def test_parse_audit_signal_rejects_bool_penalty_amount() -> None:
    payload = dict(VALID_PAYLOAD)
    payload["penalty_amount"] = True

    with pytest.raises(ValueError, match="penalty_amount must be an int or float"):
        parse_audit_signal(payload)


def test_parse_audit_signal_ignores_extra_input_fields() -> None:
    payload = dict(VALID_PAYLOAD)
    payload["extra_field"] = "ignore me"

    result = parse_audit_signal(payload)

    assert result == EXPECTED_NORMALIZED_PAYLOAD


def test_parse_audit_signal_returns_exact_required_keys_only() -> None:
    result = parse_audit_signal(VALID_PAYLOAD)

    assert set(result.keys()) == {
        "audit_id",
        "company_name",
        "agency",
        "issue_type",
        "severity",
        "opened_date",
        "status",
        "penalty_amount",
        "source_name",
    }


def test_parse_audit_signal_output_is_copy_safe_from_later_input_mutation() -> None:
    payload = dict(VALID_PAYLOAD)

    result = parse_audit_signal(payload)

    payload["audit_id"] = "MUTATED"
    payload["company_name"] = "Changed"
    payload["penalty_amount"] = 0
    payload["source_name"] = "osha_citations"

    assert result == EXPECTED_NORMALIZED_PAYLOAD


def test_parse_audit_signal_rejects_non_dict_payload() -> None:
    with pytest.raises(ValueError, match="payload must be a dictionary"):
        parse_audit_signal([VALID_PAYLOAD])  # type: ignore[arg-type]
