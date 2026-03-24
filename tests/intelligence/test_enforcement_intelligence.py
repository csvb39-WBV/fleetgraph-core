from __future__ import annotations

import pathlib
import sys

import pytest


PROJECT_ROOT = pathlib.Path(__file__).resolve().parents[2]
SRC_ROOT = PROJECT_ROOT / "src"

if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))


from fleetgraph_core.intelligence.enforcement_intelligence import parse_enforcement_signal


def _valid_payload() -> dict[str, object]:
    return {
        "action_id": "ACT-001",
        "company_name": "Summit Builders Inc",
        "agency": "State Department of Labor",
        "action_type": "Safety Violation",
        "severity": "High",
        "issued_date": "2026-03-24",
        "status": "Open",
        "penalty_amount": 45000.0,
        "source_name": "regulatory_enforcement",
    }


def test_parse_enforcement_signal_valid_payload_returns_expected_output() -> None:
    payload = _valid_payload()

    result = parse_enforcement_signal(payload)

    assert result == {
        "action_id": "ACT-001",
        "company_name": "Summit Builders Inc",
        "agency": "State Department of Labor",
        "action_type": "safety violation",
        "severity": "high",
        "issued_date": "2026-03-24",
        "status": "open",
        "penalty_amount": 45000.0,
        "source_name": "regulatory_enforcement",
    }


def test_parse_enforcement_signal_trims_whitespace_and_lowercases_required_fields() -> None:
    payload = {
        "action_id": "  ACT-002  ",
        "company_name": "  Atlas Contractors LLC  ",
        "agency": "  City Inspection Authority  ",
        "action_type": "  LICENSE BREACH  ",
        "severity": "  CRITICAL  ",
        "issued_date": "  2026-01-10  ",
        "status": "  CLOSED  ",
        "penalty_amount": 100000,
        "source_name": "  regulatory_enforcement  ",
    }

    result = parse_enforcement_signal(payload)

    assert result["action_id"] == "ACT-002"
    assert result["company_name"] == "Atlas Contractors LLC"
    assert result["agency"] == "City Inspection Authority"
    assert result["issued_date"] == "2026-01-10"
    assert result["action_type"] == "license breach"
    assert result["severity"] == "critical"
    assert result["status"] == "closed"
    assert result["source_name"] == "regulatory_enforcement"


@pytest.mark.parametrize(
    "source_name",
    ["agency_enforcement", "Regulatory_Enforcement", "", "   "],
)
def test_parse_enforcement_signal_rejects_invalid_source_name(source_name: str) -> None:
    payload = _valid_payload()
    payload["source_name"] = source_name

    with pytest.raises(ValueError, match="source_name"):
        parse_enforcement_signal(payload)


def test_parse_enforcement_signal_accepts_valid_source_name() -> None:
    payload = _valid_payload()
    payload["source_name"] = "regulatory_enforcement"

    result = parse_enforcement_signal(payload)

    assert result["source_name"] == "regulatory_enforcement"


@pytest.mark.parametrize("field_name", ["action_id", "penalty_amount", "source_name"])
def test_parse_enforcement_signal_rejects_missing_required_fields(field_name: str) -> None:
    payload = _valid_payload()
    payload.pop(field_name)

    with pytest.raises(ValueError, match=f"missing required field: {field_name}"):
        parse_enforcement_signal(payload)


@pytest.mark.parametrize(
    "field_name,invalid_value",
    [
        ("action_id", 123),
        ("company_name", None),
        ("agency", ""),
        ("action_type", "   "),
        ("severity", []),
        ("issued_date", 20260324),
        ("status", ""),
        ("source_name", 1),
    ],
)
def test_parse_enforcement_signal_rejects_invalid_string_fields(
    field_name: str,
    invalid_value: object,
) -> None:
    payload = _valid_payload()
    payload[field_name] = invalid_value

    with pytest.raises(ValueError, match=f"{field_name} must be a non-empty string"):
        parse_enforcement_signal(payload)


@pytest.mark.parametrize("invalid_value", ["5000", None, [], {}])
def test_parse_enforcement_signal_rejects_non_numeric_penalty_amount(invalid_value: object) -> None:
    payload = _valid_payload()
    payload["penalty_amount"] = invalid_value

    with pytest.raises(ValueError, match="penalty_amount must be a numeric value"):
        parse_enforcement_signal(payload)


def test_parse_enforcement_signal_rejects_boolean_penalty_amount() -> None:
    payload = _valid_payload()
    payload["penalty_amount"] = False

    with pytest.raises(ValueError, match="penalty_amount must be a numeric value"):
        parse_enforcement_signal(payload)


def test_parse_enforcement_signal_ignores_extra_input_fields() -> None:
    payload = _valid_payload()
    payload["extra_field"] = "ignored"

    result = parse_enforcement_signal(payload)

    assert "extra_field" not in result


def test_parse_enforcement_signal_output_has_exact_required_keys_only() -> None:
    result = parse_enforcement_signal(_valid_payload())

    assert set(result.keys()) == {
        "action_id",
        "company_name",
        "agency",
        "action_type",
        "severity",
        "issued_date",
        "status",
        "penalty_amount",
        "source_name",
    }


def test_parse_enforcement_signal_rejects_non_dict_payload() -> None:
    with pytest.raises(ValueError, match="payload must be a dictionary"):
        parse_enforcement_signal(["not", "a", "dict"])  # type: ignore[arg-type]


def test_parse_enforcement_signal_output_is_independent_of_input_mutation() -> None:
    payload = _valid_payload()
    result = parse_enforcement_signal(payload)

    payload["company_name"] = "Mutated Name"
    payload["penalty_amount"] = 0

    assert result["company_name"] == "Summit Builders Inc"
    assert result["penalty_amount"] == 45000.0
