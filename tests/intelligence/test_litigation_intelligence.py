from __future__ import annotations

import pathlib
import sys

import pytest


PROJECT_ROOT = pathlib.Path(__file__).resolve().parents[2]
SRC_ROOT = PROJECT_ROOT / "src"

if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))


from fleetgraph_core.intelligence.litigation_intelligence import parse_litigation_signal


def _valid_payload() -> dict[str, object]:
    return {
        "case_id": "CASE-001",
        "company_name": "Acme Construction LLC",
        "jurisdiction": "Texas",
        "case_type": "Contract Dispute",
        "filing_date": "2026-03-24",
        "status": "Open",
        "alleged_amount": 1250000.5,
        "plaintiff_role": "Plaintiff",
        "defendant_role": "Defendant",
        "source_name": "court_dockets",
    }


def test_parse_litigation_signal_valid_payload_returns_expected_output() -> None:
    payload = _valid_payload()

    result = parse_litigation_signal(payload)

    assert result == {
        "case_id": "CASE-001",
        "company_name": "Acme Construction LLC",
        "jurisdiction": "Texas",
        "case_type": "contract dispute",
        "filing_date": "2026-03-24",
        "status": "open",
        "alleged_amount": 1250000.5,
        "plaintiff_role": "plaintiff",
        "defendant_role": "defendant",
        "source_name": "court_dockets",
    }


def test_parse_litigation_signal_trims_whitespace_and_lowercases_required_fields() -> None:
    payload = {
        "case_id": "  CASE-002  ",
        "company_name": "  BuildCo Inc  ",
        "jurisdiction": "  New York  ",
        "case_type": "  NEGLIGENCE  ",
        "filing_date": "  2026-01-15  ",
        "status": "  CLOSED  ",
        "alleged_amount": 70000,
        "plaintiff_role": "  PLAINTIFF  ",
        "defendant_role": "  DEFENDANT  ",
        "source_name": "  court_dockets  ",
    }

    result = parse_litigation_signal(payload)

    assert result["case_id"] == "CASE-002"
    assert result["company_name"] == "BuildCo Inc"
    assert result["jurisdiction"] == "New York"
    assert result["filing_date"] == "2026-01-15"
    assert result["case_type"] == "negligence"
    assert result["status"] == "closed"
    assert result["plaintiff_role"] == "plaintiff"
    assert result["defendant_role"] == "defendant"
    assert result["source_name"] == "court_dockets"


@pytest.mark.parametrize("source_name", ["court_records", "Court_Dockets", "", "   "])
def test_parse_litigation_signal_rejects_invalid_source_name(source_name: str) -> None:
    payload = _valid_payload()
    payload["source_name"] = source_name

    with pytest.raises(ValueError, match="source_name"):
        parse_litigation_signal(payload)


def test_parse_litigation_signal_accepts_valid_source_name() -> None:
    payload = _valid_payload()
    payload["source_name"] = "court_dockets"

    result = parse_litigation_signal(payload)

    assert result["source_name"] == "court_dockets"


@pytest.mark.parametrize("field_name", ["case_id", "alleged_amount", "source_name"])
def test_parse_litigation_signal_rejects_missing_required_fields(field_name: str) -> None:
    payload = _valid_payload()
    payload.pop(field_name)

    with pytest.raises(ValueError, match=f"missing required field: {field_name}"):
        parse_litigation_signal(payload)


@pytest.mark.parametrize(
    "field_name,invalid_value",
    [
        ("case_id", 123),
        ("company_name", None),
        ("jurisdiction", ""),
        ("case_type", "   "),
        ("filing_date", 20260324),
        ("status", ""),
        ("plaintiff_role", "   "),
        ("defendant_role", []),
        ("source_name", 1),
    ],
)
def test_parse_litigation_signal_rejects_invalid_string_fields(
    field_name: str,
    invalid_value: object,
) -> None:
    payload = _valid_payload()
    payload[field_name] = invalid_value

    with pytest.raises(ValueError, match=f"{field_name} must be a non-empty string"):
        parse_litigation_signal(payload)


@pytest.mark.parametrize("invalid_value", ["1000", None, [], {}])
def test_parse_litigation_signal_rejects_non_numeric_alleged_amount(invalid_value: object) -> None:
    payload = _valid_payload()
    payload["alleged_amount"] = invalid_value

    with pytest.raises(ValueError, match="alleged_amount must be a numeric value"):
        parse_litigation_signal(payload)


def test_parse_litigation_signal_rejects_boolean_alleged_amount() -> None:
    payload = _valid_payload()
    payload["alleged_amount"] = True

    with pytest.raises(ValueError, match="alleged_amount must be a numeric value"):
        parse_litigation_signal(payload)


def test_parse_litigation_signal_ignores_extra_input_fields() -> None:
    payload = _valid_payload()
    payload["unexpected"] = "ignored"

    result = parse_litigation_signal(payload)

    assert "unexpected" not in result


def test_parse_litigation_signal_output_has_exact_required_keys_only() -> None:
    result = parse_litigation_signal(_valid_payload())

    assert set(result.keys()) == {
        "case_id",
        "company_name",
        "jurisdiction",
        "case_type",
        "filing_date",
        "status",
        "alleged_amount",
        "plaintiff_role",
        "defendant_role",
        "source_name",
    }


def test_parse_litigation_signal_rejects_non_dict_payload() -> None:
    with pytest.raises(ValueError, match="payload must be a dictionary"):
        parse_litigation_signal(["not", "a", "dict"])  # type: ignore[arg-type]


def test_parse_litigation_signal_output_is_independent_of_input_mutation() -> None:
    payload = _valid_payload()
    result = parse_litigation_signal(payload)

    payload["company_name"] = "Mutated Co"
    payload["alleged_amount"] = 0

    assert result["company_name"] == "Acme Construction LLC"
    assert result["alleged_amount"] == 1250000.5
