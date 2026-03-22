from __future__ import annotations

import pathlib
import sys

import pytest


PROJECT_ROOT = pathlib.Path(__file__).resolve().parents[2]
SRC_ROOT = PROJECT_ROOT / "src"

if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))


from fleetgraph_core.intelligence.permit_intelligence import extract_permit_intelligence


def test_extract_permit_intelligence_from_json() -> None:
    raw_signal = """
    {
      "permit_type": "Electrical",
      "location": {"city": "Austin", "state": "TX"},
      "issuance_date": "2026-03-20",
      "expiration_date": "2026-12-31"
    }
    """

    result = extract_permit_intelligence(raw_signal)

    assert result == {
        "permit_type": "Electrical",
        "location": {
            "city": "Austin",
            "state": "TX",
        },
        "issuance_date": "2026-03-20",
        "expiration_date": "2026-12-31",
    }


def test_extract_permit_intelligence_from_xml_without_expiration() -> None:
    raw_signal = """
    <permit>
      <permit_type>Plumbing</permit_type>
      <location>
        <city>Denver</city>
        <state>CO</state>
      </location>
      <issuance_date>2026-03-21T10:30:00Z</issuance_date>
    </permit>
    """

    result = extract_permit_intelligence(raw_signal)

    assert result == {
        "permit_type": "Plumbing",
        "location": {
            "city": "Denver",
            "state": "CO",
        },
        "issuance_date": "2026-03-21",
        "expiration_date": None,
    }


def test_extract_permit_intelligence_from_key_value_signal() -> None:
    raw_signal = (
        "permit_type=Roofing; city=Phoenix; state=AZ; "
        "issuance_date=2026-01-15; expiration_date=2026-06-15"
    )

    result = extract_permit_intelligence(raw_signal)

    assert result == {
        "permit_type": "Roofing",
        "location": {
            "city": "Phoenix",
            "state": "AZ",
        },
        "issuance_date": "2026-01-15",
        "expiration_date": "2026-06-15",
    }


def test_extract_permit_intelligence_rejects_unsupported_format() -> None:
    with pytest.raises(ValueError, match="raw_signal format is unsupported"):
        extract_permit_intelligence("just some random unstructured text")


def test_extract_permit_intelligence_rejects_missing_permit_type() -> None:
    raw_signal = '{"location": {"city": "Austin", "state": "TX"}, "issuance_date": "2026-03-20"}'

    with pytest.raises(ValueError, match="permit_type must be a non-empty string"):
        extract_permit_intelligence(raw_signal)


def test_extract_permit_intelligence_rejects_missing_location_city() -> None:
    raw_signal = '{"permit_type": "Electrical", "location": {"state": "TX"}, "issuance_date": "2026-03-20"}'

    with pytest.raises(ValueError, match="location.city must be a non-empty string"):
        extract_permit_intelligence(raw_signal)


def test_extract_permit_intelligence_rejects_missing_location_state() -> None:
    raw_signal = '{"permit_type": "Electrical", "location": {"city": "Austin"}, "issuance_date": "2026-03-20"}'

    with pytest.raises(ValueError, match="location.state must be a non-empty string"):
        extract_permit_intelligence(raw_signal)


def test_extract_permit_intelligence_rejects_missing_issuance_date() -> None:
    raw_signal = '{"permit_type": "Electrical", "location": {"city": "Austin", "state": "TX"}}'

    with pytest.raises(ValueError, match="issuance_date is required"):
        extract_permit_intelligence(raw_signal)


def test_extract_permit_intelligence_rejects_invalid_date_format() -> None:
    raw_signal = '{"permit_type": "Electrical", "location": {"city": "Austin", "state": "TX"}, "issuance_date": "03/20/2026"}'

    with pytest.raises(
        ValueError,
        match="issuance_date must be a valid date in YYYY-MM-DD format",
    ):
        extract_permit_intelligence(raw_signal)


def test_extract_permit_intelligence_is_deterministic() -> None:
    raw_signal = '{"permit_type": "Electrical", "location": {"city": "Austin", "state": "TX"}, "issuance_date": "2026-03-20", "expiration_date": "2026-12-31"}'

    first = extract_permit_intelligence(raw_signal)
    second = extract_permit_intelligence(raw_signal)

    assert first == second