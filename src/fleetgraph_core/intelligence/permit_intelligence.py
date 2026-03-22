"""Permit intelligence extraction from raw signal payloads."""

from __future__ import annotations

import json
from datetime import date, datetime
from xml.etree import ElementTree


ParsedSignal = dict[str, object]


def _validate_non_empty_string(value: object, field_name: str) -> str:
    if not isinstance(value, str):
        raise ValueError(f"{field_name} must be a non-empty string")

    normalized = value.strip()
    if not normalized:
        raise ValueError(f"{field_name} must be a non-empty string")

    return normalized


def _normalize_date(value: object, field_name: str, *, required: bool) -> str | None:
    if value is None:
        if required:
            raise ValueError(f"{field_name} is required")
        return None

    date_input = _validate_non_empty_string(value, field_name)

    try:
        if len(date_input) == 10:
            parsed_date = date.fromisoformat(date_input)
            return parsed_date.isoformat()

        parsed_datetime = datetime.fromisoformat(date_input.replace("Z", "+00:00"))
        return parsed_datetime.date().isoformat()
    except ValueError as error:
        raise ValueError(
            f"{field_name} must be a valid date in YYYY-MM-DD format"
        ) from error


def _element_to_mapping(element: ElementTree.Element) -> object:
    children = list(element)
    if not children:
        return (element.text or "").strip()

    output: dict[str, object] = {}
    for child in children:
        output[child.tag.strip().lower()] = _element_to_mapping(child)
    return output


def _parse_key_value_signal(raw_signal: str) -> ParsedSignal:
    normalized_signal = raw_signal.replace("\n", ";")
    segments = [segment.strip() for segment in normalized_signal.split(";")]
    output: ParsedSignal = {}

    for segment in segments:
        if not segment:
            continue
        if "=" in segment:
            key, value = segment.split("=", 1)
        elif ":" in segment:
            key, value = segment.split(":", 1)
        else:
            continue

        output[key.strip().lower()] = value.strip()

    return output


def _parse_raw_signal(raw_signal: object) -> ParsedSignal:
    normalized_signal = _validate_non_empty_string(raw_signal, "raw_signal")

    try:
        parsed_json = json.loads(normalized_signal)
    except json.JSONDecodeError:
        parsed_json = None

    if isinstance(parsed_json, dict):
        return parsed_json

    try:
        root = ElementTree.fromstring(normalized_signal)
    except ElementTree.ParseError:
        root = None

    if root is not None:
        parsed_xml = _element_to_mapping(root)
        if isinstance(parsed_xml, dict):
            return parsed_xml

    parsed_key_value = _parse_key_value_signal(normalized_signal)
    if parsed_key_value:
        return parsed_key_value

    raise ValueError("raw_signal format is unsupported")


def _find_field(mapping: ParsedSignal, field_names: tuple[str, ...]) -> object:
    for field_name in field_names:
        if field_name in mapping:
            return mapping[field_name]

    lower_lookup = {str(key).lower(): value for key, value in mapping.items()}
    for field_name in field_names:
        value = lower_lookup.get(field_name.lower())
        if value is not None:
            return value

    return None


def extract_permit_intelligence(raw_signal: object) -> dict[str, object]:
    """Extract normalized permit intelligence fields from raw signal payloads."""
    parsed_signal = _parse_raw_signal(raw_signal)

    permit_type_raw = _find_field(
        parsed_signal,
        ("permit_type", "permittype", "type", "permit"),
    )
    issuance_raw = _find_field(
        parsed_signal,
        ("issuance_date", "issue_date", "issued_on", "issued_at"),
    )
    expiration_raw = _find_field(
        parsed_signal,
        ("expiration_date", "expiry_date", "expires_on", "expires_at"),
    )

    location_raw = parsed_signal.get("location")
    city_raw = _find_field(parsed_signal, ("city",))
    state_raw = _find_field(parsed_signal, ("state",))

    if isinstance(location_raw, dict):
        city_raw = _find_field(location_raw, ("city",)) or city_raw
        state_raw = _find_field(location_raw, ("state",)) or state_raw

    permit_type = _validate_non_empty_string(permit_type_raw, "permit_type")
    city = _validate_non_empty_string(city_raw, "location.city")
    state = _validate_non_empty_string(state_raw, "location.state")
    issuance_date = _normalize_date(issuance_raw, "issuance_date", required=True)
    expiration_date = _normalize_date(
        expiration_raw,
        "expiration_date",
        required=False,
    )

    return {
        "permit_type": permit_type,
        "location": {
            "city": city,
            "state": state,
        },
        "issuance_date": issuance_date,
        "expiration_date": expiration_date,
    }


__all__ = ["extract_permit_intelligence"]