from __future__ import annotations

from datetime import date, datetime

__all__ = [
    "assign_signal_bucket",
    "days_since_event",
]


def _coerce_to_date(value: object, *, field_name: str) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    raise ValueError(f"{field_name} must be a date or datetime")


def days_since_event(event_date: object, reference_date: object) -> int:
    normalized_event_date = _coerce_to_date(event_date, field_name="event_date")
    normalized_reference_date = _coerce_to_date(reference_date, field_name="reference_date")
    delta_days = (normalized_reference_date - normalized_event_date).days
    if delta_days < 0:
        raise ValueError("event_date cannot be in the future relative to reference_date")
    return delta_days


def assign_signal_bucket(event_date: object, reference_date: object) -> str | None:
    signal_age_days = days_since_event(event_date, reference_date)
    if signal_age_days <= 7:
        return "T1"
    if signal_age_days <= 30:
        return "T2"
    if signal_age_days <= 365:
        return "T3A"
    if signal_age_days <= 730:
        return "T3B"
    if signal_age_days <= 1095:
        return "T3C"
    return None
