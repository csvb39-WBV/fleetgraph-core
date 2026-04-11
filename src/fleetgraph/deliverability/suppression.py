from __future__ import annotations

import re
from copy import deepcopy
from typing import Any

__all__ = [
    "is_suppressed",
    "is_valid_contact_email",
    "normalize_suppression_list",
]

_ALLOWED_REASONS = {
    "BOUNCED",
    "UNSUBSCRIBED",
    "MANUAL",
}
_EMAIL_PATTERN = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
_DISPOSABLE_PATTERNS = (
    "mailinator",
    "guerrillamail",
    "tempmail",
    "10minutemail",
    "yopmail",
    "trashmail",
)


def _normalize_non_empty_string(value: object, *, field_name: str) -> str:
    if not isinstance(value, str):
        raise ValueError(f"{field_name} must be a non-empty string")
    normalized_value = value.strip()
    if normalized_value == "":
        raise ValueError(f"{field_name} must be a non-empty string")
    return normalized_value


def _normalize_email(email: object, *, field_name: str) -> str:
    normalized_email = _normalize_non_empty_string(email, field_name=field_name).lower()
    return normalized_email


def is_valid_contact_email(email: object, *, disposable_patterns: tuple[str, ...] | None = None) -> bool:
    if not isinstance(email, str):
        return False
    normalized_email = email.strip().lower()
    if normalized_email == "":
        return False
    if _EMAIL_PATTERN.match(normalized_email) is None:
        return False
    patterns = tuple(disposable_patterns or _DISPOSABLE_PATTERNS)
    if any(pattern in normalized_email for pattern in patterns):
        return False
    return True


def normalize_suppression_list(suppression_records: list[object]) -> list[dict[str, Any]]:
    if not isinstance(suppression_records, list):
        raise ValueError("suppression_records must be a list")

    normalized_by_email: dict[str, dict[str, Any]] = {}
    for index, record in enumerate(suppression_records):
        if not isinstance(record, dict):
            raise ValueError(f"suppression_records[{index}] must be a dict")
        email = _normalize_email(record.get("email"), field_name=f"suppression_records[{index}].email")
        reason = _normalize_non_empty_string(
            record.get("reason"),
            field_name=f"suppression_records[{index}].reason",
        ).upper()
        if reason not in _ALLOWED_REASONS:
            raise ValueError(f"suppression_records[{index}].reason is invalid")
        normalized_by_email[email] = {
            "email": email,
            "reason": reason,
        }

    normalized_records = list(normalized_by_email.values())
    normalized_records.sort(key=lambda row: str(row["email"]))
    return deepcopy(normalized_records)


def is_suppressed(email: object, suppression_records: list[object]) -> bool:
    if not isinstance(email, str):
        return False
    normalized_email = email.strip().lower()
    if normalized_email == "":
        return False
    normalized_suppression_records = normalize_suppression_list(suppression_records)
    return any(str(record["email"]) == normalized_email for record in normalized_suppression_records)
