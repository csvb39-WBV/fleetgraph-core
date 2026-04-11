from __future__ import annotations

import re
from typing import Any

__all__ = [
    "validate_contact_email",
]

_EMAIL_PATTERN = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
_DISPOSABLE_DOMAINS = {
    "mailinator.com",
    "guerrillamail.com",
    "tempmail.com",
    "10minutemail.com",
    "yopmail.com",
    "trashmail.com",
}
_ROLE_BASED_LOCAL_PARTS = {
    "admin",
    "billing",
    "careers",
    "contact",
    "hello",
    "hr",
    "info",
    "office",
    "sales",
    "support",
    "team",
}
_JUNK_LOCAL_PATTERNS = (
    "test",
    "asdf",
    "qwerty",
    "noreply",
    "no-reply",
    "donotreply",
)


def _normalize_email(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    normalized_value = value.strip().lower()
    if normalized_value == "":
        return None
    return normalized_value


def validate_contact_email(email: object) -> dict[str, Any]:
    normalized_email = _normalize_email(email)
    if normalized_email is None:
        return {"email": "", "is_valid": False, "reason": "blank_email"}
    if _EMAIL_PATTERN.match(normalized_email) is None:
        return {"email": normalized_email, "is_valid": False, "reason": "invalid_format"}

    local_part, _, domain = normalized_email.partition("@")
    if domain in _DISPOSABLE_DOMAINS:
        return {"email": normalized_email, "is_valid": False, "reason": "disposable_domain"}
    if local_part in _ROLE_BASED_LOCAL_PARTS:
        return {"email": normalized_email, "is_valid": False, "reason": "role_based_email"}
    if any(pattern in local_part for pattern in _JUNK_LOCAL_PATTERNS):
        return {"email": normalized_email, "is_valid": False, "reason": "junk_pattern"}
    if ".." in normalized_email or local_part.startswith(".") or local_part.endswith("."):
        return {"email": normalized_email, "is_valid": False, "reason": "malformed_edge_case"}
    return {"email": normalized_email, "is_valid": True, "reason": "valid_email"}
