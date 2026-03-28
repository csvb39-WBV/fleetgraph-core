from __future__ import annotations


_GENERAL_EMAIL_PREFIXES = (
    "info",
    "contact",
    "admin",
    "hello",
    "support",
    "sales",
    "office",
)


def classify_email(email: str, *, website_domain: str | None = None) -> dict[str, object]:
    normalized_email = str(email or "").strip().lower()
    local_part, separator, domain_part = normalized_email.partition("@")
    normalized_domain = str(website_domain or "").strip().lower()
    is_general = separator == "@" and local_part in _GENERAL_EMAIL_PREFIXES
    return {
        "type": "general_email" if is_general else "direct_email",
        "confidence": "high",
        "is_direct": not is_general,
        "domain_match": separator == "@" and domain_part != "" and (normalized_domain == "" or domain_part == normalized_domain),
    }


def classify_phone(phone: str) -> dict[str, object]:
    normalized_phone = str(phone or "").strip()
    return {
        "type": "phone",
        "confidence": "medium" if normalized_phone != "" else "low",
        "is_direct": True,
        "domain_match": False,
    }
