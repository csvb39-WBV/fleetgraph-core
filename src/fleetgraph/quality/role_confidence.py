from __future__ import annotations

from typing import Any

__all__ = [
    "score_role_confidence",
]

_HIGH_PATTERNS = (
    "owner",
    "ceo",
    "chief executive officer",
    "founder",
    "president",
)
_MEDIUM_PATTERNS = (
    "cfo",
    "chief financial officer",
    "controller",
    "director",
    "vice president",
    "vp",
)


def _normalize_title(value: object) -> str:
    if not isinstance(value, str):
        return ""
    return " ".join(value.strip().lower().split())


def score_role_confidence(title: object) -> dict[str, Any]:
    normalized_title = _normalize_title(title)
    if normalized_title == "":
        return {"role_confidence": "LOW", "role_score": 0.2}
    if any(pattern in normalized_title for pattern in _HIGH_PATTERNS):
        return {"role_confidence": "HIGH", "role_score": 1.0}
    if any(pattern in normalized_title for pattern in _MEDIUM_PATTERNS):
        return {"role_confidence": "MEDIUM", "role_score": 0.7}
    return {"role_confidence": "LOW", "role_score": 0.35}
