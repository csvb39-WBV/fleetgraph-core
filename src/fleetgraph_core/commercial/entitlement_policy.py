from __future__ import annotations

from copy import deepcopy
from typing import Any

_REQUIRED_TOP_KEYS: frozenset[str] = frozenset(
    {"client_id", "operation_type", "subscription_tier", "limits"}
)
_REQUIRED_LIMIT_KEYS: frozenset[str] = frozenset(
    {"ingest_allowed", "retrieve_allowed", "reprocess_allowed"}
)
_ALLOWED_OPERATION_TYPES: frozenset[str] = frozenset(
    {"ingest", "retrieve", "reprocess"}
)
_ALLOWED_SUBSCRIPTION_TIERS: frozenset[str] = frozenset(
    {"basic", "pro", "enterprise"}
)


def _validate_closed_schema(payload: dict[str, Any], required: frozenset[str], label: str) -> None:
    missing = required - payload.keys()
    extra = payload.keys() - required

    if missing:
        raise ValueError(f"{label} missing keys: {sorted(missing)}")
    if extra:
        raise ValueError(f"{label} has extra keys: {sorted(extra)}")


def _validate_input(policy_input: Any) -> None:
    if not isinstance(policy_input, dict):
        raise TypeError("policy_input must be a dict")

    _validate_closed_schema(policy_input, _REQUIRED_TOP_KEYS, "policy_input")

    client_id = policy_input["client_id"]
    if not isinstance(client_id, str) or not client_id:
        raise ValueError("client_id must be a non-empty string")

    operation_type = policy_input["operation_type"]
    if operation_type not in _ALLOWED_OPERATION_TYPES:
        raise ValueError(
            "operation_type must be one of "
            f"{sorted(_ALLOWED_OPERATION_TYPES)}"
        )

    subscription_tier = policy_input["subscription_tier"]
    if subscription_tier not in _ALLOWED_SUBSCRIPTION_TIERS:
        raise ValueError(
            "subscription_tier must be one of "
            f"{sorted(_ALLOWED_SUBSCRIPTION_TIERS)}"
        )

    limits = policy_input["limits"]
    if not isinstance(limits, dict):
        raise TypeError("limits must be a dict")

    _validate_closed_schema(limits, _REQUIRED_LIMIT_KEYS, "limits")

    for key in ("ingest_allowed", "retrieve_allowed", "reprocess_allowed"):
        if not isinstance(limits[key], bool):
            raise TypeError(f"limits field '{key}' must be a bool")


def evaluate_entitlement_policy(policy_input: Any) -> dict[str, Any]:
    _validate_input(policy_input)

    operation_type: str = policy_input["operation_type"]
    limits: dict[str, bool] = deepcopy(policy_input["limits"])

    allowed_by_operation = {
        "ingest": limits["ingest_allowed"],
        "retrieve": limits["retrieve_allowed"],
        "reprocess": limits["reprocess_allowed"],
    }

    if allowed_by_operation[operation_type]:
        status = "allowed"
        reasons = ["operation_allowed"]
    else:
        status = "denied"
        reasons = [f"{operation_type}_not_allowed"]

    return {
        "status": status,
        "reasons": reasons,
    }
