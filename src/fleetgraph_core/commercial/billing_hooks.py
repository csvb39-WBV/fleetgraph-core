from __future__ import annotations

from copy import deepcopy
from typing import Any

_REQUIRED_TOP_KEYS: frozenset[str] = frozenset({"usage_record", "billing_enabled"})

_REQUIRED_USAGE_KEYS: frozenset[str] = frozenset(
    {"client_id", "request_id", "operation_type", "document_count", "data_processed_bytes"}
)

_ALLOWED_OPERATION_TYPES: frozenset[str] = frozenset({"ingest", "retrieve", "reprocess"})


def _validate_top_level(hook_input: Any) -> None:
    if not isinstance(hook_input, dict):
        raise TypeError("hook_input must be a dict")
    missing = _REQUIRED_TOP_KEYS - hook_input.keys()
    extra = hook_input.keys() - _REQUIRED_TOP_KEYS
    if missing:
        raise ValueError(f"hook_input missing keys: {sorted(missing)}")
    if extra:
        raise ValueError(f"hook_input has extra keys: {sorted(extra)}")


def _validate_billing_enabled(billing_enabled: Any) -> None:
    if not isinstance(billing_enabled, bool):
        raise TypeError("billing_enabled must be a bool")


def _validate_usage_record(usage_record: Any) -> None:
    if not isinstance(usage_record, dict):
        raise TypeError("usage_record must be a dict")
    missing = _REQUIRED_USAGE_KEYS - usage_record.keys()
    extra = usage_record.keys() - _REQUIRED_USAGE_KEYS
    if missing:
        raise ValueError(f"usage_record missing keys: {sorted(missing)}")
    if extra:
        raise ValueError(f"usage_record has extra keys: {sorted(extra)}")

    client_id = usage_record["client_id"]
    if not isinstance(client_id, str) or not client_id:
        raise ValueError("client_id must be a non-empty string")

    request_id = usage_record["request_id"]
    if not isinstance(request_id, str) or not request_id:
        raise ValueError("request_id must be a non-empty string")

    operation_type = usage_record["operation_type"]
    if operation_type not in _ALLOWED_OPERATION_TYPES:
        raise ValueError(
            f"operation_type must be one of {sorted(_ALLOWED_OPERATION_TYPES)}"
        )

    document_count = usage_record["document_count"]
    if isinstance(document_count, bool) or not isinstance(document_count, int):
        raise TypeError("document_count must be an int")
    if document_count < 0:
        raise ValueError("document_count must be non-negative")

    data_processed_bytes = usage_record["data_processed_bytes"]
    if isinstance(data_processed_bytes, bool) or not isinstance(data_processed_bytes, int):
        raise TypeError("data_processed_bytes must be an int")
    if data_processed_bytes < 0:
        raise ValueError("data_processed_bytes must be non-negative")


def _compute_billable_units(operation_type: str, document_count: int) -> int:
    if operation_type == "retrieve":
        return 1
    return document_count


def evaluate_billing_hook(hook_input: Any) -> dict[str, Any]:
    _validate_top_level(hook_input)
    _validate_billing_enabled(hook_input["billing_enabled"])
    _validate_usage_record(hook_input["usage_record"])

    usage_record = deepcopy(hook_input["usage_record"])
    billing_enabled: bool = hook_input["billing_enabled"]

    billable_units = _compute_billable_units(
        usage_record["operation_type"], usage_record["document_count"]
    )

    billing_event: dict[str, Any] = {
        "client_id": usage_record["client_id"],
        "request_id": usage_record["request_id"],
        "operation_type": usage_record["operation_type"],
        "billable_units": billable_units,
    }

    if billing_enabled:
        status = "billable"
        reasons = ["billing_enabled"]
    else:
        status = "non_billable"
        reasons = ["billing_disabled"]

    return {
        "status": status,
        "billing_event": billing_event,
        "reasons": reasons,
    }
