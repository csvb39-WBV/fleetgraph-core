"""
D16-MB2 Usage Metering Evaluator.

Deterministically validates and records request usage characteristics.

Pure in-memory Python with strict closed-schema validation.
No timestamps, no filesystem writes, no external dependencies, no mutation.
"""

from __future__ import annotations

from typing import Any

_REQUIRED_FIELDS: frozenset[str] = frozenset({
    "client_id",
    "request_id",
    "operation_type",
    "document_count",
    "data_processed_bytes",
})

_ALLOWED_OPERATION_TYPES: frozenset[str] = frozenset({
    "ingest",
    "retrieve",
    "reprocess",
})

_EXPECTED_OUTPUT_KEYS: tuple[str, ...] = (
    "status",
    "usage_record",
)

_EXPECTED_USAGE_RECORD_KEYS: tuple[str, ...] = (
    "client_id",
    "request_id",
    "operation_type",
    "document_count",
    "data_processed_bytes",
)


def _require_closed_schema(obj: dict[str, Any], required_fields: frozenset[str], label: str) -> None:
    present = set(obj.keys())

    missing = required_fields - present
    if missing:
        raise ValueError(f"{label} is missing required fields: {', '.join(sorted(missing))}")

    extra = present - required_fields
    if extra:
        raise ValueError(f"{label} contains unexpected fields: {', '.join(sorted(extra))}")


def evaluate_usage_metering(metering_input: dict[str, Any]) -> dict[str, Any]:
    """Validate and deterministically build a usage metering record."""
    if not isinstance(metering_input, dict):
        raise TypeError("metering_input must be a dict")

    _require_closed_schema(metering_input, _REQUIRED_FIELDS, "metering_input")

    client_id = metering_input["client_id"]
    if not isinstance(client_id, str):
        raise TypeError("metering_input field 'client_id' must be a str")
    if not client_id:
        raise ValueError("metering_input field 'client_id' must be a non-empty string")

    request_id = metering_input["request_id"]
    if not isinstance(request_id, str):
        raise TypeError("metering_input field 'request_id' must be a str")
    if not request_id:
        raise ValueError("metering_input field 'request_id' must be a non-empty string")

    operation_type = metering_input["operation_type"]
    if not isinstance(operation_type, str):
        raise TypeError("metering_input field 'operation_type' must be a str")
    if operation_type not in _ALLOWED_OPERATION_TYPES:
        raise ValueError(
            "metering_input field 'operation_type' must be one of "
            f"{sorted(_ALLOWED_OPERATION_TYPES)}"
        )

    document_count = metering_input["document_count"]
    if not isinstance(document_count, int) or isinstance(document_count, bool):
        raise TypeError("metering_input field 'document_count' must be an int")
    if document_count < 0:
        raise ValueError("metering_input field 'document_count' must not be negative")

    data_processed_bytes = metering_input["data_processed_bytes"]
    if not isinstance(data_processed_bytes, int) or isinstance(data_processed_bytes, bool):
        raise TypeError("metering_input field 'data_processed_bytes' must be an int")
    if data_processed_bytes < 0:
        raise ValueError("metering_input field 'data_processed_bytes' must not be negative")

    usage_record: dict[str, Any] = {
        "client_id": client_id,
        "request_id": request_id,
        "operation_type": operation_type,
        "document_count": document_count,
        "data_processed_bytes": data_processed_bytes,
    }

    if tuple(usage_record.keys()) != _EXPECTED_USAGE_RECORD_KEYS:
        raise RuntimeError("internal error: usage_record schema mismatch")

    response: dict[str, Any] = {
        "status": "recorded",
        "usage_record": usage_record,
    }

    if tuple(response.keys()) != _EXPECTED_OUTPUT_KEYS:
        raise RuntimeError("internal error: metering response schema mismatch")

    return response
