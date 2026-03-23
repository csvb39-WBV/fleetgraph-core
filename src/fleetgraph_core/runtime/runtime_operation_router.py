from __future__ import annotations


_ALLOWED_OPERATION_TYPES = ("ingest", "retrieve", "reprocess", "status")
_REQUIRED_INPUT_KEYS = ("operation_type",)


def _validate_operation_router_input(payload: dict[str, object]) -> str:
    if set(payload.keys()) != set(_REQUIRED_INPUT_KEYS):
        raise ValueError("payload must include exactly: operation_type")

    operation_type = payload["operation_type"]
    if not isinstance(operation_type, str):
        raise ValueError("operation_type must be a string")
    if operation_type not in _ALLOWED_OPERATION_TYPES:
        raise ValueError("operation_type must be one of: ingest, retrieve, reprocess, status")

    return operation_type


def route_runtime_operation(payload: dict[str, object]) -> dict[str, object]:
    if not isinstance(payload, dict):
        raise ValueError("payload must be a dict")

    operation_type = _validate_operation_router_input(payload)

    return {
        "route": operation_type,
        "reasons": [f"{operation_type}_route_selected"],
    }