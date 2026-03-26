from __future__ import annotations

from copy import deepcopy

from fleetgraph_core.api.single_record_endpoint import handle_single_record_request


def _validate_non_empty_string(value: object, field_name: str) -> str:
    if not isinstance(value, str):
        raise ValueError(f"{field_name} must be a non-empty string.")

    if value.strip() == "":
        raise ValueError(f"{field_name} must be a non-empty string.")

    return value


def _validate_batch_request(batch_request: dict[str, object]) -> tuple[str, str, list[dict[str, object]]]:
    if not isinstance(batch_request, dict):
        raise ValueError("batch_request must be a dictionary.")

    required_keys = {
        "request_id",
        "endpoint_id",
        "records",
    }
    if set(batch_request.keys()) != required_keys:
        raise ValueError("batch_request must contain exactly the required keys.")

    request_id = _validate_non_empty_string(batch_request["request_id"], "request_id")
    endpoint_id = _validate_non_empty_string(batch_request["endpoint_id"], "endpoint_id")
    records = batch_request["records"]

    if not isinstance(records, list):
        raise ValueError("records must be a list.")
    if not records:
        raise ValueError("records must contain at least one item.")

    validated_records: list[dict[str, object]] = []
    for index, record in enumerate(records):
        if not isinstance(record, dict):
            raise ValueError(f"records[{index}] must be a dictionary.")
        validated_records.append(record)

    return request_id, endpoint_id, validated_records


def _build_batch_state(success_count: int, failure_count: int) -> str:
    if failure_count == 0:
        return "completed"
    if success_count == 0:
        return "failed"
    return "partial_failure"


def apply_batch_endpoint_request(batch_request: dict[str, object]) -> dict[str, object]:
    request_id, endpoint_id, records = _validate_batch_request(batch_request)

    results: list[dict[str, object]] = []
    success_count = 0
    failure_count = 0

    for record in records:
        single_record_request = {
            "response_type": endpoint_id,
            "record": deepcopy(record),
            "limit": None,
            "minimum_priority": None,
        }
        result = handle_single_record_request(single_record_request)
        if result["ok"] is True:
            success_count += 1
        else:
            failure_count += 1

        results.append(deepcopy(result))

    return {
        "request_id": request_id,
        "endpoint_id": endpoint_id,
        "batch_state": _build_batch_state(success_count, failure_count),
        "results": results,
        "record_count": len(records),
        "success_count": success_count,
        "failure_count": failure_count,
    }

