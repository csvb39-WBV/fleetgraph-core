from __future__ import annotations

from copy import deepcopy

import pytest

from fleetgraph_core.commercial.billing_hooks import evaluate_billing_hook
from fleetgraph_core.commercial.usage_metering import evaluate_usage_metering
from fleetgraph_core.runtime.runtime_commercial_orchestrator import (
    build_runtime_commercial_orchestration,
)


def make_valid_input() -> dict[str, object]:
    return {
        "usage_metering_input": {
            "client_id": "client_001",
            "request_id": "req_001",
            "operation_type": "ingest",
            "document_count": 12,
            "data_processed_bytes": 4096,
        },
        "billing_hooks_input": {
            "usage_record": {
                "client_id": "ignored_client",
                "request_id": "ignored_request",
                "operation_type": "retrieve",
                "document_count": 1,
                "data_processed_bytes": 1,
            },
            "billing_enabled": True,
        },
    }


class TestSuccessfulPath:
    def test_successful_commercial_orchestration_path(self) -> None:
        result = build_runtime_commercial_orchestration(make_valid_input())

        assert result == {
            "status": "completed",
            "stage": "complete",
            "reasons": ["commercial_pipeline_completed"],
            "result": {
                "usage_record": {
                    "client_id": "client_001",
                    "request_id": "req_001",
                    "operation_type": "ingest",
                    "document_count": 12,
                    "data_processed_bytes": 4096,
                },
                "billing_event": {
                    "client_id": "client_001",
                    "request_id": "req_001",
                    "operation_type": "ingest",
                    "billable_units": 12,
                },
            },
        }


class TestOutputContract:
    def test_exact_output_key_order_top_and_result_levels(self) -> None:
        result = build_runtime_commercial_orchestration(make_valid_input())

        assert tuple(result.keys()) == ("status", "stage", "reasons", "result")
        assert tuple(result["result"].keys()) == ("usage_record", "billing_event")


class TestDeterminismAndImmutability:
    def test_deterministic_repeated_calls(self) -> None:
        payload = make_valid_input()

        first = build_runtime_commercial_orchestration(payload)
        second = build_runtime_commercial_orchestration(payload)

        assert first == second

    def test_input_not_mutated(self) -> None:
        payload = make_valid_input()
        before = deepcopy(payload)

        build_runtime_commercial_orchestration(payload)

        assert payload == before


class TestTopLevelMalformedInputRejection:
    def test_missing_top_level_keys_rejected(self) -> None:
        payload = make_valid_input()
        del payload["billing_hooks_input"]

        with pytest.raises(ValueError, match="missing required fields"):
            build_runtime_commercial_orchestration(payload)

    def test_extra_top_level_keys_rejected(self) -> None:
        payload = make_valid_input()
        payload["unexpected"] = {}

        with pytest.raises(ValueError, match="unexpected fields"):
            build_runtime_commercial_orchestration(payload)

    @pytest.mark.parametrize(
        "field,bad_value",
        [
            ("usage_metering_input", []),
            ("billing_hooks_input", "bad"),
        ],
    )
    def test_top_level_inputs_must_be_dicts(
        self,
        field: str,
        bad_value: object,
    ) -> None:
        payload = make_valid_input()
        payload[field] = bad_value

        with pytest.raises(TypeError, match=f"field '{field}'.*dict"):
            build_runtime_commercial_orchestration(payload)


class TestDelegatedOutputPreservation:
    def test_delegated_outputs_preserved_exactly(self) -> None:
        payload = make_valid_input()

        result = build_runtime_commercial_orchestration(payload)

        expected_usage_result = evaluate_usage_metering(
            deepcopy(payload["usage_metering_input"])
        )
        expected_billing_result = evaluate_billing_hook(
            {
                "usage_record": deepcopy(expected_usage_result["usage_record"]),
                "billing_enabled": payload["billing_hooks_input"]["billing_enabled"],
            }
        )

        assert result["status"] == "completed"
        assert result["stage"] == "complete"
        assert result["reasons"] == ["commercial_pipeline_completed"]
        assert result["result"]["usage_record"] == expected_usage_result["usage_record"]
        assert result["result"]["billing_event"] == expected_billing_result["billing_event"]
