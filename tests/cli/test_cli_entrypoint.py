"""
Test suite for D16-MB1 FleetGraph CLI entrypoint.
"""

from __future__ import annotations

from copy import deepcopy

import pytest

from fleetgraph_core.cli.cli_entrypoint import invoke_cli_entrypoint


def make_valid_input(command: str = "run") -> dict:
    return {
        "command": command,
        "payload": {
            "job_id": "job_001",
            "dry_run": True,
        },
    }


class TestValidCommandExecution:
    @pytest.mark.parametrize("command,operation", [
        ("run", "runtime_execution_requested"),
        ("validate", "runtime_validation_requested"),
        ("status", "runtime_status_requested"),
    ])
    def test_valid_command_execution(self, command: str, operation: str) -> None:
        result = invoke_cli_entrypoint(make_valid_input(command))

        assert result == {
            "status": "success",
            "result": {
                "command": command,
                "operation": operation,
                "payload": {
                    "job_id": "job_001",
                    "dry_run": True,
                },
            },
        }


class TestInvalidCommandRejection:
    def test_invalid_command_rejection(self) -> None:
        payload = make_valid_input("unknown")

        with pytest.raises(ValueError, match="command.*must be one of"):
            invoke_cli_entrypoint(payload)


class TestOutputContract:
    def test_exact_output_key_order(self) -> None:
        result = invoke_cli_entrypoint(make_valid_input("run"))

        assert tuple(result.keys()) == ("status", "result")

    def test_result_is_dict(self) -> None:
        result = invoke_cli_entrypoint(make_valid_input("status"))

        assert isinstance(result["result"], dict)


class TestDeterminism:
    def test_deterministic_repeated_calls(self) -> None:
        payload = make_valid_input("validate")

        first = invoke_cli_entrypoint(payload)
        second = invoke_cli_entrypoint(payload)

        assert first == second


class TestInputImmutability:
    def test_input_not_mutated(self) -> None:
        payload = make_valid_input("run")
        before = deepcopy(payload)

        invoke_cli_entrypoint(payload)

        assert payload == before


class TestMalformedInputRejection:
    def test_missing_keys_rejected(self) -> None:
        payload = make_valid_input()
        del payload["payload"]

        with pytest.raises(ValueError, match="missing required fields"):
            invoke_cli_entrypoint(payload)

    def test_extra_keys_rejected(self) -> None:
        payload = make_valid_input()
        payload["extra"] = "x"

        with pytest.raises(ValueError, match="unexpected fields"):
            invoke_cli_entrypoint(payload)

    def test_command_not_string_rejected(self) -> None:
        payload = make_valid_input()
        payload["command"] = 5

        with pytest.raises(TypeError, match="command.*str"):
            invoke_cli_entrypoint(payload)

    def test_payload_not_dict_rejected(self) -> None:
        payload = make_valid_input()
        payload["payload"] = ["not", "dict"]

        with pytest.raises(TypeError, match="payload.*dict"):
            invoke_cli_entrypoint(payload)
