"""
D16-MB1 FleetGraph CLI Entrypoint.

Deterministic in-memory CLI entrypoint for runtime operation invocation.

No filesystem writes, no external dependencies beyond the standard library,
no timestamps, no input mutation.
"""

from __future__ import annotations

from copy import deepcopy
from typing import Any

_REQUIRED_FIELDS: frozenset[str] = frozenset({
    "command",
    "payload",
})

_ALLOWED_COMMANDS: frozenset[str] = frozenset({
    "run",
    "validate",
    "status",
})

_EXPECTED_OUTPUT_KEYS: tuple[str, ...] = (
    "status",
    "result",
)


def _require_closed_schema(obj: dict[str, Any], required_fields: frozenset[str], label: str) -> None:
    present = set(obj.keys())

    missing = required_fields - present
    if missing:
        raise ValueError(f"{label} is missing required fields: {', '.join(sorted(missing))}")

    extra = present - required_fields
    if extra:
        raise ValueError(f"{label} contains unexpected fields: {', '.join(sorted(extra))}")


def invoke_cli_entrypoint(cli_input: dict[str, Any]) -> dict[str, Any]:
    """Invoke deterministic FleetGraph CLI command handler."""
    if not isinstance(cli_input, dict):
        raise TypeError("cli_input must be a dict")

    _require_closed_schema(cli_input, _REQUIRED_FIELDS, "cli_input")

    command = cli_input["command"]
    if not isinstance(command, str):
        raise TypeError("cli_input field 'command' must be a str")
    if command not in _ALLOWED_COMMANDS:
        raise ValueError(
            "cli_input field 'command' must be one of "
            f"{sorted(_ALLOWED_COMMANDS)}"
        )

    payload = cli_input["payload"]
    if not isinstance(payload, dict):
        raise TypeError("cli_input field 'payload' must be a dict")

    if command == "run":
        operation_result = {
            "command": "run",
            "operation": "runtime_execution_requested",
            "payload": deepcopy(payload),
        }
    elif command == "validate":
        operation_result = {
            "command": "validate",
            "operation": "runtime_validation_requested",
            "payload": deepcopy(payload),
        }
    else:
        operation_result = {
            "command": "status",
            "operation": "runtime_status_requested",
            "payload": deepcopy(payload),
        }

    response: dict[str, Any] = {
        "status": "success",
        "result": operation_result,
    }

    if tuple(response.keys()) != _EXPECTED_OUTPUT_KEYS:
        raise RuntimeError("internal error: cli response schema mismatch")

    return response
