from __future__ import annotations

from fleetgraph_core.security.api_key_auth import evaluate_api_key_auth
from fleetgraph_core.security.rate_limiting import evaluate_rate_limiting
from fleetgraph_core.security.request_validation import evaluate_request_validation


_REQUIRED_INPUT_KEYS = (
    "auth_input",
    "validation_input",
    "rate_limit_input",
)


def _validate_orchestrator_input(payload: dict[str, object]) -> None:
    if set(payload.keys()) != set(_REQUIRED_INPUT_KEYS):
        raise ValueError(
            "payload must include exactly: auth_input, validation_input, rate_limit_input"
        )

    if not isinstance(payload["auth_input"], dict):
        raise ValueError("auth_input must be a dict")
    if not isinstance(payload["validation_input"], dict):
        raise ValueError("validation_input must be a dict")
    if not isinstance(payload["rate_limit_input"], dict):
        raise ValueError("rate_limit_input must be a dict")


def orchestrate_runtime_security(payload: dict[str, object]) -> dict[str, object]:
    if not isinstance(payload, dict):
        raise ValueError("payload must be a dict")

    _validate_orchestrator_input(payload)

    auth_result = evaluate_api_key_auth(payload["auth_input"])
    if auth_result["status"] == "unauthorized":
        return {
            "status": "stop",
            "stage": "auth",
            "reasons": list(auth_result["reasons"]),
        }

    validation_result = evaluate_request_validation(payload["validation_input"])
    if validation_result["status"] == "invalid":
        return {
            "status": "stop",
            "stage": "validation",
            "reasons": list(validation_result["reasons"]),
        }

    rate_limit_result = evaluate_rate_limiting(payload["rate_limit_input"])
    if rate_limit_result["status"] == "reject":
        return {
            "status": "stop",
            "stage": "rate_limit",
            "reasons": list(rate_limit_result["reasons"]),
        }

    return {
        "status": "continue",
        "stage": "complete",
        "reasons": ["security_checks_passed"],
    }