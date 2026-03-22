from __future__ import annotations

from dataclasses import dataclass


SUPPORTED_ENVIRONMENTS = ("dev", "test", "prod")


@dataclass(frozen=True)
class ExecutionEnvelope:
    execution_id: str
    environment: str
    status: str
    signal_topic: str
    aws_region: str


def _validate_non_empty_string(value: str, field_name: str) -> str:
    if not isinstance(value, str):
        raise ValueError(f"{field_name} must be a string")

    normalized_value = value.strip()
    if not normalized_value:
        raise ValueError(f"{field_name} must be a non-empty string")

    return normalized_value


def _normalize_environment(environment: str) -> str:
    normalized_environment = _validate_non_empty_string(
        environment,
        "environment",
    ).lower()

    if normalized_environment not in SUPPORTED_ENVIRONMENTS:
        raise ValueError(
            "Unsupported environment: "
            f"{environment}. Supported environments: "
            + ", ".join(SUPPORTED_ENVIRONMENTS)
        )

    return normalized_environment


def build_execution_envelope(
    *,
    execution_id: str,
    environment: str,
    status: str,
    signal_topic: str,
    aws_region: str,
) -> ExecutionEnvelope:
    return ExecutionEnvelope(
        execution_id=_validate_non_empty_string(execution_id, "execution_id"),
        environment=_normalize_environment(environment),
        status=_validate_non_empty_string(status, "status"),
        signal_topic=_validate_non_empty_string(signal_topic, "signal_topic"),
        aws_region=_validate_non_empty_string(aws_region, "aws_region"),
    )