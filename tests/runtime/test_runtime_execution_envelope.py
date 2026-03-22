from __future__ import annotations

import sys
from pathlib import Path

import pytest


REPO_ROOT = Path(__file__).resolve().parents[2]
SRC_ROOT = REPO_ROOT / "src"

if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))


from fleetgraph_core.runtime.runtime_execution_envelope import (
    ExecutionEnvelope,
    build_execution_envelope,
)


def test_build_execution_envelope_returns_expected_object() -> None:
    envelope = build_execution_envelope(
        execution_id=" exec-001 ",
        environment=" DEV ",
        status=" pending ",
        signal_topic=" fleetgraph-topic ",
        aws_region=" us-east-1 ",
    )

    assert envelope == ExecutionEnvelope(
        execution_id="exec-001",
        environment="dev",
        status="pending",
        signal_topic="fleetgraph-topic",
        aws_region="us-east-1",
    )


def test_build_execution_envelope_rejects_blank_execution_id() -> None:
    with pytest.raises(ValueError, match="execution_id must be a non-empty string"):
        build_execution_envelope(
            execution_id="   ",
            environment="dev",
            status="pending",
            signal_topic="fleetgraph-topic",
            aws_region="us-east-1",
        )


def test_build_execution_envelope_rejects_blank_status() -> None:
    with pytest.raises(ValueError, match="status must be a non-empty string"):
        build_execution_envelope(
            execution_id="exec-001",
            environment="dev",
            status="   ",
            signal_topic="fleetgraph-topic",
            aws_region="us-east-1",
        )


def test_build_execution_envelope_rejects_blank_signal_topic() -> None:
    with pytest.raises(ValueError, match="signal_topic must be a non-empty string"):
        build_execution_envelope(
            execution_id="exec-001",
            environment="dev",
            status="pending",
            signal_topic="   ",
            aws_region="us-east-1",
        )


def test_build_execution_envelope_rejects_blank_aws_region() -> None:
    with pytest.raises(ValueError, match="aws_region must be a non-empty string"):
        build_execution_envelope(
            execution_id="exec-001",
            environment="dev",
            status="pending",
            signal_topic="fleetgraph-topic",
            aws_region="   ",
        )


def test_build_execution_envelope_rejects_invalid_environment() -> None:
    with pytest.raises(ValueError, match="Unsupported environment: stage"):
        build_execution_envelope(
            execution_id="exec-001",
            environment="stage",
            status="pending",
            signal_topic="fleetgraph-topic",
            aws_region="us-east-1",
        )


@pytest.mark.parametrize(
    ("field_name", "kwargs"),
    [
        (
            "execution_id",
            {
                "execution_id": 123,
                "environment": "dev",
                "status": "pending",
                "signal_topic": "fleetgraph-topic",
                "aws_region": "us-east-1",
            },
        ),
        (
            "environment",
            {
                "execution_id": "exec-001",
                "environment": 123,
                "status": "pending",
                "signal_topic": "fleetgraph-topic",
                "aws_region": "us-east-1",
            },
        ),
        (
            "status",
            {
                "execution_id": "exec-001",
                "environment": "dev",
                "status": 123,
                "signal_topic": "fleetgraph-topic",
                "aws_region": "us-east-1",
            },
        ),
        (
            "signal_topic",
            {
                "execution_id": "exec-001",
                "environment": "dev",
                "status": "pending",
                "signal_topic": 123,
                "aws_region": "us-east-1",
            },
        ),
        (
            "aws_region",
            {
                "execution_id": "exec-001",
                "environment": "dev",
                "status": "pending",
                "signal_topic": "fleetgraph-topic",
                "aws_region": 123,
            },
        ),
    ],
)
def test_build_execution_envelope_rejects_non_string_fields(
    field_name: str,
    kwargs: dict[str, object],
) -> None:
    with pytest.raises(ValueError, match=rf"{field_name} must be a string"):
        build_execution_envelope(**kwargs)  # type: ignore[arg-type]


def test_build_execution_envelope_is_deterministic() -> None:
    first = build_execution_envelope(
        execution_id=" exec-001 ",
        environment=" PROD ",
        status=" running ",
        signal_topic=" fleetgraph-topic ",
        aws_region=" us-west-2 ",
    )
    second = build_execution_envelope(
        execution_id="exec-001",
        environment="prod",
        status="running",
        signal_topic="fleetgraph-topic",
        aws_region="us-west-2",
    )

    assert first == second