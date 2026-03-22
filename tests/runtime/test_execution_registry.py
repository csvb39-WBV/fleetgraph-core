from __future__ import annotations

import sys
from pathlib import Path

import pytest


REPO_ROOT = Path(__file__).resolve().parents[2]
SRC_ROOT = REPO_ROOT / "src"

if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))


from fleetgraph_core.runtime.execution_registry import (
    ExecutionRecord,
    ExecutionRegistry,
    build_execution_registry,
)


def test_build_execution_registry_returns_empty_registry() -> None:
    registry = build_execution_registry()

    assert isinstance(registry, ExecutionRegistry)
    assert registry.count() == 0
    assert registry.list_records() == []


def test_register_stores_record_and_returns_validated_copy() -> None:
    registry = build_execution_registry()

    stored_record = registry.register(
        ExecutionRecord(
            execution_id=" exec-001 ",
            environment=" dev ",
            status=" pending ",
            signal_topic=" fleetgraph-topic ",
        )
    )

    assert stored_record == ExecutionRecord(
        execution_id="exec-001",
        environment="dev",
        status="pending",
        signal_topic="fleetgraph-topic",
    )
    assert registry.count() == 1
    assert registry.get("exec-001") == stored_record


def test_register_rejects_duplicate_execution_id() -> None:
    registry = build_execution_registry()

    registry.register(
        ExecutionRecord(
            execution_id="exec-001",
            environment="dev",
            status="pending",
            signal_topic="fleetgraph-topic",
        )
    )

    with pytest.raises(ValueError, match="Duplicate execution_id: exec-001"):
        registry.register(
            ExecutionRecord(
                execution_id="exec-001",
                environment="dev",
                status="running",
                signal_topic="fleetgraph-topic",
            )
        )


def test_get_returns_none_for_unknown_execution_id() -> None:
    registry = build_execution_registry()

    assert registry.get("missing-exec") is None


def test_list_records_preserves_insertion_order() -> None:
    registry = build_execution_registry()

    first = registry.register(
        ExecutionRecord(
            execution_id="exec-001",
            environment="dev",
            status="pending",
            signal_topic="topic-a",
        )
    )
    second = registry.register(
        ExecutionRecord(
            execution_id="exec-002",
            environment="test",
            status="running",
            signal_topic="topic-b",
        )
    )

    assert registry.list_records() == [first, second]


@pytest.mark.parametrize(
    ("field_name", "record"),
    [
        (
            "execution_id",
            ExecutionRecord(
                execution_id="   ",
                environment="dev",
                status="pending",
                signal_topic="topic-a",
            ),
        ),
        (
            "environment",
            ExecutionRecord(
                execution_id="exec-001",
                environment="   ",
                status="pending",
                signal_topic="topic-a",
            ),
        ),
        (
            "status",
            ExecutionRecord(
                execution_id="exec-001",
                environment="dev",
                status="   ",
                signal_topic="topic-a",
            ),
        ),
        (
            "signal_topic",
            ExecutionRecord(
                execution_id="exec-001",
                environment="dev",
                status="pending",
                signal_topic="   ",
            ),
        ),
    ],
)
def test_register_rejects_blank_required_fields(
    field_name: str,
    record: ExecutionRecord,
) -> None:
    registry = build_execution_registry()

    with pytest.raises(ValueError, match=rf"{field_name} must be a non-empty string"):
        registry.register(record)


def test_get_rejects_blank_execution_id() -> None:
    registry = build_execution_registry()

    with pytest.raises(ValueError, match="execution_id must be a non-empty string"):
        registry.get("   ")


def test_register_rejects_non_execution_record() -> None:
    registry = build_execution_registry()

    with pytest.raises(ValueError, match="record must be an ExecutionRecord"):
        registry.register(  # type: ignore[arg-type]
            {
                "execution_id": "exec-001",
                "environment": "dev",
                "status": "pending",
                "signal_topic": "topic-a",
            }
        )


def test_registry_is_deterministic() -> None:
    registry = build_execution_registry()

    first = registry.register(
        ExecutionRecord(
            execution_id="exec-001",
            environment="dev",
            status="pending",
            signal_topic="topic-a",
        )
    )
    second = registry.get("exec-001")

    assert first == second
    assert registry.count() == 1
