from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Optional


@dataclass(frozen=True)
class ExecutionRecord:
    execution_id: str
    environment: str
    status: str
    signal_topic: str


def _validate_non_empty_string(value: str, field_name: str) -> str:
    if not isinstance(value, str):
        raise ValueError(f"{field_name} must be a string")

    normalized_value = value.strip()
    if not normalized_value:
        raise ValueError(f"{field_name} must be a non-empty string")

    return normalized_value


class ExecutionRegistry:
    def __init__(self) -> None:
        self._records_by_id: Dict[str, ExecutionRecord] = {}
        self._ordered_execution_ids: List[str] = []

    def register(self, record: ExecutionRecord) -> ExecutionRecord:
        if not isinstance(record, ExecutionRecord):
            raise ValueError("record must be an ExecutionRecord")

        validated_record = ExecutionRecord(
            execution_id=_validate_non_empty_string(record.execution_id, "execution_id"),
            environment=_validate_non_empty_string(record.environment, "environment"),
            status=_validate_non_empty_string(record.status, "status"),
            signal_topic=_validate_non_empty_string(record.signal_topic, "signal_topic"),
        )

        if validated_record.execution_id in self._records_by_id:
            raise ValueError(
                f"Duplicate execution_id: {validated_record.execution_id}"
            )

        self._records_by_id[validated_record.execution_id] = validated_record
        self._ordered_execution_ids.append(validated_record.execution_id)
        return validated_record

    def get(self, execution_id: str) -> Optional[ExecutionRecord]:
        normalized_execution_id = _validate_non_empty_string(
            execution_id,
            "execution_id",
        )
        return self._records_by_id.get(normalized_execution_id)

    def count(self) -> int:
        return len(self._ordered_execution_ids)

    def list_records(self) -> list[ExecutionRecord]:
        return [
            self._records_by_id[execution_id]
            for execution_id in self._ordered_execution_ids
        ]


def build_execution_registry() -> ExecutionRegistry:
    return ExecutionRegistry()
