from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Mapping
from typing import TypeAlias, cast


SNAPSHOT_CONTENT_TYPE_TEXT = "text"
SNAPSHOT_CONTENT_TYPE_JSON = "json"
SNAPSHOT_CONTENT_TYPE_BINARY = "binary"

OBSERVABILITY_LEVEL_DEBUG = "debug"
OBSERVABILITY_LEVEL_INFO = "info"
OBSERVABILITY_LEVEL_WARNING = "warning"
OBSERVABILITY_LEVEL_ERROR = "error"
OBSERVABILITY_LEVEL_CRITICAL = "critical"

SUPPORTED_SNAPSHOT_CONTENT_TYPES = (
    SNAPSHOT_CONTENT_TYPE_BINARY,
    SNAPSHOT_CONTENT_TYPE_JSON,
    SNAPSHOT_CONTENT_TYPE_TEXT,
)
SUPPORTED_OBSERVABILITY_LEVELS = (
    OBSERVABILITY_LEVEL_CRITICAL,
    OBSERVABILITY_LEVEL_DEBUG,
    OBSERVABILITY_LEVEL_ERROR,
    OBSERVABILITY_LEVEL_INFO,
    OBSERVABILITY_LEVEL_WARNING,
)

SnapshotMetadata: TypeAlias = Mapping[str, object]
SnapshotRecord: TypeAlias = dict[str, object]
QueueMessage: TypeAlias = Mapping[str, object]
QueuePublishResult: TypeAlias = dict[str, object]
QueueConsumeResult: TypeAlias = dict[str, object]
SchedulerTrigger: TypeAlias = dict[str, object]
ObservabilityEvent: TypeAlias = dict[str, object]
ConfigMap: TypeAlias = dict[str, object]


def validate_non_empty_string(value: object, field_name: str) -> str:
    if not isinstance(value, str):
        raise ValueError(f"{field_name} must be a string")

    normalized_value = value.strip()
    if not normalized_value:
        raise ValueError(f"{field_name} must be a non-empty string")

    return normalized_value


def validate_mapping(value: object, field_name: str) -> Mapping[str, object]:
    if not isinstance(value, Mapping):
        raise ValueError(f"{field_name} must be a mapping")

    for key in value.keys():
        if not isinstance(key, str):
            raise ValueError(f"{field_name} must have string keys")

    return cast(Mapping[str, object], value)


def validate_snapshot_content_type(value: str) -> str:
    normalized_value = validate_non_empty_string(value, "content_type").lower()
    if normalized_value not in SUPPORTED_SNAPSHOT_CONTENT_TYPES:
        raise ValueError("content_type must be one of: binary, json, text")

    return normalized_value


def validate_observability_level(value: str) -> str:
    normalized_value = validate_non_empty_string(value, "level").lower()
    if normalized_value not in SUPPORTED_OBSERVABILITY_LEVELS:
        raise ValueError(
            "level must be one of: critical, debug, error, info, warning"
        )

    return normalized_value


class SnapshotStorageInterface(ABC):
    @abstractmethod
    def store_snapshot(
        self,
        *,
        snapshot_id: str,
        source_name: str,
        content_type: str,
        content: object,
        metadata: Mapping[str, object],
    ) -> SnapshotRecord:
        raise NotImplementedError

    @abstractmethod
    def get_snapshot(self, snapshot_id: str) -> SnapshotRecord:
        raise NotImplementedError


class QueuePublisherInterface(ABC):
    @abstractmethod
    def publish_message(
        self,
        *,
        queue_name: str,
        message: Mapping[str, object],
    ) -> QueuePublishResult:
        raise NotImplementedError


class QueueConsumerInterface(ABC):
    @abstractmethod
    def consume_message(
        self,
        *,
        queue_name: str,
        visibility_timeout_seconds: int | None = None,
    ) -> QueueConsumeResult:
        raise NotImplementedError

    @abstractmethod
    def acknowledge_message(
        self,
        *,
        queue_name: str,
        receipt_handle: str,
    ) -> None:
        raise NotImplementedError


class SchedulerTriggerInterface(ABC):
    @abstractmethod
    def parse_trigger(
        self,
        payload: Mapping[str, object],
    ) -> SchedulerTrigger:
        raise NotImplementedError


class ConfigProviderInterface(ABC):
    @abstractmethod
    def get_config(self) -> ConfigMap:
        raise NotImplementedError

    @abstractmethod
    def get_required_value(self, key: str) -> str:
        raise NotImplementedError


class ObservabilityEmitterInterface(ABC):
    @abstractmethod
    def emit_event(
        self,
        *,
        level: str,
        event_name: str,
        payload: Mapping[str, object],
    ) -> ObservabilityEvent:
        raise NotImplementedError


__all__ = [
    "SNAPSHOT_CONTENT_TYPE_TEXT",
    "SNAPSHOT_CONTENT_TYPE_JSON",
    "SNAPSHOT_CONTENT_TYPE_BINARY",
    "OBSERVABILITY_LEVEL_DEBUG",
    "OBSERVABILITY_LEVEL_INFO",
    "OBSERVABILITY_LEVEL_WARNING",
    "OBSERVABILITY_LEVEL_ERROR",
    "OBSERVABILITY_LEVEL_CRITICAL",
    "SUPPORTED_SNAPSHOT_CONTENT_TYPES",
    "SUPPORTED_OBSERVABILITY_LEVELS",
    "SnapshotMetadata",
    "SnapshotRecord",
    "QueueMessage",
    "QueuePublishResult",
    "QueueConsumeResult",
    "SchedulerTrigger",
    "ObservabilityEvent",
    "ConfigMap",
    "validate_snapshot_content_type",
    "validate_observability_level",
    "validate_non_empty_string",
    "validate_mapping",
    "SnapshotStorageInterface",
    "QueuePublisherInterface",
    "QueueConsumerInterface",
    "SchedulerTriggerInterface",
    "ConfigProviderInterface",
    "ObservabilityEmitterInterface",
]