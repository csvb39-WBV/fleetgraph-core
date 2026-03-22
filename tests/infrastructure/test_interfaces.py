from __future__ import annotations

import importlib
import inspect
import subprocess
import sys
from collections.abc import Mapping
from pathlib import Path

import pytest


REPO_ROOT = Path(__file__).resolve().parents[2]
SRC_ROOT = REPO_ROOT / "src"

if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))


from fleetgraph_core import infrastructure
from fleetgraph_core.infrastructure.interfaces import (
    OBSERVABILITY_LEVEL_CRITICAL,
    OBSERVABILITY_LEVEL_DEBUG,
    OBSERVABILITY_LEVEL_ERROR,
    OBSERVABILITY_LEVEL_INFO,
    OBSERVABILITY_LEVEL_WARNING,
    SNAPSHOT_CONTENT_TYPE_BINARY,
    SNAPSHOT_CONTENT_TYPE_JSON,
    SNAPSHOT_CONTENT_TYPE_TEXT,
    SUPPORTED_OBSERVABILITY_LEVELS,
    SUPPORTED_SNAPSHOT_CONTENT_TYPES,
    ConfigProviderInterface,
    ObservabilityEmitterInterface,
    QueueConsumerInterface,
    QueuePublisherInterface,
    SchedulerTriggerInterface,
    SnapshotStorageInterface,
    validate_mapping,
    validate_non_empty_string,
    validate_observability_level,
    validate_snapshot_content_type,
)


def test_supported_snapshot_content_types_match_contract() -> None:
    assert set(SUPPORTED_SNAPSHOT_CONTENT_TYPES) == {
        SNAPSHOT_CONTENT_TYPE_TEXT,
        SNAPSHOT_CONTENT_TYPE_JSON,
        SNAPSHOT_CONTENT_TYPE_BINARY,
    }


def test_supported_observability_levels_match_contract() -> None:
    assert set(SUPPORTED_OBSERVABILITY_LEVELS) == {
        OBSERVABILITY_LEVEL_DEBUG,
        OBSERVABILITY_LEVEL_INFO,
        OBSERVABILITY_LEVEL_WARNING,
        OBSERVABILITY_LEVEL_ERROR,
        OBSERVABILITY_LEVEL_CRITICAL,
    }


def test_validate_non_empty_string_accepts_and_strips() -> None:
    assert validate_non_empty_string("  value  ", "field_name") == "value"


@pytest.mark.parametrize("value", [123, None, object()])
def test_validate_non_empty_string_rejects_non_string(value: object) -> None:
    with pytest.raises(ValueError, match="field_name must be a string"):
        validate_non_empty_string(value, "field_name")


def test_validate_non_empty_string_rejects_whitespace_only() -> None:
    with pytest.raises(ValueError, match="field_name must be a non-empty string"):
        validate_non_empty_string("   ", "field_name")


def test_validate_mapping_accepts_mapping() -> None:
    value = {"key": "value"}

    assert validate_mapping(value, "payload") is value


@pytest.mark.parametrize("value", [None, 123, "not-a-mapping", ["value"]])
def test_validate_mapping_rejects_invalid_values(value: object) -> None:
    with pytest.raises(ValueError, match="payload must be a mapping"):
        validate_mapping(value, "payload")


def test_validate_mapping_rejects_non_string_keys() -> None:
    with pytest.raises(ValueError, match="payload must have string keys"):
        validate_mapping({1: "value"}, "payload")


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("text", "text"),
        (" JSON ", "json"),
        ("binary", "binary"),
    ],
)
def test_validate_snapshot_content_type_accepts_supported_values(
    value: str,
    expected: str,
) -> None:
    assert validate_snapshot_content_type(value) == expected


@pytest.mark.parametrize("value", ["xml", "yaml", "  "])
def test_validate_snapshot_content_type_rejects_invalid_values(value: str) -> None:
    message = (
        "content_type must be a non-empty string"
        if not value.strip()
        else "content_type must be one of: binary, json, text"
    )

    with pytest.raises(ValueError, match=message):
        validate_snapshot_content_type(value)


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("debug", "debug"),
        (" INFO ", "info"),
        ("critical", "critical"),
    ],
)
def test_validate_observability_level_accepts_supported_values(
    value: str,
    expected: str,
) -> None:
    assert validate_observability_level(value) == expected


@pytest.mark.parametrize("value", ["trace", "notice", "  "])
def test_validate_observability_level_rejects_invalid_values(value: str) -> None:
    message = (
        "level must be a non-empty string"
        if not value.strip()
        else "level must be one of: critical, debug, error, info, warning"
    )

    with pytest.raises(ValueError, match=message):
        validate_observability_level(value)


@pytest.mark.parametrize(
    ("interface", "method_names"),
    [
        (SnapshotStorageInterface, {"store_snapshot", "get_snapshot"}),
        (QueuePublisherInterface, {"publish_message"}),
        (QueueConsumerInterface, {"consume_message", "acknowledge_message"}),
        (SchedulerTriggerInterface, {"parse_trigger"}),
        (ConfigProviderInterface, {"get_config", "get_required_value"}),
        (ObservabilityEmitterInterface, {"emit_event"}),
    ],
)
def test_interface_classes_are_abstract(
    interface: type[object],
    method_names: set[str],
) -> None:
    assert inspect.isabstract(interface)
    assert set(interface.__abstractmethods__) == method_names
    for method_name in method_names:
        assert callable(getattr(interface, method_name))


def test_interfaces_module_imports_without_aws_dependency() -> None:
    command = [
        sys.executable,
        "-c",
        (
            "import importlib, sys; "
            f"sys.path.insert(0, {str(SRC_ROOT)!r}); "
            "module = importlib.import_module('fleetgraph_core.infrastructure.interfaces'); "
            "print(module.__name__); "
            "print('boto3' in sys.modules)"
        ),
    ]
    result = subprocess.run(command, capture_output=True, text=True, check=True)

    assert result.stdout.splitlines() == [
        "fleetgraph_core.infrastructure.interfaces",
        "False",
    ]


def test_interfaces_module_exports_mapping_validator_as_mapping() -> None:
    mapping = validate_mapping({"payload": "value"}, "payload")

    assert isinstance(mapping, Mapping)


def test_infrastructure_package_exports_required_symbols() -> None:
    package = importlib.import_module("fleetgraph_core.infrastructure")
    exported_names = set(package.__all__)

    expected_names = {
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
    }

    assert exported_names == expected_names
    assert infrastructure.SnapshotStorageInterface is SnapshotStorageInterface
    assert infrastructure.validate_mapping is validate_mapping