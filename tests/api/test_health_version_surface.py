from __future__ import annotations

from datetime import datetime
import pathlib
import sys


PROJECT_ROOT = pathlib.Path(__file__).resolve().parents[2]
SRC_ROOT = PROJECT_ROOT / "src"

if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))


from fleetgraph_core.api.health_version_surface import (
    get_health_status,
    get_version_info,
)


def test_get_health_status_returns_exact_keys_and_constant_values() -> None:
    result = get_health_status()

    assert set(result.keys()) == {
        "status",
        "system",
        "timestamp",
    }
    assert result["status"] == "ok"
    assert result["system"] == "fleetgraph-core"


def test_get_health_status_timestamp_is_valid_iso_string() -> None:
    result = get_health_status()
    parsed = datetime.fromisoformat(result["timestamp"].replace("Z", "+00:00"))

    assert isinstance(result["timestamp"], str)
    assert parsed.tzinfo is not None


def test_get_health_status_has_deterministic_structure() -> None:
    first = get_health_status()
    second = get_health_status()

    assert list(first.keys()) == ["status", "system", "timestamp"]
    assert list(second.keys()) == ["status", "system", "timestamp"]
    assert first["status"] == second["status"] == "ok"
    assert first["system"] == second["system"] == "fleetgraph-core"


def test_get_version_info_returns_exact_values() -> None:
    assert get_version_info() == {
        "version": "1.0.0",
        "api_version": "v1",
        "build": "cti-w13",
    }


def test_get_version_info_repeated_calls_return_same_structure() -> None:
    first = get_version_info()
    second = get_version_info()

    assert first == second
