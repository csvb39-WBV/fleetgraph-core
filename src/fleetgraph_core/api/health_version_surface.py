from __future__ import annotations

from datetime import datetime, timezone


def get_health_status() -> dict[str, object]:
    return {
        "status": "ok",
        "system": "fleetgraph-core",
        "timestamp": datetime.now(timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z"),
    }


def get_version_info() -> dict[str, object]:
    return {
        "version": "1.0.0",
        "api_version": "v1",
        "build": "cti-w13",
    }
