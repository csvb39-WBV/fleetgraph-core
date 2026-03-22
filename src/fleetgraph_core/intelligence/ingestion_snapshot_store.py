"""Immutable snapshot store for deterministic ingestion boundaries."""

from __future__ import annotations

from dataclasses import dataclass
from hashlib import sha256
from typing import Callable
from uuid import UUID, uuid4


def _validate_non_empty_string(value: object, field_name: str) -> str:
    if not isinstance(value, str):
        raise ValueError(f"{field_name} must be a non-empty string")

    normalized = value.strip()
    if not normalized:
        raise ValueError(f"{field_name} must be a non-empty string")

    return normalized


def _build_content_hash(raw_content: str) -> str:
    return sha256(raw_content.encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class StoredSnapshot:
    snapshot_id: str
    company_id: str
    source: str
    raw_content: str
    retrieved_at: str
    content_hash: str
    version: int


class IngestionSnapshotStore:
    """Store immutable snapshots keyed by a unique snapshot_id."""

    def __init__(self, snapshot_id_factory: Callable[[], UUID] | None = None) -> None:
        self._snapshot_id_factory = snapshot_id_factory or uuid4
        self._snapshots_by_id: dict[str, StoredSnapshot] = {}
        self._version_by_key: dict[tuple[str, str], int] = {}

    def store_snapshot(
        self,
        *,
        company_id: object,
        source: object,
        raw_content: object,
        retrieved_at: object,
    ) -> dict[str, object]:
        normalized_company_id = _validate_non_empty_string(company_id, "company_id")
        normalized_source = _validate_non_empty_string(source, "source")
        normalized_raw_content = _validate_non_empty_string(raw_content, "raw_content")
        normalized_retrieved_at = _validate_non_empty_string(retrieved_at, "retrieved_at")

        snapshot_id = str(self._snapshot_id_factory())
        if snapshot_id in self._snapshots_by_id:
            raise ValueError(f"snapshot_id already exists: {snapshot_id}")

        content_hash = _build_content_hash(normalized_raw_content)
        source_key = (normalized_company_id, normalized_source)
        next_version = self._version_by_key.get(source_key, 0) + 1

        self._snapshots_by_id[snapshot_id] = StoredSnapshot(
            snapshot_id=snapshot_id,
            company_id=normalized_company_id,
            source=normalized_source,
            raw_content=normalized_raw_content,
            retrieved_at=normalized_retrieved_at,
            content_hash=content_hash,
            version=next_version,
        )
        self._version_by_key[source_key] = next_version

        return {
            "snapshot_id": snapshot_id,
            "content_hash": content_hash,
            "stored": True,
        }

    def get_snapshot(self, snapshot_id: object) -> dict[str, object]:
        normalized_snapshot_id = _validate_non_empty_string(snapshot_id, "snapshot_id")
        snapshot = self._snapshots_by_id.get(normalized_snapshot_id)
        if snapshot is None:
            raise ValueError(f"unknown snapshot_id: {normalized_snapshot_id}")

        return {
            "snapshot_id": snapshot.snapshot_id,
            "company_id": snapshot.company_id,
            "source": snapshot.source,
            "raw_content": snapshot.raw_content,
            "retrieved_at": snapshot.retrieved_at,
            "content_hash": snapshot.content_hash,
            "version": snapshot.version,
        }


__all__ = ["IngestionSnapshotStore", "StoredSnapshot"]