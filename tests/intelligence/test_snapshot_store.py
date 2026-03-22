from __future__ import annotations

import pathlib
import sys
import uuid

import pytest


PROJECT_ROOT = pathlib.Path(__file__).resolve().parents[2]
SRC_ROOT = PROJECT_ROOT / "src"

if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))


from fleetgraph_core.intelligence.ingestion_snapshot_store import IngestionSnapshotStore


def test_store_snapshot_returns_expected_shape_and_values() -> None:
    store = IngestionSnapshotStore()

    result = store.store_snapshot(
        company_id="acme-001",
        source="sec_filings",
        raw_content="raw filing text",
        retrieved_at="2026-03-22T10:00:00Z",
    )

    assert result["stored"] is True
    assert isinstance(result["snapshot_id"], str)
    uuid.UUID(str(result["snapshot_id"]))
    assert isinstance(result["content_hash"], str)
    assert len(str(result["content_hash"])) == 64


def test_same_content_produces_same_hash() -> None:
    store = IngestionSnapshotStore()

    first = store.store_snapshot(
        company_id="acme-001",
        source="news_rss_feeds",
        raw_content="identical content",
        retrieved_at="2026-03-22T10:00:00Z",
    )
    second = store.store_snapshot(
        company_id="acme-002",
        source="news_rss_feeds",
        raw_content="identical content",
        retrieved_at="2026-03-22T11:00:00Z",
    )

    assert first["content_hash"] == second["content_hash"]


def test_snapshot_ids_are_unique_for_distinct_stores() -> None:
    store = IngestionSnapshotStore()
    snapshot_ids = set()

    for index in range(10):
        result = store.store_snapshot(
            company_id=f"acme-{index}",
            source="job_boards",
            raw_content=f"content-{index}",
            retrieved_at="2026-03-22T12:00:00Z",
        )
        snapshot_id = str(result["snapshot_id"])
        assert snapshot_id not in snapshot_ids
        snapshot_ids.add(snapshot_id)


def test_store_prevents_snapshot_id_overwrite() -> None:
    fixed_id = uuid.UUID("12345678-1234-5678-1234-567812345678")
    store = IngestionSnapshotStore(snapshot_id_factory=lambda: fixed_id)

    store.store_snapshot(
        company_id="acme-001",
        source="sec_filings",
        raw_content="content-one",
        retrieved_at="2026-03-22T10:00:00Z",
    )

    with pytest.raises(
        ValueError,
        match="snapshot_id already exists: 12345678-1234-5678-1234-567812345678",
    ):
        store.store_snapshot(
            company_id="acme-001",
            source="sec_filings",
            raw_content="content-two",
            retrieved_at="2026-03-22T10:05:00Z",
        )


def test_get_snapshot_returns_stored_record_with_versioning() -> None:
    store = IngestionSnapshotStore()

    first = store.store_snapshot(
        company_id="acme-001",
        source="press_releases",
        raw_content="release-1",
        retrieved_at="2026-03-22T10:00:00Z",
    )
    second = store.store_snapshot(
        company_id="acme-001",
        source="press_releases",
        raw_content="release-2",
        retrieved_at="2026-03-22T10:10:00Z",
    )

    first_snapshot = store.get_snapshot(first["snapshot_id"])
    second_snapshot = store.get_snapshot(second["snapshot_id"])

    assert first_snapshot["version"] == 1
    assert second_snapshot["version"] == 2
    assert first_snapshot["raw_content"] == "release-1"
    assert second_snapshot["raw_content"] == "release-2"


def test_get_snapshot_rejects_unknown_snapshot_id() -> None:
    store = IngestionSnapshotStore()

    with pytest.raises(ValueError, match="unknown snapshot_id: missing"):
        store.get_snapshot("missing")


@pytest.mark.parametrize("field_name", ["company_id", "source", "raw_content", "retrieved_at"])
def test_store_snapshot_rejects_blank_required_fields(field_name: str) -> None:
    store = IngestionSnapshotStore()
    kwargs = {
        "company_id": "acme-001",
        "source": "sec_filings",
        "raw_content": "raw filing text",
        "retrieved_at": "2026-03-22T10:00:00Z",
    }
    kwargs[field_name] = "   "

    with pytest.raises(ValueError, match=rf"{field_name} must be a non-empty string"):
        store.store_snapshot(**kwargs)