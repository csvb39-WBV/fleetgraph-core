from __future__ import annotations

import json
from pathlib import Path


def _merge_scalar(existing_value: object, new_value: object) -> object:
    if new_value not in (None, "", []):
        return new_value
    return existing_value


def _merge_list(existing_value: object, new_value: object) -> list[object]:
    existing_items = list(existing_value) if isinstance(existing_value, list) else []
    new_items = list(new_value) if isinstance(new_value, list) else []
    merged_items = existing_items + new_items
    unique_items: list[object] = []
    seen_keys: set[str] = set()
    for item in merged_items:
        item_key = json.dumps(item, sort_keys=True, separators=(",", ":"))
        if item_key not in seen_keys:
            seen_keys.add(item_key)
            unique_items.append(item)
    return unique_items


def merge_watchlist_artifact(existing_artifact: dict[str, object] | None, new_artifact: dict[str, object]) -> dict[str, object]:
    if existing_artifact is None:
        return {key: value for key, value in new_artifact.items()}
    merged_artifact: dict[str, object] = {}
    for field_name in new_artifact.keys():
        if field_name in {
            "key_people",
            "direct_phones",
            "general_emails",
            "published_emails",
            "contact_pages",
            "leadership_pages",
            "address_lines",
            "contact_sources",
            "recent_signals",
            "recent_projects",
            "source_links",
        }:
            merged_artifact[field_name] = _merge_list(existing_artifact.get(field_name), new_artifact[field_name])
            continue
        merged_artifact[field_name] = _merge_scalar(existing_artifact.get(field_name), new_artifact[field_name])
    return merged_artifact


def write_watchlist_artifact(
    enrichment_record: dict[str, object],
    output_directory: str | Path,
    *,
    company_id: str,
) -> str:
    output_root = Path(output_directory).resolve()
    output_root.mkdir(parents=True, exist_ok=True)
    artifact_path = output_root / f"{company_id}.json"
    existing_artifact = None
    if artifact_path.exists():
        existing_artifact = json.loads(artifact_path.read_text(encoding="utf-8"))
    merged_artifact = merge_watchlist_artifact(existing_artifact, enrichment_record)
    artifact_path.write_text(
        json.dumps(merged_artifact, sort_keys=True, separators=(",", ":")),
        encoding="utf-8",
    )
    return str(artifact_path)
