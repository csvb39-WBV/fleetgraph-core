from __future__ import annotations

import json
from pathlib import Path

from fleetgraph.runtime.runtime_config import build_runtime_config
from fleetgraph.watchlist.watchlist_loader import load_seed_enriched, load_verified_subset


_ARTIFACT_KEYS = {
    "company_name",
    "website",
    "main_phone",
    "hq_city",
    "hq_state",
    "priority_tier",
    "category",
    "segment",
    "key_people",
    "direct_phones",
    "general_emails",
    "published_emails",
    "contact_pages",
    "leadership_pages",
    "address_lines",
    "contact_sources",
    "email_pattern_guess",
    "contact_confidence_level",
    "reachability_score",
    "recent_signals",
    "recent_projects",
    "source_links",
    "last_enriched_at",
    "confidence_level",
}


def _resolve_watchlist_artifact_directory(runtime_config: dict) -> Path:
    validated_runtime_config = build_runtime_config(runtime_config)
    return Path(str(validated_runtime_config["output_directory"])) / "watchlist"


def _load_seed_records(dataset: str) -> list[dict[str, object]]:
    if dataset == "verified_subset":
        return load_verified_subset()
    if dataset == "seed_enriched":
        return load_seed_enriched()
    raise ValueError("invalid_watchlist_dataset")


def _sorted_watchlist_records(records: list[dict[str, object]]) -> list[dict[str, object]]:
    return sorted(
        records,
        key=lambda record: (
            str(record["priority_tier"] or "~"),
            str(record["company_name"]).lower(),
            str(record["company_id"]),
        ),
    )


def _is_valid_artifact_payload(payload: object) -> bool:
    if not isinstance(payload, dict):
        return False
    if set(payload.keys()) != _ARTIFACT_KEYS:
        return False
    if not isinstance(payload["company_name"], str) or payload["company_name"].strip() == "":
        return False
    if payload["website"] is not None and not isinstance(payload["website"], str):
        return False
    if payload["main_phone"] is not None and not isinstance(payload["main_phone"], str):
        return False
    if payload["last_enriched_at"] is not None and not isinstance(payload["last_enriched_at"], str):
        return False
    if payload["confidence_level"] not in {"low", "medium", "high"}:
        return False
    if payload["contact_confidence_level"] not in {"low", "medium", "high"}:
        return False
    if not isinstance(payload["reachability_score"], int) or isinstance(payload["reachability_score"], bool):
        return False
    for field_name in (
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
    ):
        if not isinstance(payload[field_name], list):
            return False
    if payload["email_pattern_guess"] is not None and not isinstance(payload["email_pattern_guess"], str):
        return False
    return True


def read_watchlist_artifact(
    company_id: str,
    *,
    runtime_config: dict,
) -> dict[str, object]:
    if not isinstance(company_id, str) or company_id.strip() == "":
        raise ValueError("invalid_company_id")
    artifact_directory = _resolve_watchlist_artifact_directory(runtime_config)
    artifact_path = artifact_directory / f"{company_id}.json"
    if not artifact_path.exists():
        return {
            "ok": False,
            "artifact": None,
            "artifact_path": str(artifact_path),
            "error_code": "missing_artifact",
        }
    try:
        payload = json.loads(artifact_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {
            "ok": False,
            "artifact": None,
            "artifact_path": str(artifact_path),
            "error_code": "invalid_artifact",
        }
    if not _is_valid_artifact_payload(payload):
        return {
            "ok": False,
            "artifact": None,
            "artifact_path": str(artifact_path),
            "error_code": "invalid_artifact",
        }
    return {
        "ok": True,
        "artifact": payload,
        "artifact_path": str(artifact_path),
        "error_code": None,
    }


def derive_enrichment_state(
    seed_record: dict[str, object],
    artifact_payload: dict[str, object] | None,
) -> str:
    if artifact_payload is None:
        return "seed_only"
    has_emails = len(artifact_payload["published_emails"]) > 0
    has_signals = len(artifact_payload["recent_signals"]) > 0
    has_projects = len(artifact_payload["recent_projects"]) > 0
    has_refresh_timestamp = isinstance(artifact_payload["last_enriched_at"], str) and artifact_payload["last_enriched_at"].strip() != ""
    has_email_guess = artifact_payload["email_pattern_guess"] is not None
    has_phone = artifact_payload["main_phone"] is not None or len(artifact_payload["direct_phones"]) > 0
    if has_refresh_timestamp and (has_emails or has_signals or has_projects):
        return "enriched"
    if has_refresh_timestamp or has_email_guess or has_phone:
        return "partial"
    return "seed_only"


def merge_seed_with_artifact(
    seed_record: dict[str, object],
    artifact_payload: dict[str, object] | None,
    *,
    artifact_status: str | None = None,
) -> dict[str, object]:
    artifact_status_value = artifact_status or ("ok" if artifact_payload is not None else "missing_artifact")
    artifact_source_links = [] if artifact_payload is None else list(artifact_payload["source_links"])
    merged_record = {
        "company_id": seed_record["company_id"],
        "company_name": seed_record["company_name"],
        "category": seed_record["category"],
        "segment": seed_record["segment"],
        "priority_tier": seed_record["priority_tier"],
        "website": seed_record["website"] if artifact_payload is None or artifact_payload["website"] is None else artifact_payload["website"],
        "hq_city": seed_record["hq_city"] if artifact_payload is None or artifact_payload["hq_city"] is None else artifact_payload["hq_city"],
        "hq_state": seed_record["hq_state"] if artifact_payload is None or artifact_payload["hq_state"] is None else artifact_payload["hq_state"],
        "hq_zip": seed_record["hq_zip"],
        "phone": seed_record["main_phone"],
        "main_phone": seed_record["main_phone"] if artifact_payload is None or artifact_payload["main_phone"] is None else artifact_payload["main_phone"],
        "ceo_name": seed_record["ceo_name"],
        "cfo_name": seed_record["cfo_name"],
        "chief_risk_officer_name": seed_record["chief_risk_officer_name"],
        "verification_status": seed_record["verification_status"],
        "notes": seed_record["notes"],
        "key_people": [] if artifact_payload is None else list(artifact_payload["key_people"]),
        "direct_phones": [] if artifact_payload is None else list(artifact_payload["direct_phones"]),
        "general_emails": [] if artifact_payload is None else list(artifact_payload["general_emails"]),
        "published_emails": [] if artifact_payload is None else list(artifact_payload["published_emails"]),
        "contact_pages": [] if artifact_payload is None else list(artifact_payload["contact_pages"]),
        "leadership_pages": [] if artifact_payload is None else list(artifact_payload["leadership_pages"]),
        "address_lines": [] if artifact_payload is None else list(artifact_payload["address_lines"]),
        "contact_sources": [] if artifact_payload is None else list(artifact_payload["contact_sources"]),
        "email_pattern_guess": None if artifact_payload is None else artifact_payload["email_pattern_guess"],
        "contact_confidence_level": "low" if artifact_payload is None else artifact_payload["contact_confidence_level"],
        "reachability_score": 0 if artifact_payload is None else artifact_payload["reachability_score"],
        "recent_signals": [] if artifact_payload is None else list(artifact_payload["recent_signals"]),
        "recent_projects": [] if artifact_payload is None else list(artifact_payload["recent_projects"]),
        "source_links": artifact_source_links if len(artifact_source_links) > 0 else list(seed_record["sources"]),
        "last_enriched_at": None if artifact_payload is None else artifact_payload["last_enriched_at"],
        "confidence_level": "low" if artifact_payload is None else artifact_payload["confidence_level"],
        "enrichment_state": derive_enrichment_state(seed_record, artifact_payload),
        "artifact_status": artifact_status_value,
    }
    return merged_record


def list_watchlist_company_records(
    *,
    runtime_config: dict,
    dataset: str = "verified_subset",
) -> list[dict[str, object]]:
    seed_records = _load_seed_records(dataset)
    merged_records = []
    for seed_record in seed_records:
        artifact_result = read_watchlist_artifact(str(seed_record["company_id"]), runtime_config=runtime_config)
        artifact_payload = artifact_result["artifact"] if artifact_result["ok"] is True else None
        artifact_status = "ok" if artifact_result["ok"] is True else str(artifact_result["error_code"])
        merged_records.append(merge_seed_with_artifact(seed_record, artifact_payload, artifact_status=artifact_status))
    return _sorted_watchlist_records(merged_records)


def get_watchlist_company_record(
    company_id: str,
    *,
    runtime_config: dict,
    dataset: str = "verified_subset",
) -> dict[str, object]:
    if not isinstance(company_id, str) or company_id.strip() == "":
        raise ValueError("invalid_company_id")
    seed_records = _load_seed_records(dataset)
    for seed_record in seed_records:
        if seed_record["company_id"] == company_id:
            artifact_result = read_watchlist_artifact(company_id, runtime_config=runtime_config)
            artifact_payload = artifact_result["artifact"] if artifact_result["ok"] is True else None
            artifact_status = "ok" if artifact_result["ok"] is True else str(artifact_result["error_code"])
            return {
                "ok": True,
                "company": merge_seed_with_artifact(seed_record, artifact_payload, artifact_status=artifact_status),
                "error_code": None,
            }
    return {
        "ok": False,
        "company": None,
        "error_code": "unknown_company_id",
    }
