from __future__ import annotations

import json
from pathlib import Path

from fleetgraph.runtime.runtime_config import build_runtime_config
from fleetgraph.watchlist.priority_engine import derive_needs_review, score_watchlist_company
from fleetgraph.watchlist.read_service import get_watchlist_company_record, list_watchlist_company_records


_DELTA_SUMMARY_KEYS = {
    "company_id",
    "company_name",
    "change_detected",
    "change_types",
    "previous_enrichment_state",
    "current_enrichment_state",
    "new_signal_count",
    "new_project_count",
    "new_email_count",
    "new_key_people_count",
    "confidence_changed",
    "last_enriched_at",
    "priority_score",
    "priority_reason_codes",
}


def _resolve_delta_directory(runtime_config: dict) -> Path:
    validated_runtime_config = build_runtime_config(runtime_config)
    return Path(str(validated_runtime_config["output_directory"])) / "watchlist_deltas"


def _is_valid_delta_summary(payload: object) -> bool:
    if not isinstance(payload, dict):
        return False
    if set(payload.keys()) != _DELTA_SUMMARY_KEYS:
        return False
    if payload["company_id"] is not None and not isinstance(payload["company_id"], str):
        return False
    if payload["company_name"] is not None and not isinstance(payload["company_name"], str):
        return False
    if not isinstance(payload["change_detected"], bool):
        return False
    if not isinstance(payload["change_types"], list):
        return False
    if not isinstance(payload["confidence_changed"], bool):
        return False
    if payload["last_enriched_at"] is not None and not isinstance(payload["last_enriched_at"], str):
        return False
    if not isinstance(payload["priority_score"], int) or isinstance(payload["priority_score"], bool):
        return False
    if not isinstance(payload["priority_reason_codes"], list):
        return False
    for field_name in (
        "new_signal_count",
        "new_project_count",
        "new_email_count",
        "new_key_people_count",
    ):
        value = payload[field_name]
        if not isinstance(value, int) or isinstance(value, bool) or value < 0:
            return False
    return True


def write_watchlist_delta_summary(
    delta_summary: dict[str, object],
    *,
    runtime_config: dict,
    company_id: str,
) -> str:
    if not _is_valid_delta_summary(delta_summary):
        raise ValueError("invalid_delta_summary")
    output_root = _resolve_delta_directory(runtime_config)
    output_root.mkdir(parents=True, exist_ok=True)
    delta_path = output_root / f"{company_id}.json"
    delta_path.write_text(
        json.dumps(delta_summary, sort_keys=True, separators=(",", ":")),
        encoding="utf-8",
    )
    return str(delta_path)


def read_watchlist_delta_summary(
    company_id: str,
    *,
    runtime_config: dict,
) -> dict[str, object]:
    if not isinstance(company_id, str) or company_id.strip() == "":
        raise ValueError("invalid_company_id")
    delta_path = _resolve_delta_directory(runtime_config) / f"{company_id}.json"
    if not delta_path.exists():
        return {
            "ok": False,
            "delta_summary": None,
            "delta_path": str(delta_path),
            "error_code": "missing_delta_summary",
        }
    try:
        payload = json.loads(delta_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {
            "ok": False,
            "delta_summary": None,
            "delta_path": str(delta_path),
            "error_code": "invalid_delta_summary",
        }
    if not _is_valid_delta_summary(payload):
        return {
            "ok": False,
            "delta_summary": None,
            "delta_path": str(delta_path),
            "error_code": "invalid_delta_summary",
        }
    return {
        "ok": True,
        "delta_summary": payload,
        "delta_path": str(delta_path),
        "error_code": None,
    }


def _sorted_attention_records(records: list[dict[str, object]]) -> list[dict[str, object]]:
    return sorted(
        records,
        key=lambda record: (
            -int(record["priority_score"]),
            str(record["company_name"]).lower(),
            str(record["company_id"]),
        ),
    )


def _build_company_attention_record(
    company_record: dict[str, object],
    *,
    runtime_config: dict,
) -> dict[str, object]:
    delta_result = read_watchlist_delta_summary(str(company_record["company_id"]), runtime_config=runtime_config)
    delta_summary = delta_result["delta_summary"] if delta_result["ok"] is True else None
    priority = score_watchlist_company(
        company_record,
        delta_summary=delta_summary,
        reference_date=str(build_runtime_config(runtime_config)["run_date"]),
    )
    review = derive_needs_review(company_record, delta_summary=delta_summary)
    return {
        "company_id": company_record["company_id"],
        "company_name": company_record["company_name"],
        "priority_score": priority["priority_score"],
        "priority_band": priority["priority_band"],
        "priority_reason_codes": priority["priority_reason_codes"],
        "enrichment_state": company_record["enrichment_state"],
        "confidence_level": company_record["confidence_level"],
        "last_enriched_at": company_record["last_enriched_at"],
        "change_detected": False if delta_summary is None else bool(delta_summary["change_detected"]),
        "change_types": [] if delta_summary is None else list(delta_summary["change_types"]),
        "needs_review": review["needs_review"],
        "review_reason_codes": review["review_reason_codes"],
    }


def list_changed_companies(
    runtime_config: dict,
    *,
    dataset: str = "verified_subset",
) -> dict[str, object]:
    validated_runtime_config = build_runtime_config(runtime_config)
    changed_records: list[dict[str, object]] = []
    for company_record in list_watchlist_company_records(runtime_config=validated_runtime_config, dataset=dataset):
        delta_result = read_watchlist_delta_summary(str(company_record["company_id"]), runtime_config=validated_runtime_config)
        if delta_result["ok"] is not True:
            continue
        delta_summary = dict(delta_result["delta_summary"])
        if delta_summary["change_detected"] is not True:
            continue
        priority = score_watchlist_company(
            company_record,
            delta_summary=delta_summary,
            reference_date=str(validated_runtime_config["run_date"]),
        )
        delta_summary["priority_score"] = priority["priority_score"]
        delta_summary["priority_reason_codes"] = priority["priority_reason_codes"]
        changed_records.append(delta_summary)
    return {
        "mode": "watchlist",
        "ok": True,
        "changed_companies": _sorted_attention_records(changed_records),
        "error_code": None,
    }


def list_top_target_companies(
    runtime_config: dict,
    *,
    dataset: str = "verified_subset",
    limit: int = 10,
) -> dict[str, object]:
    validated_runtime_config = build_runtime_config(runtime_config)
    company_records = list_watchlist_company_records(runtime_config=validated_runtime_config, dataset=dataset)
    attention_records = [
        _build_company_attention_record(company_record, runtime_config=validated_runtime_config)
        for company_record in company_records
    ]
    return {
        "mode": "watchlist",
        "ok": True,
        "top_targets": _sorted_attention_records(attention_records)[:limit],
        "error_code": None,
    }


def list_needs_review_companies(
    runtime_config: dict,
    *,
    dataset: str = "verified_subset",
) -> dict[str, object]:
    validated_runtime_config = build_runtime_config(runtime_config)
    company_records = list_watchlist_company_records(runtime_config=validated_runtime_config, dataset=dataset)
    attention_records = [
        _build_company_attention_record(company_record, runtime_config=validated_runtime_config)
        for company_record in company_records
    ]
    review_records = [record for record in attention_records if record["needs_review"] is True]
    return {
        "mode": "watchlist",
        "ok": True,
        "needs_review": _sorted_attention_records(review_records),
        "error_code": None,
    }


def get_changed_company_record(
    company_id: str,
    *,
    runtime_config: dict,
    dataset: str = "verified_subset",
) -> dict[str, object]:
    company_result = get_watchlist_company_record(company_id, runtime_config=runtime_config, dataset=dataset)
    if company_result["ok"] is not True:
        return {
            "ok": False,
            "company": None,
            "delta_summary": None,
            "error_code": str(company_result["error_code"]),
        }
    delta_result = read_watchlist_delta_summary(company_id, runtime_config=runtime_config)
    if delta_result["ok"] is not True:
        return {
            "ok": True,
            "company": company_result["company"],
            "delta_summary": None,
            "error_code": None,
        }
    return {
        "ok": True,
        "company": company_result["company"],
        "delta_summary": delta_result["delta_summary"],
        "error_code": None,
    }
