from __future__ import annotations

import json
from pathlib import Path

from fleetgraph.runtime.runtime_config import build_runtime_config
from fleetgraph.watchlist.outreach_templates import (
    build_email_body,
    build_subject_line,
    build_why_now,
    build_why_this_company,
)
from fleetgraph.watchlist.read_service import get_watchlist_company_record, list_watchlist_company_records


_OUTREACH_STATUSES = {
    "not_ready",
    "ready_to_draft",
    "drafted",
    "reviewed",
    "sent",
    "suppressed",
}


def _resolve_outreach_directory(runtime_config: dict) -> Path:
    validated_runtime_config = build_runtime_config(runtime_config)
    return Path(str(validated_runtime_config["output_directory"])) / "watchlist_outreach"


def _is_non_empty_string(value: object) -> bool:
    return isinstance(value, str) and value.strip() != ""


def _first_signal_summary(company_record: dict[str, object]) -> str:
    recent_signals = list(company_record.get("recent_signals", []))
    if len(recent_signals) == 0:
        return ""
    first_signal = recent_signals[0]
    if not isinstance(first_signal, dict):
        return ""
    return str(first_signal.get("event_summary", "")).strip()


def _guess_target_role(company_record: dict[str, object]) -> str:
    signal_summary = _first_signal_summary(company_record).lower()
    if any(term in signal_summary for term in ("lawsuit", "sued", "litigation", "subpoena", "investigation", "audit")):
        return "legal_risk"
    if any(term in signal_summary for term in ("default", "claim", "dispute", "delay")):
        return "finance"
    if signal_summary != "":
        return "operations"
    return "general"


def _select_contact(company_record: dict[str, object]) -> dict[str, object]:
    published_emails = list(company_record.get("published_emails", []))
    if len(published_emails) > 0:
        email_entry = published_emails[0]
        return {
            "contact_email": email_entry.get("email"),
            "contact_phone": None,
            "contact_type": "direct_email",
        }
    general_emails = list(company_record.get("general_emails", []))
    if len(general_emails) > 0:
        email_entry = general_emails[0]
        return {
            "contact_email": email_entry.get("email"),
            "contact_phone": None,
            "contact_type": "general_email",
        }
    direct_phones = list(company_record.get("direct_phones", []))
    if len(direct_phones) > 0:
        phone_entry = direct_phones[0]
        return {
            "contact_email": None,
            "contact_phone": phone_entry.get("phone"),
            "contact_type": "phone",
        }
    return {
        "contact_email": None,
        "contact_phone": None,
        "contact_type": "none",
    }


def _select_contact_name(company_record: dict[str, object], *, target_role_guess: str) -> str | None:
    key_people = [item for item in list(company_record.get("key_people", [])) if isinstance(item, dict)]
    title_preferences = {
        "legal_risk": ("Chief Risk Officer", "CFO", "CEO"),
        "finance": ("CFO", "Chief Risk Officer", "CEO"),
        "operations": ("CEO", "CFO", "Chief Risk Officer"),
        "general": ("CEO", "CFO", "Chief Risk Officer"),
    }
    preferred_titles = title_preferences.get(target_role_guess, title_preferences["general"])
    for preferred_title in preferred_titles:
        for person in key_people:
            if str(person.get("title")) == preferred_title and _is_non_empty_string(person.get("name")):
                return str(person.get("name")).strip()
    for person in key_people:
        if _is_non_empty_string(person.get("name")):
            return str(person.get("name")).strip()
    return None


def _qualification_reasons(company_record: dict[str, object], *, outreach_status: str, contact_type: str) -> list[str]:
    reasons: list[str] = []
    if outreach_status == "suppressed":
        reasons.append("company_suppressed")
        return reasons
    if not _is_non_empty_string(company_record.get("company_name")):
        reasons.append("missing_company_name")
    has_signals = len(list(company_record.get("recent_signals", []))) > 0
    has_review_condition = company_record.get("artifact_status") == "invalid_artifact"
    if not has_signals and not has_review_condition:
        reasons.append("missing_meaningful_signal")
    if contact_type == "none":
        reasons.append("missing_contact_method")
    if contact_type == "phone":
        reasons.append("phone_only_contact")
    if not _is_non_empty_string(company_record.get("contact_confidence_level")):
        reasons.append("missing_contact_confidence")
    return reasons


def determine_outreach_status(
    company_record: dict[str, object],
    *,
    existing_status: str | None = None,
    suppressed: bool = False,
) -> dict[str, object]:
    if suppressed or str(existing_status or "").strip() == "suppressed":
        return {
            "outreach_status": "suppressed",
            "qualification_reasons": ["company_suppressed"],
            "ready_to_draft": False,
        }
    contact_selection = _select_contact(company_record)
    qualification_reasons = _qualification_reasons(
        company_record,
        outreach_status="not_ready",
        contact_type=str(contact_selection["contact_type"]),
    )
    if len(qualification_reasons) > 0:
        return {
            "outreach_status": "not_ready",
            "qualification_reasons": qualification_reasons,
            "ready_to_draft": False,
        }
    if str(existing_status or "").strip() == "drafted":
        return {
            "outreach_status": "drafted",
            "qualification_reasons": ["qualified_for_outreach"],
            "ready_to_draft": True,
        }
    return {
        "outreach_status": "ready_to_draft",
        "qualification_reasons": ["qualified_for_outreach"],
        "ready_to_draft": True,
    }


def build_outreach_record(
    company_record: dict[str, object],
    *,
    run_date: str,
    existing_status: str | None = None,
    suppressed: bool = False,
) -> dict[str, object]:
    target_role_guess = _guess_target_role(company_record)
    contact_selection = _select_contact(company_record)
    contact_name = _select_contact_name(company_record, target_role_guess=target_role_guess)
    status = determine_outreach_status(
        company_record,
        existing_status=existing_status,
        suppressed=suppressed,
    )
    signal_summary = _first_signal_summary(company_record)
    why_now = build_why_now(
        signal_summary=signal_summary,
        qualification_reasons=list(status["qualification_reasons"]),
    )
    why_this_company = build_why_this_company(
        company_name=str(company_record.get("company_name", "")),
        target_role_guess=target_role_guess,
        contact_type=str(contact_selection["contact_type"]),
    )
    subject_line = build_subject_line(
        company_name=str(company_record.get("company_name", "")),
        signal_summary=signal_summary,
        target_role_guess=target_role_guess,
    )
    email_body = build_email_body(
        contact_name=contact_name,
        company_name=str(company_record.get("company_name", "")),
        signal_summary=signal_summary,
        why_now=why_now,
        why_this_company=why_this_company,
    )
    return {
        "company_id": str(company_record.get("company_id", "")),
        "company_name": str(company_record.get("company_name", "")),
        "contact_name": contact_name,
        "contact_email": contact_selection["contact_email"],
        "contact_phone": contact_selection["contact_phone"],
        "contact_type": str(contact_selection["contact_type"]),
        "target_role_guess": target_role_guess,
        "signal_summary": signal_summary,
        "why_now": why_now,
        "why_this_company": why_this_company,
        "subject_line": subject_line,
        "email_body": email_body,
        "source_links": list(company_record.get("source_links", [])),
        "outreach_status": str(status["outreach_status"]),
        "qualification_reasons": list(status["qualification_reasons"]),
        "draft_generated_at": str(run_date),
    }


def _is_valid_outreach_record(payload: object) -> bool:
    required_keys = {
        "company_id",
        "company_name",
        "contact_name",
        "contact_email",
        "contact_phone",
        "contact_type",
        "target_role_guess",
        "signal_summary",
        "why_now",
        "why_this_company",
        "subject_line",
        "email_body",
        "source_links",
        "outreach_status",
        "qualification_reasons",
        "draft_generated_at",
    }
    if not isinstance(payload, dict):
        return False
    if set(payload.keys()) != required_keys:
        return False
    if not _is_non_empty_string(payload.get("company_id")):
        return False
    if not _is_non_empty_string(payload.get("company_name")):
        return False
    if payload.get("contact_name") is not None and not isinstance(payload.get("contact_name"), str):
        return False
    if payload.get("contact_email") is not None and not isinstance(payload.get("contact_email"), str):
        return False
    if payload.get("contact_phone") is not None and not isinstance(payload.get("contact_phone"), str):
        return False
    if str(payload.get("outreach_status")) not in _OUTREACH_STATUSES:
        return False
    if not isinstance(payload.get("source_links"), list):
        return False
    if not isinstance(payload.get("qualification_reasons"), list):
        return False
    return True


def read_outreach_record(
    company_id: str,
    *,
    runtime_config: dict,
) -> dict[str, object]:
    if not _is_non_empty_string(company_id):
        raise ValueError("invalid_company_id")
    record_path = _resolve_outreach_directory(runtime_config) / f"{company_id}.json"
    if not record_path.exists():
        return {
            "ok": False,
            "outreach_record": None,
            "record_path": str(record_path),
            "error_code": "missing_outreach_record",
        }
    try:
        payload = json.loads(record_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {
            "ok": False,
            "outreach_record": None,
            "record_path": str(record_path),
            "error_code": "invalid_outreach_record",
        }
    if not _is_valid_outreach_record(payload):
        return {
            "ok": False,
            "outreach_record": None,
            "record_path": str(record_path),
            "error_code": "invalid_outreach_record",
        }
    return {
        "ok": True,
        "outreach_record": payload,
        "record_path": str(record_path),
        "error_code": None,
    }


def write_outreach_record(
    outreach_record: dict[str, object],
    *,
    runtime_config: dict,
    company_id: str,
) -> str:
    if not _is_valid_outreach_record(outreach_record):
        raise ValueError("invalid_outreach_record")
    output_root = _resolve_outreach_directory(runtime_config)
    output_root.mkdir(parents=True, exist_ok=True)
    record_path = output_root / f"{company_id}.json"
    record_path.write_text(
        json.dumps(outreach_record, sort_keys=True, separators=(",", ":")),
        encoding="utf-8",
    )
    return str(record_path)


def prepare_outreach_record(
    company_id: str,
    *,
    runtime_config: dict,
    dataset: str = "verified_subset",
    status_override: str | None = None,
) -> dict[str, object]:
    company_result = get_watchlist_company_record(company_id, runtime_config=runtime_config, dataset=dataset)
    if company_result["ok"] is not True:
        return {
            "ok": False,
            "outreach_record": None,
            "record_path": None,
            "error_code": str(company_result["error_code"]),
        }
    existing_result = read_outreach_record(company_id, runtime_config=runtime_config)
    existing_status = None
    if existing_result["ok"] is True:
        existing_status = str(existing_result["outreach_record"]["outreach_status"])
    if status_override is not None:
        existing_status = status_override
    outreach_record = build_outreach_record(
        company_result["company"],
        run_date=str(build_runtime_config(runtime_config)["run_date"]),
        existing_status=existing_status,
        suppressed=str(existing_status or "") == "suppressed",
    )
    record_path = write_outreach_record(outreach_record, runtime_config=runtime_config, company_id=company_id)
    return {
        "ok": True,
        "outreach_record": outreach_record,
        "record_path": record_path,
        "error_code": None,
    }


def get_outreach_record(
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
            "outreach_record": None,
            "error_code": str(company_result["error_code"]),
        }
    outreach_result = read_outreach_record(company_id, runtime_config=runtime_config)
    if outreach_result["ok"] is True:
        return {
            "ok": True,
            "company": company_result["company"],
            "outreach_record": outreach_result["outreach_record"],
            "error_code": None,
        }
    draft = build_outreach_record(
        company_result["company"],
        run_date=str(build_runtime_config(runtime_config)["run_date"]),
    )
    return {
        "ok": True,
        "company": company_result["company"],
        "outreach_record": draft,
        "error_code": None,
    }


def list_outreach_ready_companies(
    runtime_config: dict,
    *,
    dataset: str = "verified_subset",
) -> dict[str, object]:
    validated_runtime_config = build_runtime_config(runtime_config)
    ready_records: list[dict[str, object]] = []
    for company_record in list_watchlist_company_records(runtime_config=validated_runtime_config, dataset=dataset):
        existing_result = read_outreach_record(str(company_record["company_id"]), runtime_config=validated_runtime_config)
        existing_status = None
        if existing_result["ok"] is True:
            existing_status = str(existing_result["outreach_record"]["outreach_status"])
        outreach_record = build_outreach_record(
            company_record,
            run_date=str(validated_runtime_config["run_date"]),
            existing_status=existing_status,
            suppressed=str(existing_status or "") == "suppressed",
        )
        if outreach_record["outreach_status"] == "ready_to_draft":
            ready_records.append(outreach_record)
    ready_records = sorted(
        ready_records,
        key=lambda record: (
            str(record["company_name"]).lower(),
            str(record["company_id"]),
        ),
    )
    return {
        "ok": True,
        "outreach_ready": ready_records,
        "error_code": None,
    }
