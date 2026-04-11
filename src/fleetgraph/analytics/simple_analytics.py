from __future__ import annotations

from copy import deepcopy
from typing import Any

from fleetgraph.quality.role_confidence import score_role_confidence

__all__ = [
    "build_campaign_summary",
    "build_role_summary",
    "build_signal_summary",
    "build_simple_analytics_report",
    "build_variant_summary",
    "normalize_analytics_inputs",
]

_SENT_STATUSES = {
    "SENT",
    "REPLIED",
    "BOUNCED",
    "UNSUBSCRIBED",
}
_REQUIRED_OPTIMIZED_DRAFT_FIELDS = (
    "prospect_id",
    "company_id",
    "company_name",
    "contact",
    "selected_bucket",
    "signal_type",
    "signal_detail",
    "template_family",
    "template_variant_id",
    "message_optimization_id",
    "optimization_metadata",
)
_REQUIRED_PLAN_FIELDS = (
    "draft_id",
    "prospect_id",
    "company_id",
    "company_name",
    "contact_email",
    "contact_name",
    "sequence_step",
    "send_window",
    "scheduled_send_at",
)
_REQUIRED_STATE_FIELDS = (
    "draft_id",
    "prospect_id",
    "company_id",
    "contact_email",
    "sequence_step",
    "status",
    "last_event_at",
    "next_scheduled_at",
)
_REQUIRED_CONVERSION_SIGNAL_FIELDS = (
    "prospect_id",
    "conversion_flag",
    "reason",
)
_REQUIRED_CONVERSION_ENTRY_FIELDS = (
    "conversion_entry_id",
    "prospect_id",
    "company_id",
    "company_name",
    "contact_email",
    "contact_name",
    "signal_family",
    "selected_bucket",
    "campaign_id",
    "entry_mode",
    "attribution_seed",
    "factledger_handoff",
)


def _normalize_non_empty_string(value: object, *, field_name: str) -> str:
    if not isinstance(value, str):
        raise ValueError(f"{field_name} must be a non-empty string")
    normalized_value = value.strip()
    if normalized_value == "":
        raise ValueError(f"{field_name} must be a non-empty string")
    return normalized_value


def _normalize_bool(value: object, *, field_name: str) -> bool:
    if not isinstance(value, bool):
        raise ValueError(f"{field_name} must be a bool")
    return value


def _normalize_int(value: object, *, field_name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"{field_name} must be an int")
    return value


def _safe_rate(numerator: int, denominator: int) -> float:
    if denominator <= 0:
        return 0.0
    return round(numerator / denominator, 4)


def _contact_lookup_key(*, prospect_id: str, company_id: str, contact_email: str) -> tuple[str, str, str]:
    return (prospect_id, company_id, contact_email.lower())


def _normalized_title_group(title: str) -> str:
    normalized_title = " ".join(title.strip().lower().split())
    if normalized_title == "":
        return "unknown"
    if any(pattern in normalized_title for pattern in ("owner", "ceo", "founder", "president")):
        return "executive"
    if any(pattern in normalized_title for pattern in ("cfo", "controller", "finance")):
        return "finance"
    if any(pattern in normalized_title for pattern in ("director", "vice president", "vp")):
        return "director_vp"
    return "other"


def _normalize_contact(contact: object, *, field_name: str) -> dict[str, Any]:
    if not isinstance(contact, dict):
        raise ValueError(f"{field_name} must be a dict")
    normalized_contact = {
        "name": _normalize_non_empty_string(contact.get("name"), field_name=f"{field_name}.name"),
        "email": _normalize_non_empty_string(contact.get("email"), field_name=f"{field_name}.email").lower(),
    }
    title = contact.get("title")
    if title is not None:
        normalized_contact["title"] = _normalize_non_empty_string(title, field_name=f"{field_name}.title")
    else:
        normalized_contact["title"] = ""
    return normalized_contact


def _normalize_optimized_message_drafts(records: list[object]) -> list[dict[str, Any]]:
    if not isinstance(records, list):
        raise ValueError("optimized_message_drafts must be a list")
    normalized_records: list[dict[str, Any]] = []
    for index, record in enumerate(records):
        if not isinstance(record, dict):
            raise ValueError(f"optimized_message_drafts[{index}] must be a dict")
        for field_name in _REQUIRED_OPTIMIZED_DRAFT_FIELDS:
            if field_name not in record:
                raise ValueError(f"optimized_message_drafts[{index}] missing required field: {field_name}")
        optimization_metadata = record.get("optimization_metadata")
        if not isinstance(optimization_metadata, dict):
            raise ValueError(f"optimized_message_drafts[{index}].optimization_metadata must be a dict")
        normalized_records.append(
            {
                "prospect_id": _normalize_non_empty_string(record.get("prospect_id"), field_name=f"optimized_message_drafts[{index}].prospect_id"),
                "company_id": _normalize_non_empty_string(record.get("company_id"), field_name=f"optimized_message_drafts[{index}].company_id"),
                "company_name": _normalize_non_empty_string(record.get("company_name"), field_name=f"optimized_message_drafts[{index}].company_name"),
                "contact": _normalize_contact(record.get("contact"), field_name=f"optimized_message_drafts[{index}].contact"),
                "selected_bucket": _normalize_non_empty_string(record.get("selected_bucket"), field_name=f"optimized_message_drafts[{index}].selected_bucket"),
                "signal_type": _normalize_non_empty_string(record.get("signal_type"), field_name=f"optimized_message_drafts[{index}].signal_type"),
                "signal_detail": _normalize_non_empty_string(record.get("signal_detail"), field_name=f"optimized_message_drafts[{index}].signal_detail"),
                "template_family": _normalize_non_empty_string(record.get("template_family"), field_name=f"optimized_message_drafts[{index}].template_family").lower(),
                "template_variant_id": _normalize_non_empty_string(record.get("template_variant_id"), field_name=f"optimized_message_drafts[{index}].template_variant_id"),
                "message_optimization_id": _normalize_non_empty_string(record.get("message_optimization_id"), field_name=f"optimized_message_drafts[{index}].message_optimization_id"),
                "optimization_metadata": {
                    "variant_group": _normalize_non_empty_string(optimization_metadata.get("variant_group"), field_name=f"optimized_message_drafts[{index}].optimization_metadata.variant_group"),
                    "selection_mode": _normalize_non_empty_string(optimization_metadata.get("selection_mode"), field_name=f"optimized_message_drafts[{index}].optimization_metadata.selection_mode"),
                    "copy_style": _normalize_non_empty_string(optimization_metadata.get("copy_style"), field_name=f"optimized_message_drafts[{index}].optimization_metadata.copy_style"),
                },
            }
        )
    return deepcopy(normalized_records)


def _normalize_execution_plan(records: list[object]) -> list[dict[str, Any]]:
    if not isinstance(records, list):
        raise ValueError("execution_plan_records must be a list")
    normalized_records: list[dict[str, Any]] = []
    for index, record in enumerate(records):
        if not isinstance(record, dict):
            raise ValueError(f"execution_plan_records[{index}] must be a dict")
        for field_name in _REQUIRED_PLAN_FIELDS:
            if field_name not in record:
                raise ValueError(f"execution_plan_records[{index}] missing required field: {field_name}")
        normalized_records.append(
            {
                "draft_id": _normalize_non_empty_string(record.get("draft_id"), field_name=f"execution_plan_records[{index}].draft_id"),
                "prospect_id": _normalize_non_empty_string(record.get("prospect_id"), field_name=f"execution_plan_records[{index}].prospect_id"),
                "company_id": _normalize_non_empty_string(record.get("company_id"), field_name=f"execution_plan_records[{index}].company_id"),
                "company_name": _normalize_non_empty_string(record.get("company_name"), field_name=f"execution_plan_records[{index}].company_name"),
                "contact_email": _normalize_non_empty_string(record.get("contact_email"), field_name=f"execution_plan_records[{index}].contact_email").lower(),
                "contact_name": _normalize_non_empty_string(record.get("contact_name"), field_name=f"execution_plan_records[{index}].contact_name"),
                "sequence_step": _normalize_int(record.get("sequence_step"), field_name=f"execution_plan_records[{index}].sequence_step"),
                "send_window": _normalize_non_empty_string(record.get("send_window"), field_name=f"execution_plan_records[{index}].send_window"),
                "scheduled_send_at": _normalize_non_empty_string(record.get("scheduled_send_at"), field_name=f"execution_plan_records[{index}].scheduled_send_at"),
            }
        )
    normalized_records.sort(key=lambda row: (row["scheduled_send_at"], row["draft_id"]))
    return deepcopy(normalized_records)


def _normalize_state_records(records: list[object]) -> list[dict[str, Any]]:
    if not isinstance(records, list):
        raise ValueError("state_records must be a list")
    normalized_records: list[dict[str, Any]] = []
    for index, record in enumerate(records):
        if not isinstance(record, dict):
            raise ValueError(f"state_records[{index}] must be a dict")
        for field_name in _REQUIRED_STATE_FIELDS:
            if field_name not in record:
                raise ValueError(f"state_records[{index}] missing required field: {field_name}")
        normalized_records.append(
            {
                "draft_id": _normalize_non_empty_string(record.get("draft_id"), field_name=f"state_records[{index}].draft_id"),
                "prospect_id": _normalize_non_empty_string(record.get("prospect_id"), field_name=f"state_records[{index}].prospect_id"),
                "company_id": _normalize_non_empty_string(record.get("company_id"), field_name=f"state_records[{index}].company_id"),
                "contact_email": _normalize_non_empty_string(record.get("contact_email"), field_name=f"state_records[{index}].contact_email").lower(),
                "sequence_step": _normalize_int(record.get("sequence_step"), field_name=f"state_records[{index}].sequence_step"),
                "status": _normalize_non_empty_string(record.get("status"), field_name=f"state_records[{index}].status"),
                "last_event_at": record.get("last_event_at"),
                "next_scheduled_at": record.get("next_scheduled_at"),
            }
        )
    normalized_records.sort(key=lambda row: (row["draft_id"], row["status"]))
    return deepcopy(normalized_records)


def _normalize_conversion_signals(records: list[object]) -> list[dict[str, Any]]:
    if not isinstance(records, list):
        raise ValueError("conversion_signal_records must be a list")
    normalized_records: list[dict[str, Any]] = []
    for index, record in enumerate(records):
        if not isinstance(record, dict):
            raise ValueError(f"conversion_signal_records[{index}] must be a dict")
        for field_name in _REQUIRED_CONVERSION_SIGNAL_FIELDS:
            if field_name not in record:
                raise ValueError(f"conversion_signal_records[{index}] missing required field: {field_name}")
        normalized_records.append(
            {
                "prospect_id": _normalize_non_empty_string(record.get("prospect_id"), field_name=f"conversion_signal_records[{index}].prospect_id"),
                "conversion_flag": _normalize_bool(record.get("conversion_flag"), field_name=f"conversion_signal_records[{index}].conversion_flag"),
                "reason": _normalize_non_empty_string(record.get("reason"), field_name=f"conversion_signal_records[{index}].reason"),
            }
        )
    normalized_records.sort(key=lambda row: (row["prospect_id"], row["reason"]))
    return deepcopy(normalized_records)


def _normalize_conversion_entries(records: list[object]) -> list[dict[str, Any]]:
    if not isinstance(records, list):
        raise ValueError("conversion_entries must be a list")
    normalized_records: list[dict[str, Any]] = []
    for index, record in enumerate(records):
        if not isinstance(record, dict):
            raise ValueError(f"conversion_entries[{index}] must be a dict")
        for field_name in _REQUIRED_CONVERSION_ENTRY_FIELDS:
            if field_name not in record:
                raise ValueError(f"conversion_entries[{index}] missing required field: {field_name}")
        attribution_seed = record.get("attribution_seed")
        if not isinstance(attribution_seed, dict):
            raise ValueError(f"conversion_entries[{index}].attribution_seed must be a dict")
        normalized_records.append(
            {
                "conversion_entry_id": _normalize_non_empty_string(record.get("conversion_entry_id"), field_name=f"conversion_entries[{index}].conversion_entry_id"),
                "prospect_id": _normalize_non_empty_string(record.get("prospect_id"), field_name=f"conversion_entries[{index}].prospect_id"),
                "company_id": _normalize_non_empty_string(record.get("company_id"), field_name=f"conversion_entries[{index}].company_id"),
                "company_name": _normalize_non_empty_string(record.get("company_name"), field_name=f"conversion_entries[{index}].company_name"),
                "contact_email": _normalize_non_empty_string(record.get("contact_email"), field_name=f"conversion_entries[{index}].contact_email").lower(),
                "contact_name": _normalize_non_empty_string(record.get("contact_name"), field_name=f"conversion_entries[{index}].contact_name"),
                "signal_family": _normalize_non_empty_string(record.get("signal_family"), field_name=f"conversion_entries[{index}].signal_family").lower(),
                "selected_bucket": _normalize_non_empty_string(record.get("selected_bucket"), field_name=f"conversion_entries[{index}].selected_bucket"),
                "campaign_id": _normalize_non_empty_string(record.get("campaign_id"), field_name=f"conversion_entries[{index}].campaign_id"),
                "entry_mode": _normalize_non_empty_string(record.get("entry_mode"), field_name=f"conversion_entries[{index}].entry_mode"),
                "attribution_seed": {
                    "draft_id": _normalize_non_empty_string(attribution_seed.get("draft_id"), field_name=f"conversion_entries[{index}].attribution_seed.draft_id"),
                    "sequence_step": _normalize_int(attribution_seed.get("sequence_step"), field_name=f"conversion_entries[{index}].attribution_seed.sequence_step"),
                    "send_window": _normalize_non_empty_string(attribution_seed.get("send_window"), field_name=f"conversion_entries[{index}].attribution_seed.send_window"),
                },
            }
        )
    normalized_records.sort(key=lambda row: (row["campaign_id"], row["conversion_entry_id"]))
    return deepcopy(normalized_records)


def normalize_analytics_inputs(
    *,
    optimized_message_drafts: list[object],
    execution_plan_records: list[object],
    state_records: list[object],
    conversion_signal_records: list[object],
    conversion_entries: list[object],
) -> dict[str, list[dict[str, Any]]]:
    return {
        "optimized_message_drafts": _normalize_optimized_message_drafts(optimized_message_drafts),
        "execution_plan_records": _normalize_execution_plan(execution_plan_records),
        "state_records": _normalize_state_records(state_records),
        "conversion_signal_records": _normalize_conversion_signals(conversion_signal_records),
        "conversion_entries": _normalize_conversion_entries(conversion_entries),
    }


def _build_analytics_rows(normalized_inputs: dict[str, list[dict[str, Any]]]) -> list[dict[str, Any]]:
    optimized_drafts = normalized_inputs["optimized_message_drafts"]
    execution_plan_records = normalized_inputs["execution_plan_records"]
    state_records = normalized_inputs["state_records"]
    conversion_signal_records = normalized_inputs["conversion_signal_records"]
    conversion_entries = normalized_inputs["conversion_entries"]

    drafts_by_key = {
        _contact_lookup_key(
            prospect_id=str(record["prospect_id"]),
            company_id=str(record["company_id"]),
            contact_email=str(record["contact"]["email"]),
        ): record
        for record in optimized_drafts
    }
    state_by_draft_id = {str(record["draft_id"]): record for record in state_records}
    conversion_flag_by_prospect_id = {
        str(record["prospect_id"]): bool(record["conversion_flag"])
        for record in conversion_signal_records
    }
    campaign_id_by_draft_id = {
        str(record["attribution_seed"]["draft_id"]): str(record["campaign_id"])
        for record in conversion_entries
    }

    rows: list[dict[str, Any]] = []
    for execution_plan_record in execution_plan_records:
        draft_lookup_key = _contact_lookup_key(
            prospect_id=str(execution_plan_record["prospect_id"]),
            company_id=str(execution_plan_record["company_id"]),
            contact_email=str(execution_plan_record["contact_email"]),
        )
        optimized_draft = drafts_by_key.get(draft_lookup_key)
        if optimized_draft is None:
            raise ValueError(f"missing optimized draft for plan record: {execution_plan_record['draft_id']}")
        state_record = state_by_draft_id.get(str(execution_plan_record["draft_id"]))
        status = "PENDING" if state_record is None else str(state_record["status"])
        campaign_id = campaign_id_by_draft_id.get(str(execution_plan_record["draft_id"]), "campaign:unassigned")
        role_confidence = score_role_confidence(optimized_draft["contact"].get("title", ""))
        rows.append(
            {
                "campaign_id": campaign_id,
                "draft_id": str(execution_plan_record["draft_id"]),
                "prospect_id": str(execution_plan_record["prospect_id"]),
                "company_id": str(execution_plan_record["company_id"]),
                "contact_email": str(execution_plan_record["contact_email"]),
                "contact_name": str(execution_plan_record["contact_name"]),
                "contact_title": str(optimized_draft["contact"].get("title", "")),
                "contact_title_group": _normalized_title_group(str(optimized_draft["contact"].get("title", ""))),
                "role_confidence": str(role_confidence["role_confidence"]),
                "template_variant_id": str(optimized_draft["template_variant_id"]),
                "template_family": str(optimized_draft["template_family"]),
                "selected_bucket": str(optimized_draft["selected_bucket"]),
                "planned": 1,
                "sent": 1 if status in _SENT_STATUSES else 0,
                "replied": 1 if status == "REPLIED" else 0,
                "bounced": 1 if status == "BOUNCED" else 0,
                "unsubscribed": 1 if status == "UNSUBSCRIBED" else 0,
                "suppressed": 1 if status == "SUPPRESSED" else 0,
                "converted": 1 if conversion_flag_by_prospect_id.get(str(execution_plan_record["prospect_id"]), False) else 0,
            }
        )

    rows.sort(
        key=lambda row: (
            str(row["campaign_id"]),
            str(row["draft_id"]),
            str(row["template_variant_id"]),
            str(row["contact_email"]),
        )
    )
    return rows


def build_campaign_summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    if not isinstance(rows, list):
        raise ValueError("rows must be a list")
    campaign_ids = sorted({str(row["campaign_id"]) for row in rows})
    if campaign_ids == []:
        campaign_id = "campaign:empty"
    elif len(campaign_ids) == 1:
        campaign_id = campaign_ids[0]
    else:
        campaign_id = "campaign:all"

    total_planned = sum(int(row["planned"]) for row in rows)
    total_sent = sum(int(row["sent"]) for row in rows)
    total_replied = sum(int(row["replied"]) for row in rows)
    total_bounced = sum(int(row["bounced"]) for row in rows)
    total_unsubscribed = sum(int(row["unsubscribed"]) for row in rows)
    total_suppressed = sum(int(row["suppressed"]) for row in rows)
    total_converted = sum(int(row["converted"]) for row in rows)
    return {
        "campaign_id": campaign_id,
        "total_planned": total_planned,
        "total_sent": total_sent,
        "total_replied": total_replied,
        "total_bounced": total_bounced,
        "total_unsubscribed": total_unsubscribed,
        "total_suppressed": total_suppressed,
        "total_converted": total_converted,
        "reply_rate": _safe_rate(total_replied, total_sent),
        "conversion_rate": _safe_rate(total_converted, total_sent),
    }


def build_variant_summary(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    if not isinstance(rows, list):
        raise ValueError("rows must be a list")
    grouped_rows: dict[tuple[str, str, str], dict[str, Any]] = {}
    for row in rows:
        group_key = (str(row["template_variant_id"]), str(row["template_family"]), str(row["selected_bucket"]))
        if group_key not in grouped_rows:
            grouped_rows[group_key] = {
                "template_variant_id": group_key[0],
                "template_family": group_key[1],
                "selected_bucket": group_key[2],
                "planned": 0,
                "sent": 0,
                "replied": 0,
                "converted": 0,
            }
        grouped_rows[group_key]["planned"] += int(row["planned"])
        grouped_rows[group_key]["sent"] += int(row["sent"])
        grouped_rows[group_key]["replied"] += int(row["replied"])
        grouped_rows[group_key]["converted"] += int(row["converted"])

    summary = list(grouped_rows.values())
    for row in summary:
        row["reply_rate"] = _safe_rate(int(row["replied"]), int(row["sent"]))
        row["conversion_rate"] = _safe_rate(int(row["converted"]), int(row["sent"]))
    summary.sort(key=lambda row: (row["template_variant_id"], row["template_family"], row["selected_bucket"]))
    return summary


def build_signal_summary(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    if not isinstance(rows, list):
        raise ValueError("rows must be a list")
    grouped_rows: dict[tuple[str, str], dict[str, Any]] = {}
    for row in rows:
        group_key = (str(row["template_family"]), str(row["selected_bucket"]))
        if group_key not in grouped_rows:
            grouped_rows[group_key] = {
                "template_family": group_key[0],
                "selected_bucket": group_key[1],
                "planned": 0,
                "sent": 0,
                "replied": 0,
                "bounced": 0,
                "converted": 0,
            }
        grouped_rows[group_key]["planned"] += int(row["planned"])
        grouped_rows[group_key]["sent"] += int(row["sent"])
        grouped_rows[group_key]["replied"] += int(row["replied"])
        grouped_rows[group_key]["bounced"] += int(row["bounced"])
        grouped_rows[group_key]["converted"] += int(row["converted"])

    summary = list(grouped_rows.values())
    summary.sort(key=lambda row: (row["template_family"], row["selected_bucket"]))
    return summary


def build_role_summary(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    if not isinstance(rows, list):
        raise ValueError("rows must be a list")
    grouped_rows: dict[tuple[str, str], dict[str, Any]] = {}
    for row in rows:
        group_key = (str(row["role_confidence"]), str(row["contact_title_group"]))
        if group_key not in grouped_rows:
            grouped_rows[group_key] = {
                "role_confidence": group_key[0],
                "contact_title_group": group_key[1],
                "planned": 0,
                "sent": 0,
                "replied": 0,
                "converted": 0,
            }
        grouped_rows[group_key]["planned"] += int(row["planned"])
        grouped_rows[group_key]["sent"] += int(row["sent"])
        grouped_rows[group_key]["replied"] += int(row["replied"])
        grouped_rows[group_key]["converted"] += int(row["converted"])

    summary = list(grouped_rows.values())
    summary.sort(key=lambda row: (row["role_confidence"], row["contact_title_group"]))
    return summary


def build_simple_analytics_report(
    *,
    optimized_message_drafts: list[object],
    execution_plan_records: list[object],
    state_records: list[object],
    conversion_signal_records: list[object],
    conversion_entries: list[object],
) -> dict[str, Any]:
    normalized_inputs = normalize_analytics_inputs(
        optimized_message_drafts=optimized_message_drafts,
        execution_plan_records=execution_plan_records,
        state_records=state_records,
        conversion_signal_records=conversion_signal_records,
        conversion_entries=conversion_entries,
    )
    analytics_rows = _build_analytics_rows(normalized_inputs)
    return {
        "campaign_summary": build_campaign_summary(analytics_rows),
        "variant_summary": build_variant_summary(analytics_rows),
        "signal_summary": build_signal_summary(analytics_rows),
        "role_summary": build_role_summary(analytics_rows),
    }
