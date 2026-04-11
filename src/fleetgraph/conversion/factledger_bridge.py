from __future__ import annotations

import hashlib
import json
from copy import deepcopy
from typing import Any

from fleetgraph.messaging.message_engine import resolve_signal_family

__all__ = [
    "build_conversion_entries",
    "build_landing_reference",
    "build_recommended_handoff",
    "resolve_landing_path",
]

_REQUIRED_PLAN_FIELDS = (
    "draft_id",
    "prospect_id",
    "company_id",
    "company_name",
    "contact_email",
    "contact_name",
    "sequence_step",
    "send_window",
)
_REQUIRED_DRAFT_FIELDS = (
    "prospect_id",
    "company_id",
    "company_name",
    "contact",
    "selected_bucket",
    "signal_type",
    "signal_detail",
)
_BLOCKED_STATE_STATUSES = {
    "REPLIED",
    "BOUNCED",
    "UNSUBSCRIBED",
    "SUPPRESSED",
}
_LANDING_PATHS = {
    "litigation": "/litigation-case",
    "payment": "/payment-dispute",
    "enforcement": "/compliance-review",
    "generic": "/general-review",
}
_HANDOFF_TEMPLATES = {
    "litigation": {
        "intake_type": "claims_evidence_reconstruction",
        "recommended_flow": "self_serve_claims_record_review",
        "source_context": "litigation_signal",
    },
    "payment": {
        "intake_type": "payment_support_review",
        "recommended_flow": "self_serve_payment_record_review",
        "source_context": "payment_signal",
    },
    "enforcement": {
        "intake_type": "compliance_document_review",
        "recommended_flow": "self_serve_compliance_record_review",
        "source_context": "enforcement_signal",
    },
    "generic": {
        "intake_type": "general_document_review",
        "recommended_flow": "self_serve_general_record_review",
        "source_context": "generic_signal",
    },
}


def _normalize_non_empty_string(value: object, *, field_name: str) -> str:
    if not isinstance(value, str):
        raise ValueError(f"{field_name} must be a non-empty string")
    normalized_value = value.strip()
    if normalized_value == "":
        raise ValueError(f"{field_name} must be a non-empty string")
    return normalized_value


def _normalize_optional_string(value: object) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise ValueError("optional string fields must be strings or None")
    normalized_value = value.strip()
    if normalized_value == "":
        return None
    return normalized_value


def _normalize_sequence_step(value: object, *, field_name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"{field_name} must be an int")
    if value <= 0:
        raise ValueError(f"{field_name} must be greater than 0")
    return value


def _normalize_contact(contact: object, *, location: str) -> dict[str, Any]:
    if not isinstance(contact, dict):
        raise ValueError(f"{location} must be a dict")
    return {
        "name": _normalize_non_empty_string(contact.get("name"), field_name=f"{location}.name"),
        "email": _normalize_non_empty_string(contact.get("email"), field_name=f"{location}.email").lower(),
    }


def _normalize_plan_record(plan_record: object, *, index: int) -> dict[str, Any]:
    if not isinstance(plan_record, dict):
        raise ValueError(f"execution_plan[{index}] must be a dict")
    for field_name in _REQUIRED_PLAN_FIELDS:
        if field_name not in plan_record:
            raise ValueError(f"execution_plan[{index}] missing required field: {field_name}")
    return {
        "draft_id": _normalize_non_empty_string(plan_record.get("draft_id"), field_name=f"execution_plan[{index}].draft_id"),
        "prospect_id": _normalize_non_empty_string(plan_record.get("prospect_id"), field_name=f"execution_plan[{index}].prospect_id"),
        "company_id": _normalize_non_empty_string(plan_record.get("company_id"), field_name=f"execution_plan[{index}].company_id"),
        "company_name": _normalize_non_empty_string(plan_record.get("company_name"), field_name=f"execution_plan[{index}].company_name"),
        "contact_email": _normalize_non_empty_string(plan_record.get("contact_email"), field_name=f"execution_plan[{index}].contact_email").lower(),
        "contact_name": _normalize_non_empty_string(plan_record.get("contact_name"), field_name=f"execution_plan[{index}].contact_name"),
        "sequence_step": _normalize_sequence_step(plan_record.get("sequence_step"), field_name=f"execution_plan[{index}].sequence_step"),
        "send_window": _normalize_non_empty_string(plan_record.get("send_window"), field_name=f"execution_plan[{index}].send_window"),
    }


def _normalize_message_draft(message_draft: object, *, index: int) -> dict[str, Any]:
    if not isinstance(message_draft, dict):
        raise ValueError(f"message_drafts[{index}] must be a dict")
    for field_name in _REQUIRED_DRAFT_FIELDS:
        if field_name not in message_draft:
            raise ValueError(f"message_drafts[{index}] missing required field: {field_name}")
    return {
        "prospect_id": _normalize_non_empty_string(message_draft.get("prospect_id"), field_name=f"message_drafts[{index}].prospect_id"),
        "company_id": _normalize_non_empty_string(message_draft.get("company_id"), field_name=f"message_drafts[{index}].company_id"),
        "company_name": _normalize_non_empty_string(message_draft.get("company_name"), field_name=f"message_drafts[{index}].company_name"),
        "contact": _normalize_contact(message_draft.get("contact"), location=f"message_drafts[{index}].contact"),
        "selected_bucket": _normalize_non_empty_string(message_draft.get("selected_bucket"), field_name=f"message_drafts[{index}].selected_bucket"),
        "signal_type": _normalize_non_empty_string(message_draft.get("signal_type"), field_name=f"message_drafts[{index}].signal_type"),
        "signal_detail": _normalize_non_empty_string(message_draft.get("signal_detail"), field_name=f"message_drafts[{index}].signal_detail"),
    }


def _normalize_state_records(state_records: list[object] | None) -> dict[str, str]:
    if state_records is None:
        return {}
    if not isinstance(state_records, list):
        raise ValueError("state_records must be a list or None")
    normalized_status_by_draft_id: dict[str, str] = {}
    for index, record in enumerate(state_records):
        if not isinstance(record, dict):
            raise ValueError(f"state_records[{index}] must be a dict")
        draft_id = _normalize_non_empty_string(record.get("draft_id"), field_name=f"state_records[{index}].draft_id")
        status = _normalize_non_empty_string(record.get("status"), field_name=f"state_records[{index}].status")
        normalized_status_by_draft_id[draft_id] = status
    return normalized_status_by_draft_id


def resolve_landing_path(signal_family: str) -> str:
    normalized_signal_family = _normalize_non_empty_string(signal_family, field_name="signal_family").lower()
    return _LANDING_PATHS.get(normalized_signal_family, _LANDING_PATHS["generic"])


def build_recommended_handoff(signal_family: str, selected_bucket: str) -> dict[str, str]:
    normalized_signal_family = _normalize_non_empty_string(signal_family, field_name="signal_family").lower()
    normalized_selected_bucket = _normalize_non_empty_string(selected_bucket, field_name="selected_bucket")
    template = _HANDOFF_TEMPLATES.get(normalized_signal_family, _HANDOFF_TEMPLATES["generic"])
    return {
        "intake_type": str(template["intake_type"]),
        "recommended_flow": str(template["recommended_flow"]),
        "source_context": f"{template['source_context']}|{normalized_selected_bucket}",
    }


def build_landing_reference(landing_path: str, *, base_path: str | None = None) -> str:
    normalized_landing_path = _normalize_non_empty_string(landing_path, field_name="landing_path")
    normalized_base_path = _normalize_optional_string(base_path)
    if normalized_base_path is None:
        return normalized_landing_path
    base = normalized_base_path.rstrip("/")
    if base == "":
        return normalized_landing_path
    return f"{base}{normalized_landing_path}"


def _campaign_id(signal_family: str, selected_bucket: str, landing_path: str, campaign_key: str) -> str:
    digest_source = json.dumps(
        [campaign_key, signal_family, selected_bucket, landing_path],
        ensure_ascii=True,
        separators=(",", ":"),
    )
    digest = hashlib.sha256(digest_source.encode("utf-8")).hexdigest()[:16]
    return f"campaign:{digest}"


def _conversion_entry_id(values: dict[str, Any]) -> str:
    digest_source = json.dumps(values, ensure_ascii=True, separators=(",", ":"))
    digest = hashlib.sha256(digest_source.encode("utf-8")).hexdigest()[:16]
    return f"conversion:{digest}"


def _draft_lookup_key(*, prospect_id: str, company_id: str, contact_email: str) -> tuple[str, str, str]:
    return (prospect_id, company_id, contact_email.lower())


def _is_entry_eligible(plan_record: dict[str, Any], state_status_by_draft_id: dict[str, str]) -> bool:
    status = state_status_by_draft_id.get(str(plan_record["draft_id"]))
    if status is None:
        return True
    return status not in _BLOCKED_STATE_STATUSES


def build_conversion_entries(
    execution_plan: list[object],
    message_drafts: list[object],
    *,
    state_records: list[object] | None = None,
    base_path: str | None = None,
    campaign_key: str = "factledger-self-serve",
) -> list[dict[str, Any]]:
    if not isinstance(execution_plan, list):
        raise ValueError("execution_plan must be a list")
    if not isinstance(message_drafts, list):
        raise ValueError("message_drafts must be a list")

    normalized_plan_records = [
        _normalize_plan_record(plan_record, index=index)
        for index, plan_record in enumerate(execution_plan)
    ]
    normalized_message_drafts = [
        _normalize_message_draft(message_draft, index=index)
        for index, message_draft in enumerate(message_drafts)
    ]
    normalized_state_status_by_draft_id = _normalize_state_records(state_records)

    message_draft_by_key = {
        _draft_lookup_key(
            prospect_id=str(message_draft["prospect_id"]),
            company_id=str(message_draft["company_id"]),
            contact_email=str(message_draft["contact"]["email"]),
        ): message_draft
        for message_draft in normalized_message_drafts
    }

    conversion_entries: list[dict[str, Any]] = []
    for plan_record in normalized_plan_records:
        if _is_entry_eligible(plan_record, normalized_state_status_by_draft_id) is not True:
            continue
        message_draft = message_draft_by_key.get(
            _draft_lookup_key(
                prospect_id=str(plan_record["prospect_id"]),
                company_id=str(plan_record["company_id"]),
                contact_email=str(plan_record["contact_email"]),
            )
        )
        if message_draft is None:
            continue

        signal_family = resolve_signal_family(
            signal_type=str(message_draft["signal_type"]),
            signal_detail=str(message_draft["signal_detail"]),
        )
        landing_path = build_landing_reference(
            resolve_landing_path(signal_family),
            base_path=base_path,
        )
        campaign_id = _campaign_id(
            signal_family,
            str(message_draft["selected_bucket"]),
            landing_path,
            campaign_key,
        )
        attribution_seed = {
            "draft_id": str(plan_record["draft_id"]),
            "sequence_step": int(plan_record["sequence_step"]),
            "send_window": str(plan_record["send_window"]),
        }
        factledger_handoff = build_recommended_handoff(
            signal_family,
            str(message_draft["selected_bucket"]),
        )
        conversion_entry_payload = {
            "prospect_id": str(plan_record["prospect_id"]),
            "company_id": str(plan_record["company_id"]),
            "company_name": str(plan_record["company_name"]),
            "contact_email": str(plan_record["contact_email"]),
            "contact_name": str(plan_record["contact_name"]),
            "signal_family": signal_family,
            "selected_bucket": str(message_draft["selected_bucket"]),
            "campaign_id": campaign_id,
            "landing_path": landing_path,
            "entry_mode": "SELF_SERVE",
            "attribution_seed": attribution_seed,
            "factledger_handoff": factledger_handoff,
        }
        conversion_entry = deepcopy(conversion_entry_payload)
        conversion_entry["conversion_entry_id"] = _conversion_entry_id(
            {
                "prospect_id": conversion_entry_payload["prospect_id"],
                "contact_email": conversion_entry_payload["contact_email"],
                "campaign_id": conversion_entry_payload["campaign_id"],
                "attribution_seed": conversion_entry_payload["attribution_seed"],
                "landing_path": conversion_entry_payload["landing_path"],
            }
        )
        conversion_entries.append(conversion_entry)

    return conversion_entries
