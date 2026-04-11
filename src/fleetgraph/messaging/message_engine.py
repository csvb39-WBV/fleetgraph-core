from __future__ import annotations

from copy import deepcopy
from datetime import date, datetime
from typing import Any

__all__ = [
    "generate_message_drafts",
    "resolve_signal_family",
]

_REQUIRED_PROSPECT_FIELDS = (
    "prospect_id",
    "company_id",
    "company_name",
    "selected_bucket",
    "signal_type",
    "signal_detail",
    "event_date",
    "source_url",
    "contacts",
)
_REQUIRED_CONTACT_FIELDS = (
    "name",
    "title",
    "email",
    "priority_rank",
)
_SIGNAL_FAMILY_PATTERNS = (
    (("lawsuit", "litigation", "docket", "court"), "litigation"),
    (("lien", "payment", "bond claim", "bond_claim"), "payment"),
    (("enforcement", "regulatory", "osha", "safety"), "enforcement"),
)
_TIER_FRAMING = {
    "T1": {
        "context": "active",
        "opening": "Active issue handling usually puts document response workflows under immediate pressure.",
        "relevance": "Teams in that position often need a cleaner way to collect, review, and turn around records without extra follow-up.",
        "subject_generic": "Quick question for {company_name}",
        "subject_litigation": "Quick question for {company_name}",
        "subject_payment": "Question about project documentation at {company_name}",
        "subject_enforcement": "Question regarding documentation workflows at {company_name}",
    },
    "T2": {
        "context": "recent",
        "opening": "A recent issue often exposes where document requests and response handoffs slow teams down.",
        "relevance": "That is usually the moment when a simpler workflow for assembling and sharing records becomes useful.",
        "subject_generic": "Following up on a recent issue at {company_name}",
        "subject_litigation": "Question after a recent matter at {company_name}",
        "subject_payment": "Question after a recent payment issue at {company_name}",
        "subject_enforcement": "Question after a recent compliance issue at {company_name}",
    },
    "T3A": {
        "context": "prior",
        "opening": "A prior issue often makes hindsight around documentation and response workflows especially concrete.",
        "relevance": "We usually speak with teams that want a more repeatable way to prepare records before the next request hits.",
        "subject_generic": "Question based on prior workflow pressure at {company_name}",
        "subject_litigation": "Question based on prior matter workflows at {company_name}",
        "subject_payment": "Question based on prior payment documentation at {company_name}",
        "subject_enforcement": "Question based on prior compliance workflows at {company_name}",
    },
    "T3B": {
        "context": "pattern",
        "opening": "Over time, repeated issues usually point to a broader pattern in how records are organized and delivered.",
        "relevance": "That pattern is where standardized intake, search, and response workflows can remove recurring friction.",
        "subject_generic": "Question about workflow patterns at {company_name}",
        "subject_litigation": "Question about matter response patterns at {company_name}",
        "subject_payment": "Question about payment documentation patterns at {company_name}",
        "subject_enforcement": "Question about compliance response patterns at {company_name}",
    },
    "T3C": {
        "context": "structural",
        "opening": "Longer-term exposure usually turns documentation readiness into a structural risk rather than a one-off task.",
        "relevance": "That is where teams tend to value a repeatable system for collecting, organizing, and producing records.",
        "subject_generic": "Question about documentation readiness at {company_name}",
        "subject_litigation": "Question about matter readiness at {company_name}",
        "subject_payment": "Question about payment record readiness at {company_name}",
        "subject_enforcement": "Question about compliance readiness at {company_name}",
    },
}
_FAMILY_OPENINGS = {
    "litigation": "Your team may already be familiar with legal or case-related document requests.",
    "payment": "Payment and lien issues usually create fast-moving requests for contracts, change orders, and backup.",
    "enforcement": "Enforcement and safety reviews often depend on having the right records ready without delay.",
    "generic": "When documentation pressure appears, teams usually need a faster way to pull together the right records.",
}
_CLOSE = "Best,\nFleetGraph"


def _normalize_non_empty_string(value: object, field_name: str) -> str:
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


def _coerce_event_date(value: object, field_name: str) -> date | datetime:
    if isinstance(value, datetime):
        return value
    if isinstance(value, date):
        return value
    raise ValueError(f"{field_name} must be a date or datetime")


def _normalized_signal_text(signal_type: str, signal_detail: str) -> str:
    return f"{signal_type} {signal_detail}".strip().lower()


def resolve_signal_family(*, signal_type: str, signal_detail: str) -> str:
    normalized_text = _normalized_signal_text(signal_type, signal_detail)
    for patterns, family in _SIGNAL_FAMILY_PATTERNS:
        if any(pattern in normalized_text for pattern in patterns):
            return family
    return "generic"


def _validate_contact(contact: object, prospect_index: int, contact_index: int) -> dict[str, Any]:
    if not isinstance(contact, dict):
        raise ValueError(f"prospects[{prospect_index}].contacts[{contact_index}] must be a dict")

    normalized_contact: dict[str, Any] = {}
    for field_name in _REQUIRED_CONTACT_FIELDS:
        if field_name not in contact:
            raise ValueError(
                f"prospects[{prospect_index}].contacts[{contact_index}] missing required field: {field_name}"
            )

    for field_name in ("name", "title", "email"):
        normalized_contact[field_name] = _normalize_non_empty_string(
            contact.get(field_name),
            f"prospects[{prospect_index}].contacts[{contact_index}].{field_name}",
        )

    priority_rank = contact.get("priority_rank")
    if isinstance(priority_rank, bool) or not isinstance(priority_rank, int):
        raise ValueError(
            f"prospects[{prospect_index}].contacts[{contact_index}].priority_rank must be an int"
        )
    normalized_contact["priority_rank"] = priority_rank
    return normalized_contact


def _validate_prospect(prospect: object, index: int) -> dict[str, Any]:
    if not isinstance(prospect, dict):
        raise ValueError(f"prospects[{index}] must be a dict")

    normalized_prospect: dict[str, Any] = {}
    for field_name in _REQUIRED_PROSPECT_FIELDS:
        if field_name not in prospect:
            raise ValueError(f"prospects[{index}] missing required field: {field_name}")

    for field_name in (
        "prospect_id",
        "company_id",
        "company_name",
        "selected_bucket",
        "signal_type",
        "signal_detail",
    ):
        normalized_prospect[field_name] = _normalize_non_empty_string(
            prospect.get(field_name),
            f"prospects[{index}].{field_name}",
        )

    normalized_prospect["event_date"] = _coerce_event_date(
        prospect.get("event_date"),
        f"prospects[{index}].event_date",
    )
    normalized_prospect["source_url"] = _normalize_optional_string(prospect.get("source_url"))
    normalized_prospect["project_name"] = _normalize_optional_string(prospect.get("project_name"))

    contacts = prospect.get("contacts")
    if not isinstance(contacts, list):
        raise ValueError(f"prospects[{index}].contacts must be a list")
    normalized_prospect["contacts"] = [
        _validate_contact(contact, index, contact_index)
        for contact_index, contact in enumerate(contacts)
    ]
    return normalized_prospect


def _subject_for(prospect: dict[str, Any], signal_family: str) -> str:
    tier_config = _TIER_FRAMING[str(prospect["selected_bucket"])]
    subject_template = tier_config.get(f"subject_{signal_family}")
    if subject_template is None:
        subject_template = tier_config["subject_generic"]
    return subject_template.format(company_name=prospect["company_name"])


def _signal_line(prospect: dict[str, Any], signal_family: str) -> str:
    family_line = _FAMILY_OPENINGS[signal_family]
    signal_detail = str(prospect["signal_detail"])
    return f"{family_line} The signal in view was: {signal_detail}."


def _project_line(prospect: dict[str, Any]) -> str | None:
    project_name = prospect.get("project_name")
    if project_name is None:
        return None
    return f"If {project_name} is still a useful internal reference point, that context can help prioritize the right records quickly."


def _cta_line(contact: dict[str, Any]) -> str:
    return f"If this is relevant on your side, the easiest next step is usually to review the self-serve workflow your team could use to evaluate it directly."


def _render_body(prospect: dict[str, Any], contact: dict[str, Any], signal_family: str) -> str:
    tier_config = _TIER_FRAMING[str(prospect["selected_bucket"])]
    lines = [
        f"Hi {contact['name']},",
        "",
        _signal_line(prospect, signal_family),
        tier_config["opening"],
        tier_config["relevance"],
        "FleetGraph helps teams keep matter and project documentation organized so requests, reviews, and handoffs take less manual chasing.",
    ]
    project_line = _project_line(prospect)
    if project_line is not None:
        lines.append(project_line)
    lines.extend(
        [
            _cta_line(contact),
            "",
            _CLOSE,
        ]
    )
    body = "\n".join(lines)
    if "{{" in body or "}}" in body:
        raise ValueError("unresolved placeholders remain in body")
    return body


def generate_message_drafts(prospects: list[object]) -> list[dict[str, Any]]:
    if not isinstance(prospects, list):
        raise ValueError("prospects must be a list")

    normalized_prospects = [
        _validate_prospect(prospect, index)
        for index, prospect in enumerate(prospects)
    ]

    drafts: list[dict[str, Any]] = []
    for prospect in normalized_prospects:
        signal_family = resolve_signal_family(
            signal_type=str(prospect["signal_type"]),
            signal_detail=str(prospect["signal_detail"]),
        )
        subject = _subject_for(prospect, signal_family)
        if "{{" in subject or "}}" in subject:
            raise ValueError("unresolved placeholders remain in subject")
        for contact in prospect["contacts"]:
            preserved_contact = deepcopy(contact)
            drafts.append(
                {
                    "prospect_id": prospect["prospect_id"],
                    "company_id": prospect["company_id"],
                    "company_name": prospect["company_name"],
                    "contact": preserved_contact,
                    "selected_bucket": prospect["selected_bucket"],
                    "signal_type": prospect["signal_type"],
                    "signal_detail": prospect["signal_detail"],
                    "subject": subject,
                    "body": _render_body(prospect, preserved_contact, signal_family),
                }
            )

    return drafts
