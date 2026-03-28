from __future__ import annotations

import json


_CHANGE_TYPE_ORDER = (
    "missing_previous_artifact",
    "missing_current_artifact",
    "enrichment_state_changed",
    "new_signals_added",
    "new_projects_added",
    "new_public_emails_added",
    "new_key_people_added",
    "confidence_level_changed",
    "last_enriched_at_changed",
    "source_link_count_changed",
)


def _stable_key(item: object) -> str:
    return json.dumps(item, sort_keys=True, separators=(",", ":"))


def _new_item_count(previous_items: object, current_items: object) -> int:
    previous_list = [] if not isinstance(previous_items, list) else previous_items
    current_list = [] if not isinstance(current_items, list) else current_items
    previous_set = {_stable_key(item) for item in previous_list}
    current_set = {_stable_key(item) for item in current_list}
    return len(current_set - previous_set)


def build_company_delta_summary(
    previous_company: dict[str, object] | None,
    current_company: dict[str, object] | None,
) -> dict[str, object]:
    company_id = None
    company_name = None
    if current_company is not None:
        company_id = current_company.get("company_id")
        company_name = current_company.get("company_name")
    elif previous_company is not None:
        company_id = previous_company.get("company_id")
        company_name = previous_company.get("company_name")

    change_types: list[str] = []
    if previous_company is None:
        change_types.append("missing_previous_artifact")
    if current_company is None:
        change_types.append("missing_current_artifact")

    previous_enrichment_state = None if previous_company is None else previous_company.get("enrichment_state")
    current_enrichment_state = None if current_company is None else current_company.get("enrichment_state")

    if previous_company is not None and current_company is not None:
        if previous_enrichment_state != current_enrichment_state:
            change_types.append("enrichment_state_changed")

    new_signal_count = 0 if current_company is None else _new_item_count(
        [] if previous_company is None else previous_company.get("recent_signals", []),
        current_company.get("recent_signals", []),
    )
    if new_signal_count > 0:
        change_types.append("new_signals_added")

    new_project_count = 0 if current_company is None else _new_item_count(
        [] if previous_company is None else previous_company.get("recent_projects", []),
        current_company.get("recent_projects", []),
    )
    if new_project_count > 0:
        change_types.append("new_projects_added")

    new_email_count = 0 if current_company is None else _new_item_count(
        [] if previous_company is None else previous_company.get("published_emails", []),
        current_company.get("published_emails", []),
    )
    if new_email_count > 0:
        change_types.append("new_public_emails_added")

    new_key_people_count = 0 if current_company is None else _new_item_count(
        [] if previous_company is None else previous_company.get("key_people", []),
        current_company.get("key_people", []),
    )
    if new_key_people_count > 0:
        change_types.append("new_key_people_added")

    previous_confidence = None if previous_company is None else previous_company.get("confidence_level")
    current_confidence = None if current_company is None else current_company.get("confidence_level")
    confidence_changed = previous_confidence != current_confidence
    if previous_company is not None and current_company is not None and confidence_changed:
        change_types.append("confidence_level_changed")

    previous_last_enriched_at = None if previous_company is None else previous_company.get("last_enriched_at")
    current_last_enriched_at = None if current_company is None else current_company.get("last_enriched_at")
    if previous_last_enriched_at != current_last_enriched_at:
        change_types.append("last_enriched_at_changed")

    previous_source_link_count = 0 if previous_company is None else len(list(previous_company.get("source_links", [])))
    current_source_link_count = 0 if current_company is None else len(list(current_company.get("source_links", [])))
    if previous_source_link_count != current_source_link_count:
        change_types.append("source_link_count_changed")

    ordered_change_types = [
        change_type for change_type in _CHANGE_TYPE_ORDER if change_type in set(change_types)
    ]

    return {
        "company_id": company_id,
        "company_name": company_name,
        "change_detected": len(ordered_change_types) > 0,
        "change_types": ordered_change_types,
        "previous_enrichment_state": previous_enrichment_state,
        "current_enrichment_state": current_enrichment_state,
        "new_signal_count": new_signal_count,
        "new_project_count": new_project_count,
        "new_email_count": new_email_count,
        "new_key_people_count": new_key_people_count,
        "confidence_changed": confidence_changed,
        "last_enriched_at": current_last_enriched_at,
        "priority_score": 0,
        "priority_reason_codes": [],
    }
