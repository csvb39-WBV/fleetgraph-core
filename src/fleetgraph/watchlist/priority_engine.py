from __future__ import annotations


_HIGH_URGENCY_TERMS = (
    "lawsuit",
    "sued",
    "litigation",
    "investigation",
    "audit",
    "subpoena",
    "default",
    "delay",
    "dispute",
)


def _normalized_text(value: object) -> str:
    return str(value or "").strip().lower()


def score_watchlist_company(
    company_record: dict[str, object],
    *,
    delta_summary: dict[str, object] | None = None,
    reference_date: str | None = None,
) -> dict[str, object]:
    score = 0
    reason_codes: list[str] = []

    priority_tier = _normalized_text(company_record.get("priority_tier"))
    if priority_tier == "1":
        score += 40
        reason_codes.append("priority_tier_1")
    elif priority_tier == "2":
        score += 25
        reason_codes.append("priority_tier_2")
    elif priority_tier == "3":
        score += 15
        reason_codes.append("priority_tier_3")
    else:
        score += 5
        reason_codes.append("priority_tier_unranked")

    verification_status = _normalized_text(company_record.get("verification_status"))
    if "verified" in verification_status:
        score += 10
        reason_codes.append("verification_verified")

    enrichment_state = _normalized_text(company_record.get("enrichment_state"))
    if enrichment_state == "enriched":
        score += 20
        reason_codes.append("enrichment_enriched")
    elif enrichment_state == "partial":
        score += 12
        reason_codes.append("enrichment_partial")
    else:
        score += 6
        reason_codes.append("enrichment_seed_only")

    recent_signals = list(company_record.get("recent_signals", []))
    recent_signal_count = len(recent_signals)
    if recent_signal_count > 0:
        score += min(recent_signal_count * 4, 16)
        reason_codes.append("recent_signals_present")

    if any(
        any(term in _normalized_text(signal.get("event_summary")) for term in _HIGH_URGENCY_TERMS)
        for signal in recent_signals
        if isinstance(signal, dict)
    ):
        score += 12
        reason_codes.append("high_urgency_signal_present")

    if delta_summary is not None and bool(delta_summary.get("change_detected")):
        score += 14
        reason_codes.append("changed_since_last_refresh")
        if int(delta_summary.get("new_signal_count", 0)) > 0:
            score += 10
            reason_codes.append("new_signals_detected")
        if int(delta_summary.get("new_email_count", 0)) > 0:
            score += 8
            reason_codes.append("new_public_email_detected")
        if int(delta_summary.get("new_key_people_count", 0)) > 0:
            score += 6
            reason_codes.append("new_key_person_detected")

    last_enriched_at = company_record.get("last_enriched_at")
    if last_enriched_at is None:
        score += 8
        reason_codes.append("never_refreshed")
    elif reference_date is not None and str(last_enriched_at) != str(reference_date):
        score += 4
        reason_codes.append("not_refreshed_in_current_run")

    if enrichment_state == "seed_only" and priority_tier == "1":
        score += 12
        reason_codes.append("high_priority_seed_only")

    artifact_status = _normalized_text(company_record.get("artifact_status"))
    if artifact_status == "invalid_artifact":
        score += 18
        reason_codes.append("invalid_artifact_state")

    if score >= 90:
        priority_band = "critical"
    elif score >= 65:
        priority_band = "high"
    elif score >= 40:
        priority_band = "medium"
    else:
        priority_band = "low"

    return {
        "priority_score": score,
        "priority_band": priority_band,
        "priority_reason_codes": reason_codes,
    }


def derive_needs_review(
    company_record: dict[str, object],
    *,
    delta_summary: dict[str, object] | None = None,
) -> dict[str, object]:
    review_reason_codes: list[str] = []
    enrichment_state = _normalized_text(company_record.get("enrichment_state"))
    recent_signal_count = len(list(company_record.get("recent_signals", [])))
    priority_tier = _normalized_text(company_record.get("priority_tier"))
    artifact_status = _normalized_text(company_record.get("artifact_status"))
    last_enriched_at = company_record.get("last_enriched_at")

    if delta_summary is not None and bool(delta_summary.get("change_detected")):
        review_reason_codes.append("changed_since_last_refresh")
    if enrichment_state == "partial" and recent_signal_count > 0:
        review_reason_codes.append("partial_with_active_signals")
    if enrichment_state == "enriched" and delta_summary is not None and int(delta_summary.get("new_signal_count", 0)) > 0:
        review_reason_codes.append("enriched_with_new_signals")
    if artifact_status == "invalid_artifact":
        review_reason_codes.append("invalid_artifact_state")
    if enrichment_state == "seed_only" and priority_tier == "1" and last_enriched_at is None:
        review_reason_codes.append("high_priority_seed_only")

    return {
        "needs_review": len(review_reason_codes) > 0,
        "review_reason_codes": review_reason_codes,
    }
