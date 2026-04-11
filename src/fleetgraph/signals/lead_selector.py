from __future__ import annotations

from datetime import date, datetime
from typing import Any

from fleetgraph.signals.signal_normalizer import normalize_signal_batch
from fleetgraph.signals.time_bucket import assign_signal_bucket

__all__ = [
    "select_phased_leads",
]

_TIER_ORDER = ("T1", "T2", "T3A", "T3B", "T3C")
_TIER_RANK = {
    "T1": 0,
    "T2": 1,
    "T3A": 2,
    "T3B": 3,
    "T3C": 4,
}
_STRONG_HISTORICAL_SIGNAL_PATTERNS = (
    "lawsuit",
    "litigation",
    "court docket",
    "docket",
    "mechanics lien",
    "lien",
    "regulatory enforcement",
    "enforcement",
    "bond claim",
)
_SIGNAL_STRENGTH_PATTERNS = (
    ("bond claim", 0),
    ("mechanics lien", 1),
    ("regulatory enforcement", 2),
    ("enforcement", 3),
    ("court docket", 4),
    ("lawsuit", 5),
    ("litigation", 6),
    ("docket", 7),
    ("lien", 8),
)


def _coerce_to_date(value: object, *, field_name: str) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    raise ValueError(f"{field_name} must be a date or datetime")


def _normalized_text(value: str) -> str:
    return " ".join(value.strip().lower().split())


def _signal_strength_rank(signal: dict[str, Any]) -> int:
    combined_text = _normalized_text(
        f"{signal['signal_type']} {signal['signal_detail']}"
    )
    for pattern, rank in _SIGNAL_STRENGTH_PATTERNS:
        if pattern in combined_text:
            return rank
    return 999


def _is_strong_historical_signal(signal: dict[str, Any]) -> bool:
    combined_text = _normalized_text(
        f"{signal['signal_type']} {signal['signal_detail']}"
    )
    return any(pattern in combined_text for pattern in _STRONG_HISTORICAL_SIGNAL_PATTERNS)


def _selection_sort_key(
    signal: dict[str, Any],
    *,
    reference_date: date,
) -> tuple[int, int, str, str, str, str]:
    bucket = assign_signal_bucket(signal["event_date"], reference_date)
    if bucket is None:
        bucket_rank = 999
    else:
        bucket_rank = _TIER_RANK[bucket]
    normalized_company_name = _normalized_text(str(signal["company_name"]))
    normalized_company_id = _normalized_text(str(signal["company_id"]))
    normalized_signal_type = _normalized_text(str(signal["signal_type"]))
    event_date = _coerce_to_date(signal["event_date"], field_name="event_date")
    return (
        bucket_rank,
        _signal_strength_rank(signal),
        f"{999999999 - event_date.toordinal():09d}",
        normalized_company_name,
        normalized_company_id,
        normalized_signal_type,
    )


def _prior_contact_map(
    prior_contact_records: list[dict[str, Any]] | None,
) -> dict[str, date]:
    if prior_contact_records is None:
        return {}
    if not isinstance(prior_contact_records, list):
        raise ValueError("prior_contact_records must be a list or None")

    normalized_contact_map: dict[str, date] = {}
    for index, record in enumerate(prior_contact_records):
        if not isinstance(record, dict):
            raise ValueError(f"prior_contact_records[{index}] must be a dict")

        company_id = record.get("company_id")
        if not isinstance(company_id, str) or company_id.strip() == "":
            raise ValueError(f"prior_contact_records[{index}].company_id must be a non-empty string")

        contacted_at = record.get("contacted_at")
        if contacted_at is None:
            contacted_at = record.get("last_contacted_at")
        contacted_date = _coerce_to_date(contacted_at, field_name=f"prior_contact_records[{index}].contacted_at")
        normalized_contact_map[company_id.strip()] = contacted_date
    return normalized_contact_map


def _is_in_cooldown(
    company_id: str,
    *,
    contact_dates_by_company_id: dict[str, date],
    reference_date: date,
    cooldown_days: int,
) -> bool:
    if company_id not in contact_dates_by_company_id:
        return False
    last_contact_date = contact_dates_by_company_id[company_id]
    delta_days = (reference_date - last_contact_date).days
    if delta_days < 0:
        return True
    return delta_days <= cooldown_days


def _build_selected_lead(
    signal: dict[str, Any],
    *,
    bucket: str,
) -> dict[str, Any]:
    return {
        "company_id": str(signal["company_id"]),
        "company_name": str(signal["company_name"]),
        "selected_bucket": bucket,
        "signal_type": str(signal["signal_type"]),
        "signal_detail": str(signal["signal_detail"]),
        "event_date": signal["event_date"],
        "source_url": signal["source_url"],
    }


def select_phased_leads(
    signal_records: list[object],
    *,
    reference_date: date | datetime,
    max_batch_size: int = 25,
    cooldown_days: int = 45,
    prior_contact_records: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    if max_batch_size <= 0:
        raise ValueError("max_batch_size must be greater than 0")
    if cooldown_days < 0:
        raise ValueError("cooldown_days must be greater than or equal to 0")

    normalized_reference_date = _coerce_to_date(reference_date, field_name="reference_date")
    normalized_signals = normalize_signal_batch(signal_records)
    contact_dates_by_company_id = _prior_contact_map(prior_contact_records)

    eligible_signals_by_tier: dict[str, list[dict[str, Any]]] = {tier: [] for tier in _TIER_ORDER}
    skipped_company_ids_by_reason = {
        "cooldown": [],
        "historical_gate": [],
        "out_of_window": [],
    }

    for signal in normalized_signals:
        bucket = assign_signal_bucket(signal["event_date"], normalized_reference_date)
        if bucket is None:
            skipped_company_ids_by_reason["out_of_window"].append(str(signal["company_id"]))
            continue
        if bucket in ("T3B", "T3C") and _is_strong_historical_signal(signal) is not True:
            skipped_company_ids_by_reason["historical_gate"].append(str(signal["company_id"]))
            continue
        if _is_in_cooldown(
            str(signal["company_id"]),
            contact_dates_by_company_id=contact_dates_by_company_id,
            reference_date=normalized_reference_date,
            cooldown_days=cooldown_days,
        ):
            skipped_company_ids_by_reason["cooldown"].append(str(signal["company_id"]))
            continue
        eligible_signals_by_tier[bucket].append(signal)

    for tier in _TIER_ORDER:
        eligible_signals_by_tier[tier].sort(
            key=lambda signal: _selection_sort_key(
                signal,
                reference_date=normalized_reference_date,
            )
        )

    selected_leads: list[dict[str, Any]] = []
    selected_company_ids: set[str] = set()
    tiers_evaluated: list[str] = []

    for tier in _TIER_ORDER:
        tiers_evaluated.append(tier)
        for signal in eligible_signals_by_tier[tier]:
            company_id = str(signal["company_id"])
            if company_id in selected_company_ids:
                continue
            selected_leads.append(_build_selected_lead(signal, bucket=tier))
            selected_company_ids.add(company_id)
            if len(selected_leads) >= max_batch_size:
                break
        if len(selected_leads) >= max_batch_size:
            break

    remaining_capacity = max_batch_size - len(selected_leads)
    return {
        "selected_leads": selected_leads,
        "tiers_evaluated": tiers_evaluated,
        "eligible_counts_by_tier": {
            tier: len(eligible_signals_by_tier[tier])
            for tier in _TIER_ORDER
        },
        "skipped_company_ids_by_reason": {
            reason: sorted(set(company_ids))
            for reason, company_ids in skipped_company_ids_by_reason.items()
        },
        "remaining_capacity": remaining_capacity,
        "icp_fallback": {
            "eligible": remaining_capacity > 0,
            "next_phase": "ICP_FALLBACK",
            "reason": "signal_tiers_exhausted" if remaining_capacity > 0 else "batch_full",
        },
    }
