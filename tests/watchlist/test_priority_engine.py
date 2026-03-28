from __future__ import annotations

import pathlib
import sys


PROJECT_ROOT = pathlib.Path(__file__).resolve().parents[2]
SRC_ROOT = PROJECT_ROOT / "src"

if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))


from fleetgraph.watchlist.priority_engine import derive_needs_review, score_watchlist_company


def _company(**overrides: object) -> dict[str, object]:
    company = {
        "company_id": "turner-construction--ny",
        "company_name": "Turner Construction",
        "priority_tier": "1",
        "verification_status": "verified",
        "enrichment_state": "partial",
        "recent_signals": [],
        "last_enriched_at": None,
        "artifact_status": "missing_artifact",
    }
    company.update(overrides)
    return company


def test_priority_scoring_is_deterministic_and_explainable() -> None:
    company = _company(
        enrichment_state="enriched",
        recent_signals=[{"event_summary": "Lawsuit filed over delay"}],
        last_enriched_at="2026-03-28",
        artifact_status="ok",
    )
    delta_summary = {
        "change_detected": True,
        "new_signal_count": 1,
        "new_email_count": 1,
        "new_key_people_count": 1,
    }

    first = score_watchlist_company(company, delta_summary=delta_summary, reference_date="2026-03-28")
    second = score_watchlist_company(company, delta_summary=delta_summary, reference_date="2026-03-28")

    assert first == second
    assert first["priority_score"] > 0
    assert first["priority_band"] in {"critical", "high", "medium", "low"}
    assert first["priority_reason_codes"] == [
        "priority_tier_1",
        "verification_verified",
        "enrichment_enriched",
        "recent_signals_present",
        "high_urgency_signal_present",
        "changed_since_last_refresh",
        "new_signals_detected",
        "new_public_email_detected",
        "new_key_person_detected",
    ]


def test_needs_review_derivation_is_rule_based_and_deterministic() -> None:
    company = _company(
        enrichment_state="partial",
        recent_signals=[{"event_summary": "Investigation announced"}],
    )
    delta_summary = {
        "change_detected": True,
        "new_signal_count": 1,
    }

    first = derive_needs_review(company, delta_summary=delta_summary)
    second = derive_needs_review(company, delta_summary=delta_summary)

    assert first == second
    assert first == {
        "needs_review": True,
        "review_reason_codes": [
            "changed_since_last_refresh",
            "partial_with_active_signals",
        ],
    }


def test_invalid_artifact_and_high_priority_seed_only_require_review() -> None:
    invalid_company = _company(artifact_status="invalid_artifact")

    review = derive_needs_review(invalid_company, delta_summary=None)
    priority = score_watchlist_company(invalid_company, delta_summary=None, reference_date="2026-03-28")

    assert review["review_reason_codes"] == [
        "invalid_artifact_state",
        "high_priority_seed_only",
    ]
    assert "invalid_artifact_state" in priority["priority_reason_codes"]
    assert "high_priority_seed_only" in priority["priority_reason_codes"]
