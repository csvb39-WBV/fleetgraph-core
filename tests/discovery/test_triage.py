from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[2]
SRC = ROOT / "src"

if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from fleetgraph_core.discovery.triage import attach_triage, evaluate_priority


def test_high_priority_with_rank_assigned_correctly() -> None:
    records = [
        {
            "signal_id": "SIG-HIGH",
            "corroboration_level": "Strong",
            "relationship_type": "shared_domain",
            "link_count": 4,
            "organization_count": 3,
            "shared_domain_count": 2,
            "corroborating_types": ["Type A", "Type B", "Type C"],
        },
        {
            "signal_id": "SIG-MED",
            "corroboration_level": "Moderate",
            "relationship_type": "partner_reference",
            "link_count": 2,
            "organization_count": 2,
            "corroborating_types": ["Type A", "Type B"],
        },
    ]

    output = attach_triage(records)

    high = next(item for item in output if item["signal_id"] == "SIG-HIGH")
    assert high["priority_level"] == "High"
    assert high["priority_rank"] == 1


def test_medium_priority_for_moderate_score_band() -> None:
    record = {
        "corroboration_level": "Moderate",
        "relationship_type": "partner_reference",
        "link_count": 2,
        "organization_count": 2,
        "corroborating_types": ["Type A", "Type B"],
    }

    result = evaluate_priority(record)

    assert result["priority_level"] == "Medium"


def test_low_priority_for_limited_evidence_only() -> None:
    record = {
        "corroboration_level": "Limited",
        "relationship_type": "",
        "link_count": 1,
        "organization_count": 1,
        "shared_domain_count": 0,
        "evidence_signals": [{"type": "single_type"}],
    }

    result = evaluate_priority(record)

    assert result["priority_level"] == "Low"


def test_reason_priority_prefers_strong_corroboration_first() -> None:
    record = {
        "corroboration_level": "Strong",
        "relationship_type": "shared_domain",
        "organization_count": 4,
        "link_count": 8,
    }

    result = evaluate_priority(record)

    assert (
        result["priority_reason"]
        == "Multiple independent evidence types reinforce this record, making it a high-priority relationship candidate."
    )


def test_ranking_reflects_importance_without_reordering() -> None:
    records = [
        {
            "signal_id": "SIG-LOW",
            "corroboration_level": "Limited",
            "organization_count": 1,
            "link_count": 1,
        },
        {
            "signal_id": "SIG-HIGH",
            "corroboration_level": "Strong",
            "relationship_type": "shared_domain",
            "organization_count": 3,
            "link_count": 4,
            "shared_domain_count": 2,
            "corroborating_types": ["A", "B", "C"],
        },
        {
            "signal_id": "SIG-MED",
            "corroboration_level": "Moderate",
            "relationship_type": "partner_reference",
            "organization_count": 2,
            "link_count": 2,
            "corroborating_types": ["A", "B"],
        },
    ]

    output = attach_triage(records)

    assert [item["signal_id"] for item in output] == [
        "SIG-LOW",
        "SIG-HIGH",
        "SIG-MED",
    ]
    by_id = {item["signal_id"]: item for item in output}
    assert by_id["SIG-HIGH"]["priority_rank"] == 1
    assert by_id["SIG-MED"]["priority_rank"] == 2
    assert by_id["SIG-LOW"]["priority_rank"] == 3


def test_deterministic_for_same_input() -> None:
    records = [
        {
            "signal_id": "SIG-001",
            "corroboration_level": "Moderate",
            "relationship_type": "partner_reference",
            "organization_count": 2,
            "link_count": 3,
            "corroborating_types": ["X", "Y"],
        },
        {
            "signal_id": "SIG-002",
            "corroboration_level": "Strong",
            "relationship_type": "shared_domain",
            "organization_count": 3,
            "link_count": 4,
            "corroborating_types": ["X", "Y", "Z"],
        },
    ]

    first = attach_triage(records)
    second = attach_triage(records)

    assert first == second


def test_sparse_record_falls_back_to_low_with_default_reason() -> None:
    result = attach_triage([{}])[0]

    assert result["priority_level"] == "Low"
    assert result["priority_rank"] == 1
    assert (
        result["priority_reason"]
        == "This record remains useful but currently has fewer reinforcing indicators than higher-priority records."
    )
