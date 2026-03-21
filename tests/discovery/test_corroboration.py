from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[2]
SRC = ROOT / "src"

if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from fleetgraph_core.discovery.corroboration import attach_corroboration, evaluate_corroboration


def test_strong_when_three_or_more_distinct_types() -> None:
    record = {
        "evidence_signals": [
            {"type": "shared_email_domain"},
            {"type": "mentioned_domain"},
            {"type": "partner_reference"},
        ]
    }

    result = evaluate_corroboration(record)

    assert result["corroboration_level"] == "Strong"


def test_strong_when_legal_entity_plus_another_type() -> None:
    record = {
        "evidence_signals": [
            {"type": "legal_entity_reference"},
            {"type": "mentioned_domain"},
        ]
    }

    result = evaluate_corroboration(record)

    assert result["corroboration_level"] == "Strong"


def test_moderate_when_exactly_two_types() -> None:
    record = {
        "evidence_signals": [
            {"type": "mentioned_domain"},
            {"type": "partner_reference"},
        ]
    }

    result = evaluate_corroboration(record)

    assert result["corroboration_level"] == "Moderate"


def test_moderate_when_shared_email_domain_present() -> None:
    record = {"evidence_signals": [{"type": "shared_email_domain"}]}

    result = evaluate_corroboration(record)

    assert result["corroboration_level"] == "Moderate"


def test_limited_when_single_non_moderate_type() -> None:
    record = {"evidence_signals": [{"type": "custom_signal"}]}

    result = evaluate_corroboration(record)

    assert result["corroboration_level"] == "Limited"


def test_no_evidence_record_unchanged() -> None:
    records = [{"signal_id": "SIG-001", "organization_name": "Alpha"}]

    output = attach_corroboration(records)

    assert output[0] == records[0]
    assert "corroboration_level" not in output[0]


def test_readable_types_formatting_and_order() -> None:
    record = {
        "evidence_signals": [
            {"type": "shared_email_domain"},
            {"type": "legal_entity_reference"},
            {"type": "mentioned_domain"},
        ]
    }

    result = evaluate_corroboration(record)

    assert result["corroborating_types"] == [
        "Legal Entity Reference",
        "Mentioned Domain",
        "Shared Email Domain",
    ]


def test_deterministic_for_same_input() -> None:
    records = [
        {
            "signal_id": "SIG-001",
            "evidence_signals": [
                {"type": "shared_phone", "value": "15550001111"},
                {"type": "mentioned_domain", "value": "example.net"},
            ],
        }
    ]

    first = attach_corroboration(records)
    second = attach_corroboration(records)

    assert first == second
