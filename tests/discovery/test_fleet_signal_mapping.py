from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[2]
SRC = ROOT / "src"

if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from fleetgraph_core.discovery.fleet_signal_mapping import (
    attach_fleet_signal_mapping,
    evaluate_fleet_signal,
)


def test_expansion_mapping_from_organization_count() -> None:
    result = evaluate_fleet_signal({"organization_count": 2})

    assert result["fleet_signal_category"] == "Expansion"
    assert result["fleet_commercial_motion"] == "New Fleet Demand"


def test_vendor_entry_mapping_from_shared_domain() -> None:
    result = evaluate_fleet_signal(
        {
            "organization_count": 1,
            "relationship_type": "shared_domain",
        }
    )

    assert result["fleet_signal_category"] == "Vendor Entry"
    assert result["fleet_commercial_motion"] == "Service / Vendor Engagement"


def test_replacement_upgrade_mapping_from_link_count() -> None:
    result = evaluate_fleet_signal(
        {
            "organization_count": 1,
            "link_count": 3,
            "shared_domain_count": 0,
        }
    )

    assert result["fleet_signal_category"] == "Replacement / Upgrade"
    assert result["fleet_commercial_motion"] == "Replacement Cycle Opportunity"


def test_operational_strain_mapping_when_no_earlier_match() -> None:
    result = evaluate_fleet_signal(
        {
            "organization_count": 1,
            "link_count": 0,
            "shared_domain_count": 0,
            "corroboration_level": "Strong",
            "priority_level": "High",
        }
    )

    assert result["fleet_signal_category"] == "Operational Strain"
    assert result["fleet_commercial_motion"] == "Operational Support Opportunity"


def test_general_fallback_for_sparse_record() -> None:
    result = evaluate_fleet_signal({})

    assert result["fleet_signal_category"] == "General Fleet Signal"
    assert result["fleet_commercial_motion"] == "General Fleet Prospecting"


def test_reason_text_selection_by_category() -> None:
    expansion = evaluate_fleet_signal({"organization_count": 2})
    vendor = evaluate_fleet_signal({"relationship_type": "shared_domain"})
    replacement = evaluate_fleet_signal({"organization_count": 1, "link_count": 3})
    strain = evaluate_fleet_signal(
        {
            "organization_count": 1,
            "corroboration_level": "Strong",
            "priority_level": "High",
        }
    )
    general = evaluate_fleet_signal({"organization_count": 1})

    assert (
        expansion["fleet_signal_reason"]
        == "The signal pattern suggests organizational growth or broader operating scope, which often precedes additional fleet demand."
    )
    assert (
        vendor["fleet_signal_reason"]
        == "Shared infrastructure or partner-linked evidence suggests a service or vendor entry point into an active fleet environment."
    )
    assert (
        replacement["fleet_signal_reason"]
        == "Signal density and repeated relationship indicators suggest a likely replacement, upgrade, or modernization opportunity."
    )
    assert (
        strain["fleet_signal_reason"]
        == "High-priority, strongly corroborated signals may indicate operational pressure that can create near-term fleet support needs."
    )
    assert (
        general["fleet_signal_reason"]
        == "The current record suggests a relevant fleet-related opportunity, though the signal pattern is less specific than stronger categorized cases."
    )


def test_deterministic_for_same_input() -> None:
    record = {
        "opportunity_type": "Fleet Vendor / Service Opportunity",
        "likelihood": "Medium",
        "priority_level": "Medium",
        "priority_reason": "example reason",
        "corroboration_level": "Moderate",
        "relationship_type": "shared_domain",
        "organization_count": 2,
        "link_count": 2,
        "shared_domain_count": 1,
        "evidence_signals": [{"type": "partner_reference"}],
        "signal_type": "hiring_signal",
        "connection_type": "shared_infrastructure",
    }

    first = evaluate_fleet_signal(record)
    second = evaluate_fleet_signal(record)

    assert first == second


def test_attach_mapping_is_additive_and_non_mutating() -> None:
    original = {
        "signal_id": "SIG-200",
        "organization_count": 1,
        "link_count": 0,
    }
    records = [original]

    output = attach_fleet_signal_mapping(records)

    assert output[0]["signal_id"] == "SIG-200"
    assert output[0]["organization_count"] == 1
    assert output[0]["link_count"] == 0
    assert "fleet_signal_category" in output[0]
    assert "fleet_commercial_motion" in output[0]
    assert "fleet_signal_reason" in output[0]
    assert output[0] is not records[0]
    assert "fleet_signal_category" not in original
