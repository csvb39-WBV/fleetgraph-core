from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[2]
SRC = ROOT / "src"

if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from fleetgraph_core.discovery.opportunity import attach_opportunity, evaluate_opportunity


def test_expansion_detection_from_organization_count() -> None:
    record = {"organization_count": 2}

    result = evaluate_opportunity(record)

    assert result["opportunity_type"] == "Fleet Expansion Opportunity"


def test_vendor_service_detection_from_shared_domain_relationship() -> None:
    record = {
        "organization_count": 1,
        "relationship_type": "shared_domain",
    }

    result = evaluate_opportunity(record)

    assert result["opportunity_type"] == "Fleet Vendor / Service Opportunity"


def test_replacement_detection_when_not_expansion_or_vendor_service() -> None:
    record = {
        "organization_count": 1,
        "relationship_type": "other_relationship",
        "link_count": 3,
        "shared_domain_count": 0,
    }

    result = evaluate_opportunity(record)

    assert result["opportunity_type"] == "Fleet Replacement / Upgrade Opportunity"


def test_likelihood_mapping_high_medium_low() -> None:
    high = evaluate_opportunity({"priority_level": "High"})
    medium = evaluate_opportunity({"priority_level": "Medium"})
    low = evaluate_opportunity({"priority_level": "Low", "corroboration_level": "Limited"})

    assert high["likelihood"] == "High"
    assert medium["likelihood"] == "Medium"
    assert low["likelihood"] == "Low"


def test_why_now_selection_priority_order() -> None:
    strong = evaluate_opportunity(
        {
            "corroboration_level": "Strong",
            "organization_count": 3,
            "relationship_type": "shared_domain",
        }
    )
    expansion = evaluate_opportunity({"organization_count": 2})
    vendor = evaluate_opportunity(
        {
            "organization_count": 1,
            "relationship_type": "shared_domain",
        }
    )
    fallback = evaluate_opportunity({"organization_count": 1})

    assert (
        strong["why_now"]
        == "Multiple independent signals indicate that this organization is actively undergoing operational or structural change."
    )
    assert (
        expansion["why_now"]
        == "Signals suggest current expansion or increased operational demand, which typically precedes fleet growth or procurement activity."
    )
    assert (
        vendor["why_now"]
        == "Shared infrastructure or partner indicators suggest active vendor relationships that may present entry points for service engagement."
    )
    assert (
        fallback["why_now"]
        == "Available signals indicate a potential opportunity, though additional validation may be required."
    )


def test_suggested_action_selection_priority_order() -> None:
    high = evaluate_opportunity(
        {
            "priority_level": "High",
            "relationship_type": "shared_domain",
            "organization_count": 2,
        }
    )
    vendor = evaluate_opportunity(
        {
            "priority_level": "Low",
            "relationship_type": "shared_domain",
            "organization_count": 1,
        }
    )
    expansion = evaluate_opportunity(
        {
            "priority_level": "Low",
            "organization_count": 2,
        }
    )
    fallback = evaluate_opportunity({"priority_level": "Low", "organization_count": 1})

    assert (
        high["suggested_action"]
        == "Prioritize outreach to fleet or operations leadership to explore immediate needs."
    )
    assert (
        vendor["suggested_action"]
        == "Engage through service or vendor channels aligned with existing infrastructure relationships."
    )
    assert (
        expansion["suggested_action"]
        == "Initiate contact with procurement or fleet planning roles regarding upcoming capacity needs."
    )
    assert (
        fallback["suggested_action"]
        == "Monitor and consider light outreach to validate opportunity relevance."
    )


def test_deterministic_for_same_input() -> None:
    record = {
        "priority_level": "Medium",
        "corroboration_level": "Moderate",
        "organization_count": 2,
        "relationship_type": "shared_domain",
        "link_count": 2,
        "shared_domain_count": 1,
        "evidence_signals": [{"type": "partner_reference"}],
        "signal_type": "growth_signal",
        "connection_type": "shared_infrastructure",
    }

    first = evaluate_opportunity(record)
    second = evaluate_opportunity(record)

    assert first == second


def test_attach_opportunity_is_additive_and_non_mutating() -> None:
    original = {
        "signal_id": "SIG-100",
        "organization_count": 1,
        "link_count": 0,
    }
    records = [original]

    output = attach_opportunity(records)

    assert output[0]["signal_id"] == "SIG-100"
    assert output[0]["organization_count"] == 1
    assert output[0]["link_count"] == 0
    assert "opportunity_type" in output[0]
    assert "likelihood" in output[0]
    assert "why_now" in output[0]
    assert "suggested_action" in output[0]
    assert output[0] is not records[0]
    assert "opportunity_type" not in original
