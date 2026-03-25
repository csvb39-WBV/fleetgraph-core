import copy
import pathlib
import sys

import pytest


PROJECT_ROOT = pathlib.Path(__file__).resolve().parents[2]
SRC_ROOT = PROJECT_ROOT / "src"

if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from fleetgraph_core.graph_construction.supporting_entity_models import (
    build_supporting_entity_node_batch,
    build_supporting_entity_nodes,
    get_supported_supporting_entity_types,
)
from fleetgraph_core.intelligence.unified_event_schema import build_unified_event_record


def _event_details_for(event_type: str) -> dict[str, str]:
    if event_type == "litigation":
        return {
            "case_id": "CASE-1",
            "case_type": "civil",
            "filing_date": "2026-03-01",
            "plaintiff_role": "plaintiff",
            "defendant_role": "defendant",
        }
    if event_type == "audit":
        return {
            "audit_id": "AUD-1",
            "issue_type": "compliance",
            "opened_date": "2026-03-01",
            "agency": "Inspector",
        }
    if event_type == "enforcement":
        return {
            "action_id": "ENF-1",
            "issue_type": "safety",
            "opened_date": "2026-03-01",
            "agency": "Regulator",
        }
    if event_type == "lien":
        return {
            "lien_id": "LIEN-1",
            "filing_date": "2026-03-01",
            "claimant_role": "claimant",
        }
    return {
        "bond_claim_id": "BOND-1",
        "filing_date": "2026-03-01",
        "claimant_role": "claimant",
    }


def _make_record(
    event_type: str = "litigation",
    event_id: str = "evt-1",
    company_name: str = "Acme Builders",
    project_name: str | None = "North Tower",
    agency_or_court: str | None = "Superior Court",
) -> dict[str, object]:
    return build_unified_event_record(
        {
            "event_id": event_id,
            "event_type": event_type,
            "company_name": company_name,
            "source_name": "Daily Ledger",
            "status": "open",
            "event_date": "2026-03-01",
            "jurisdiction": "CA",
            "project_name": project_name,
            "agency_or_court": agency_or_court,
            "severity": "medium",
            "amount": 1000,
            "currency": "USD",
            "service_fit": ["legal_monitoring"],
            "trigger_tags": ["construction"],
            "evidence": {
                "summary": "summary",
                "source_record_id": "src-1",
            },
            "event_details": _event_details_for(event_type),
        }
    )


def test_supported_supporting_entity_types_exact_tuple():
    assert get_supported_supporting_entity_types() == (
        "company",
        "project",
        "agency",
        "court",
    )


def test_litigation_builds_company_project_court():
    result = build_supporting_entity_nodes(
        _make_record(event_type="litigation", agency_or_court="Superior Court")
    )
    assert [node["node_type"] for node in result] == ["company", "project", "court"]
