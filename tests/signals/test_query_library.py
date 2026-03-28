from __future__ import annotations

import copy

from fleetgraph.signals.query_library import get_ordered_query_definitions


EXPECTED_QUERIES = [
    "lawsuit filed against contractor company major project",
    "mechanics lien filed against company services group",
    "developer sued contractor project delay infrastructure",
    "contractor default notice issued developer public project",
    "audit investigation company project firm holdings",
    "federal investigation announced contractor infrastructure project",
    "subpoena issued company litigation counsel law firm",
    "document production ordered lawsuit company holdings",
    "contractor terminated project delay public project",
    "fleet accident investigation company logistics",
    "trucking company compliance violation investigation",
    "service company dispute contract project",
    "supplier dispute manufacturing company contract",
    "supply chain disruption company contract dispute",
]


EXPECTED_QUERY_IDS = [
    "litigation_lawsuit_filed_major_project",
    "litigation_mechanics_lien_services_group",
    "project_distress_developer_sued_delay",
    "project_distress_default_notice_public_project",
    "audit_investigation_company_project",
    "government_federal_investigation_contractor",
    "litigation_subpoena_counsel",
    "litigation_document_production_holdings",
    "project_distress_contractor_terminated_delay",
    "fleet_accident_investigation",
    "fleet_compliance_violation",
    "field_service_dispute",
    "manufacturing_supplier_dispute",
    "supply_chain_disruption",
]


def test_query_ordering_deterministic() -> None:
    first = get_ordered_query_definitions()
    second = get_ordered_query_definitions()

    assert first == second
    assert [query_definition["query"] for query_definition in first] == EXPECTED_QUERIES
    assert [query_definition["query_id"] for query_definition in first] == EXPECTED_QUERY_IDS


def test_query_library_contract() -> None:
    query_definitions = get_ordered_query_definitions()

    assert all(set(query_definition.keys()) == {
        "query_id",
        "signal_type",
        "query",
        "priority_weight",
        "max_results",
        "intent_type",
    } for query_definition in query_definitions)
    assert all(query_definition["priority_weight"] > 0 for query_definition in query_definitions)
    assert all(query_definition["max_results"] > 0 for query_definition in query_definitions)
    assert all(query_definition["intent_type"] == "event_based" for query_definition in query_definitions)
    assert len(query_definitions) == 14


def test_query_library_all_queries_are_event_based() -> None:
    query_definitions = get_ordered_query_definitions()

    assert all(any(term in query_definition["query"] for term in (
        "lawsuit",
        "lien",
        "sued",
        "default notice",
        "investigation",
        "subpoena",
        "terminated",
        "dispute",
        "disruption",
        "violation",
    )) for query_definition in query_definitions)


def test_query_library_includes_industry_expansion_queries() -> None:
    query_definitions = get_ordered_query_definitions()
    query_ids = {query_definition["query_id"] for query_definition in query_definitions}

    assert {
        "fleet_accident_investigation",
        "fleet_compliance_violation",
        "field_service_dispute",
        "manufacturing_supplier_dispute",
        "supply_chain_disruption",
    }.issubset(query_ids)


def test_query_library_no_mutation() -> None:
    first = get_ordered_query_definitions()
    snapshot = copy.deepcopy(first)

    _ = get_ordered_query_definitions()

    assert first == snapshot
