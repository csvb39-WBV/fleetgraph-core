from __future__ import annotations

import copy

from fleetgraph.signals.query_library import get_fallback_query_definitions, get_ordered_query_definitions


EXPECTED_PRIMARY_QUERIES = [
    "company sued over project delays",
    "contractor payment lawsuit filed project",
    "developer dispute construction project",
    "mechanics lien filed against company services group",
    "contractor default notice issued developer public project",
    "federal investigation announced contractor infrastructure project",
    "fleet company accident investigation lawsuit",
    "logistics company contract dispute filed",
    "trucking company investigation announced",
    "service company lawsuit filed client dispute",
    "field service company investigation announced",
    "manufacturing company supplier dispute lawsuit",
    "supply chain company contract dispute filed",
    "vendor dispute company investigation",
]
EXPECTED_FALLBACK_QUERIES = [
    "company sued project",
    "contractor delay lawsuit",
    "investigation announced company",
    "default notice contractor",
    "subpoena issued company",
]


def test_query_ordering_deterministic() -> None:
    first = get_ordered_query_definitions()
    second = get_ordered_query_definitions()

    assert first == second
    assert [query_definition["query"] for query_definition in first] == EXPECTED_PRIMARY_QUERIES


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
    assert 12 <= len(query_definitions) <= 14


def test_primary_query_library_covers_requested_industries() -> None:
    query_definitions = get_ordered_query_definitions()
    query_ids = {query_definition["query_id"] for query_definition in query_definitions}

    assert {
        "construction_company_sued_project_delays",
        "fleet_company_accident_investigation_lawsuit",
        "field_service_lawsuit_client_dispute",
        "manufacturing_supplier_dispute_lawsuit",
        "supply_chain_contract_dispute_filed",
        "vendor_dispute_company_investigation",
    }.issubset(query_ids)


def test_fallback_query_library_is_deterministic_and_event_based() -> None:
    first = get_fallback_query_definitions()
    second = get_fallback_query_definitions()

    assert first == second
    assert [query_definition["query"] for query_definition in first] == EXPECTED_FALLBACK_QUERIES
    assert all(query_definition["intent_type"] == "event_based" for query_definition in first)
    assert all(query_definition["max_results"] > 0 for query_definition in first)


def test_query_library_no_mutation() -> None:
    first = get_ordered_query_definitions()
    snapshot = copy.deepcopy(first)

    _ = get_ordered_query_definitions()

    assert first == snapshot
