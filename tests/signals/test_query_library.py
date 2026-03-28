from __future__ import annotations

import copy

from fleetgraph.signals.query_library import get_ordered_query_definitions


def test_query_ordering_deterministic() -> None:
    first = get_ordered_query_definitions()
    second = get_ordered_query_definitions()

    assert first == second
    assert [query_definition["query"] for query_definition in first] == [
        "lawsuit filed against contractor company major project",
        "mechanics lien filed against company services group",
        "developer sued contractor project delay infrastructure",
        "contractor default notice issued developer public project",
        "audit investigation company project firm holdings",
        "federal investigation announced contractor infrastructure project",
        "subpoena issued company litigation counsel law firm",
    ]


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
    assert len(query_definitions) == 7


def test_query_library_all_queries_are_event_based() -> None:
    query_definitions = get_ordered_query_definitions()

    assert all(any(term in query_definition["query"] for term in (
        "lawsuit",
        "lien",
        "sued",
        "delay",
        "default notice",
        "investigation",
        "subpoena",
    )) for query_definition in query_definitions)


def test_query_library_no_mutation() -> None:
    first = get_ordered_query_definitions()
    snapshot = copy.deepcopy(first)

    _ = get_ordered_query_definitions()

    assert first == snapshot
