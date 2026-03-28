from __future__ import annotations

import copy

from fleetgraph.signals.query_library import get_ordered_query_definitions


def test_query_ordering_deterministic() -> None:
    first = get_ordered_query_definitions()
    second = get_ordered_query_definitions()

    assert first == second
    assert [query_definition["query"] for query_definition in first] == [
        "construction lawsuit contractor",
        "contract dispute contractor project",
        "mechanics lien filed contractor",
        "audit construction company compliance review contractor",
        "project delay construction dispute",
        "contractor default notice project",
        "government investigation contractor contractor debarred construction",
    ]


def test_query_library_contract() -> None:
    query_definitions = get_ordered_query_definitions()

    assert all(set(query_definition.keys()) == {
        "query_id",
        "signal_type",
        "query",
        "priority_weight",
        "max_results",
    } for query_definition in query_definitions)
    assert all(query_definition["priority_weight"] > 0 for query_definition in query_definitions)
    assert all(query_definition["max_results"] > 0 for query_definition in query_definitions)
    assert len(query_definitions) == 7


def test_query_library_no_mutation() -> None:
    first = get_ordered_query_definitions()
    snapshot = copy.deepcopy(first)

    _ = get_ordered_query_definitions()

    assert first == snapshot
