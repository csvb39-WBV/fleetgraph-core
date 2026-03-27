from __future__ import annotations

import copy

from fleetgraph.signals.query_library import get_ordered_query_definitions


def test_query_ordering_deterministic() -> None:
    first = get_ordered_query_definitions()
    second = get_ordered_query_definitions()

    assert first == second
    assert [query_definition["signal_type"] for query_definition in first] == [
        "litigation",
        "litigation",
        "audit",
        "project_distress",
        "project_distress",
        "government",
        "government",
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


def test_query_library_no_mutation() -> None:
    first = get_ordered_query_definitions()
    snapshot = copy.deepcopy(first)

    _ = get_ordered_query_definitions()

    assert first == snapshot
