from __future__ import annotations

import copy

import pytest

from fleetgraph.control.query_budget_controller import QueryBudgetError, validate_query_budget


def _query_definition(**overrides: object) -> dict[str, object]:
    query_definition: dict[str, object] = {
        "query_id": "litigation_general",
        "signal_type": "litigation",
        "query": '"mechanics lien" contractor lawsuit',
        "priority_weight": 5,
        "max_results": 5,
    }
    query_definition.update(overrides)
    return query_definition


def test_budget_enforcement() -> None:
    query_definitions = [_query_definition(), _query_definition(query_id="audit_public", signal_type="audit")]

    with pytest.raises(QueryBudgetError, match="max_queries_per_run_exceeded"):
        validate_query_budget(
            query_definitions,
            max_queries_per_run=1,
            max_results_per_query=5,
        )

    with pytest.raises(QueryBudgetError, match="max_results_per_query_exceeded"):
        validate_query_budget(
            [_query_definition(max_results=6)],
            max_queries_per_run=5,
            max_results_per_query=5,
        )


def test_budget_validation_returns_copy() -> None:
    query_definitions = [_query_definition()]

    result = validate_query_budget(
        query_definitions,
        max_queries_per_run=3,
        max_results_per_query=5,
    )

    assert result == query_definitions
    assert result is not query_definitions


def test_budget_validation_no_mutation() -> None:
    query_definitions = [_query_definition()]
    snapshot = copy.deepcopy(query_definitions)

    _ = validate_query_budget(
        query_definitions,
        max_queries_per_run=3,
        max_results_per_query=5,
    )

    assert query_definitions == snapshot
