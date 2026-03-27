from __future__ import annotations


class QueryBudgetError(ValueError):
    pass


def _is_valid_int(value: object) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def validate_query_budget(
    query_definitions: object,
    *,
    max_queries_per_run: int,
    max_results_per_query: int,
) -> list[dict[str, object]]:
    if not isinstance(query_definitions, list):
        raise QueryBudgetError("query_definitions_must_be_list")
    if not _is_valid_int(max_queries_per_run) or max_queries_per_run <= 0:
        raise QueryBudgetError("invalid_max_queries_per_run")
    if not _is_valid_int(max_results_per_query) or max_results_per_query <= 0:
        raise QueryBudgetError("invalid_max_results_per_query")
    if len(query_definitions) > max_queries_per_run:
        raise QueryBudgetError("max_queries_per_run_exceeded")

    validated_query_definitions: list[dict[str, object]] = []
    for query_definition in query_definitions:
        if not isinstance(query_definition, dict):
            raise QueryBudgetError("invalid_query_definition")
        if "max_results" not in query_definition:
            raise QueryBudgetError("query_definition_missing_max_results")
        max_results = query_definition["max_results"]
        if not _is_valid_int(max_results) or max_results <= 0:
            raise QueryBudgetError("invalid_query_max_results")
        if max_results > max_results_per_query:
            raise QueryBudgetError("max_results_per_query_exceeded")
        validated_query_definitions.append(
            {
                key: value
                for key, value in query_definition.items()
            }
        )
    return validated_query_definitions
