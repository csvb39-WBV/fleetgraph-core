from __future__ import annotations


_QUERY_DEFINITIONS = (
    {
        "query_id": "litigation_general",
        "signal_type": "litigation",
        "query": '"mechanics lien" contractor lawsuit',
        "priority_weight": 5,
        "max_results": 5,
    },
    {
        "query_id": "litigation_public_filings",
        "signal_type": "litigation",
        "query": '"breach of contract" construction contractor',
        "priority_weight": 4,
        "max_results": 5,
    },
    {
        "query_id": "audit_public",
        "signal_type": "audit",
        "query": '"audit findings" construction company',
        "priority_weight": 4,
        "max_results": 4,
    },
    {
        "query_id": "project_distress_delay",
        "signal_type": "project_distress",
        "query": '"project delay" contractor dispute',
        "priority_weight": 4,
        "max_results": 5,
    },
    {
        "query_id": "project_distress_default",
        "signal_type": "project_distress",
        "query": '"default notice" developer contractor',
        "priority_weight": 3,
        "max_results": 4,
    },
    {
        "query_id": "government_debarment",
        "signal_type": "government",
        "query": '"debarred contractor" government',
        "priority_weight": 5,
        "max_results": 4,
    },
    {
        "query_id": "government_investigation",
        "signal_type": "government",
        "query": '"government investigation" contractor',
        "priority_weight": 3,
        "max_results": 4,
    },
)
_REQUIRED_QUERY_KEYS = {
    "query_id",
    "signal_type",
    "query",
    "priority_weight",
    "max_results",
}
_VALID_SIGNAL_TYPES = {
    "litigation",
    "audit",
    "project_distress",
    "government",
}


def _is_valid_int(value: object) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def _is_valid_query_definition(query_definition: object) -> bool:
    if not isinstance(query_definition, dict):
        return False
    if set(query_definition.keys()) != _REQUIRED_QUERY_KEYS:
        return False
    if not isinstance(query_definition["query_id"], str) or query_definition["query_id"].strip() == "":
        return False
    if query_definition["signal_type"] not in _VALID_SIGNAL_TYPES:
        return False
    if not isinstance(query_definition["query"], str) or query_definition["query"].strip() == "":
        return False
    if not _is_valid_int(query_definition["priority_weight"]) or query_definition["priority_weight"] <= 0:
        return False
    if not _is_valid_int(query_definition["max_results"]) or query_definition["max_results"] <= 0:
        return False
    return True


def get_ordered_query_definitions() -> list[dict[str, object]]:
    ordered_query_definitions = [
        {
            "query_id": query_definition["query_id"],
            "signal_type": query_definition["signal_type"],
            "query": query_definition["query"],
            "priority_weight": query_definition["priority_weight"],
            "max_results": query_definition["max_results"],
        }
        for query_definition in _QUERY_DEFINITIONS
    ]
    if not all(_is_valid_query_definition(query_definition) for query_definition in ordered_query_definitions):
        raise ValueError("invalid_query_library")
    return ordered_query_definitions
