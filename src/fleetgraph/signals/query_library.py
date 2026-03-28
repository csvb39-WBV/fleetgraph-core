from __future__ import annotations


_QUERY_DEFINITIONS = (
    {
        "query_id": "litigation_construction_lawsuit",
        "signal_type": "litigation",
        "query": "construction lawsuit contractor",
        "priority_weight": 5,
        "max_results": 5,
    },
    {
        "query_id": "litigation_contract_dispute",
        "signal_type": "litigation",
        "query": "contract dispute contractor project",
        "priority_weight": 4,
        "max_results": 5,
    },
    {
        "query_id": "litigation_mechanics_lien",
        "signal_type": "litigation",
        "query": "mechanics lien filed contractor",
        "priority_weight": 5,
        "max_results": 5,
    },
    {
        "query_id": "audit_compliance_review",
        "signal_type": "audit",
        "query": "audit construction company compliance review contractor",
        "priority_weight": 4,
        "max_results": 4,
    },
    {
        "query_id": "project_distress_delay",
        "signal_type": "project_distress",
        "query": "project delay construction dispute",
        "priority_weight": 4,
        "max_results": 5,
    },
    {
        "query_id": "project_distress_default_notice",
        "signal_type": "project_distress",
        "query": "contractor default notice project",
        "priority_weight": 3,
        "max_results": 4,
    },
    {
        "query_id": "government_enforcement",
        "signal_type": "government",
        "query": "government investigation contractor contractor debarred construction",
        "priority_weight": 5,
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
