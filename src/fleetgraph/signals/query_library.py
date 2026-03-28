
from __future__ import annotations


_PRIMARY_QUERY_DEFINITIONS = (
    {
        "query_id": "construction_company_sued_project_delays",
        "signal_type": "project_distress",
        "query": "company sued over project delays",
        "priority_weight": 5,
        "max_results": 5,
        "intent_type": "event_based",
    },
    {
        "query_id": "construction_contractor_payment_lawsuit",
        "signal_type": "litigation",
        "query": "contractor payment lawsuit filed project",
        "priority_weight": 5,
        "max_results": 5,
        "intent_type": "event_based",
    },
    {
        "query_id": "construction_developer_dispute_project",
        "signal_type": "project_distress",
        "query": "developer dispute construction project",
        "priority_weight": 4,
        "max_results": 5,
        "intent_type": "event_based",
    },
    {
        "query_id": "litigation_mechanics_lien_services_group",
        "signal_type": "litigation",
        "query": "mechanics lien filed against company services group",
        "priority_weight": 5,
        "max_results": 5,
        "intent_type": "event_based",
    },
    {
        "query_id": "project_distress_default_notice_public_project",
        "signal_type": "project_distress",
        "query": "contractor default notice issued developer public project",
        "priority_weight": 4,
        "max_results": 5,
        "intent_type": "event_based",
    },
    {
        "query_id": "government_audit_investigation_contractor",
        "signal_type": "government",
        "query": "federal investigation announced contractor infrastructure project",
        "priority_weight": 5,
        "max_results": 5,
        "intent_type": "event_based",
    },
    {
        "query_id": "fleet_company_accident_investigation_lawsuit",
        "signal_type": "audit",
        "query": "fleet company accident investigation lawsuit",
        "priority_weight": 5,
        "max_results": 5,
        "intent_type": "event_based",
    },
    {
        "query_id": "fleet_logistics_contract_dispute_filed",
        "signal_type": "project_distress",
        "query": "logistics company contract dispute filed",
        "priority_weight": 4,
        "max_results": 5,
        "intent_type": "event_based",
    },
    {
        "query_id": "fleet_trucking_investigation_announced",
        "signal_type": "audit",
        "query": "trucking company investigation announced",
        "priority_weight": 5,
        "max_results": 5,
        "intent_type": "event_based",
    },
    {
        "query_id": "field_service_lawsuit_client_dispute",
        "signal_type": "project_distress",
        "query": "service company lawsuit filed client dispute",
        "priority_weight": 4,
        "max_results": 5,
        "intent_type": "event_based",
    },
    {
        "query_id": "field_service_investigation_announced",
        "signal_type": "audit",
        "query": "field service company investigation announced",
        "priority_weight": 4,
        "max_results": 5,
        "intent_type": "event_based",
    },
    {
        "query_id": "manufacturing_supplier_dispute_lawsuit",
        "signal_type": "litigation",
        "query": "manufacturing company supplier dispute lawsuit",
        "priority_weight": 4,
        "max_results": 5,
        "intent_type": "event_based",
    },
    {
        "query_id": "supply_chain_contract_dispute_filed",
        "signal_type": "project_distress",
        "query": "supply chain company contract dispute filed",
        "priority_weight": 4,
        "max_results": 5,
        "intent_type": "event_based",
    },
    {
        "query_id": "vendor_dispute_company_investigation",
        "signal_type": "audit",
        "query": "vendor dispute company investigation",
        "priority_weight": 4,
        "max_results": 5,
        "intent_type": "event_based",
    },
)

_FALLBACK_QUERY_DEFINITIONS = (
    {
        "query_id": "fallback_company_sued_project",
        "signal_type": "litigation",
        "query": "company sued project",
        "priority_weight": 3,
        "max_results": 4,
        "intent_type": "event_based",
    },
    {
        "query_id": "fallback_contractor_delay_lawsuit",
        "signal_type": "project_distress",
        "query": "contractor delay lawsuit",
        "priority_weight": 3,
        "max_results": 4,
        "intent_type": "event_based",
    },
    {
        "query_id": "fallback_investigation_announced_company",
        "signal_type": "audit",
        "query": "investigation announced company",
        "priority_weight": 3,
        "max_results": 4,
        "intent_type": "event_based",
    },
    {
        "query_id": "fallback_default_notice_contractor",
        "signal_type": "project_distress",
        "query": "default notice contractor",
        "priority_weight": 3,
        "max_results": 4,
        "intent_type": "event_based",
    },
    {
        "query_id": "fallback_subpoena_issued_company",
        "signal_type": "litigation",
        "query": "subpoena issued company",
        "priority_weight": 3,
        "max_results": 4,
        "intent_type": "event_based",
    },
)

_REQUIRED_QUERY_KEYS = {
    "query_id",
    "signal_type",
    "query",
    "priority_weight",
    "max_results",
    "intent_type",
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
    if query_definition["intent_type"] != "event_based":
        return False
    return True


def _copy_query_definitions(query_definitions: tuple[dict[str, object], ...]) -> list[dict[str, object]]:
    copied_query_definitions = [
        {
            "query_id": query_definition["query_id"],
            "signal_type": query_definition["signal_type"],
            "query": query_definition["query"],
            "priority_weight": query_definition["priority_weight"],
            "max_results": query_definition["max_results"],
            "intent_type": query_definition["intent_type"],
        }
        for query_definition in query_definitions
    ]
    if not all(_is_valid_query_definition(query_definition) for query_definition in copied_query_definitions):
        raise ValueError("invalid_query_library")
    return copied_query_definitions


def get_ordered_query_definitions() -> list[dict[str, object]]:
    return _copy_query_definitions(_PRIMARY_QUERY_DEFINITIONS)


def get_fallback_query_definitions() -> list[dict[str, object]]:
    return _copy_query_definitions(_FALLBACK_QUERY_DEFINITIONS)