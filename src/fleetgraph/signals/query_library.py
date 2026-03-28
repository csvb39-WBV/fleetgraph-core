from __future__ import annotations


_QUERY_DEFINITIONS = (
    {
        "query_id": "litigation_lawsuit_filed_major_project",
        "signal_type": "litigation",
        "query": "lawsuit filed against contractor company major project",
        "priority_weight": 5,
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
        "query_id": "project_distress_developer_sued_delay",
        "signal_type": "project_distress",
        "query": "developer sued contractor project delay infrastructure",
        "priority_weight": 4,
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
        "query_id": "audit_investigation_company_project",
        "signal_type": "audit",
        "query": "audit investigation company project firm holdings",
        "priority_weight": 4,
        "max_results": 4,
        "intent_type": "event_based",
    },
    {
        "query_id": "government_federal_investigation_contractor",
        "signal_type": "government",
        "query": "federal investigation announced contractor infrastructure project",
        "priority_weight": 5,
        "max_results": 4,
        "intent_type": "event_based",
    },
    {
        "query_id": "litigation_subpoena_counsel",
        "signal_type": "litigation",
        "query": "subpoena issued company litigation counsel law firm",
        "priority_weight": 5,
        "max_results": 4,
        "intent_type": "event_based",
    },
    {
        "query_id": "litigation_document_production_holdings",
        "signal_type": "litigation",
        "query": "document production ordered lawsuit company holdings",
        "priority_weight": 4,
        "max_results": 4,
        "intent_type": "event_based",
    },
    {
        "query_id": "project_distress_contractor_terminated_delay",
        "signal_type": "project_distress",
        "query": "contractor terminated project delay public project",
        "priority_weight": 4,
        "max_results": 5,
        "intent_type": "event_based",
    },
    {
        "query_id": "fleet_accident_investigation",
        "signal_type": "audit",
        "query": "fleet accident investigation company logistics",
        "priority_weight": 5,
        "max_results": 5,
        "intent_type": "event_based",
    },
    {
        "query_id": "fleet_compliance_violation",
        "signal_type": "audit",
        "query": "trucking company compliance violation investigation",
        "priority_weight": 5,
        "max_results": 5,
        "intent_type": "event_based",
    },
    {
        "query_id": "field_service_dispute",
        "signal_type": "project_distress",
        "query": "service company dispute contract project",
        "priority_weight": 4,
        "max_results": 5,
        "intent_type": "event_based",
    },
    {
        "query_id": "manufacturing_supplier_dispute",
        "signal_type": "litigation",
        "query": "supplier dispute manufacturing company contract",
        "priority_weight": 4,
        "max_results": 5,
        "intent_type": "event_based",
    },
    {
        "query_id": "supply_chain_disruption",
        "signal_type": "project_distress",
        "query": "supply chain disruption company contract dispute",
        "priority_weight": 4,
        "max_results": 5,
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


def get_ordered_query_definitions() -> list[dict[str, object]]:
    ordered_query_definitions = [
        {
            "query_id": query_definition["query_id"],
            "signal_type": query_definition["signal_type"],
            "query": query_definition["query"],
            "priority_weight": query_definition["priority_weight"],
            "max_results": query_definition["max_results"],
            "intent_type": query_definition["intent_type"],
        }
        for query_definition in _QUERY_DEFINITIONS
    ]
    if not all(_is_valid_query_definition(query_definition) for query_definition in ordered_query_definitions):
        raise ValueError("invalid_query_library")
    return ordered_query_definitions
