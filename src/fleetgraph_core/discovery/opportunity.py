from typing import Any, Dict, List


_HIGH_CORROBORATION_MESSAGE = (
    "Multiple independent signals indicate that this organization is actively "
    "undergoing operational or structural change."
)
_EXPANSION_MESSAGE = (
    "Signals suggest current expansion or increased operational demand, which "
    "typically precedes fleet growth or procurement activity."
)
_VENDOR_MESSAGE = (
    "Shared infrastructure or partner indicators suggest active vendor "
    "relationships that may present entry points for service engagement."
)
_FALLBACK_MESSAGE = (
    "Available signals indicate a potential opportunity, though additional "
    "validation may be required."
)

_HIGH_ACTION = "Prioritize outreach to fleet or operations leadership to explore immediate needs."
_VENDOR_ACTION = (
    "Engage through service or vendor channels aligned with existing "
    "infrastructure relationships."
)
_EXPANSION_ACTION = (
    "Initiate contact with procurement or fleet planning roles regarding "
    "upcoming capacity needs."
)
_FALLBACK_ACTION = (
    "Monitor and consider light outreach to validate opportunity relevance."
)

_GROWTH_TERMS = (
    "hiring",
    "growth",
    "expansion",
    "scaling",
    "scale-up",
    "scale_up",
)


def _to_int(value: Any) -> int:
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, str):
        stripped = value.strip()
        if stripped.isdigit():
            return int(stripped)
    return 0


def _contains_growth_pattern(record: Dict[str, Any]) -> bool:
    signal_type = record.get("signal_type")
    if not signal_type:
        return False

    normalized = str(signal_type).strip().lower()
    return any(term in normalized for term in _GROWTH_TERMS)


def _has_partner_reference_evidence(record: Dict[str, Any]) -> bool:
    evidence_signals = record.get("evidence_signals")
    if not isinstance(evidence_signals, list):
        return False

    for signal in evidence_signals:
        if isinstance(signal, dict):
            signal_type = str(signal.get("type", "")).strip().lower()
            if signal_type == "partner_reference":
                return True
    return False


def _is_expansion(record: Dict[str, Any]) -> bool:
    return _to_int(record.get("organization_count")) >= 2 or _contains_growth_pattern(record)


def _is_vendor_service(record: Dict[str, Any]) -> bool:
    relationship_type = str(record.get("relationship_type", "")).strip()
    return relationship_type == "shared_domain" or _has_partner_reference_evidence(record)


def _is_replacement(record: Dict[str, Any]) -> bool:
    return _to_int(record.get("link_count")) >= 3 or _to_int(record.get("shared_domain_count")) >= 2


def _opportunity_type(record: Dict[str, Any]) -> str:
    if _is_expansion(record):
        return "Fleet Expansion Opportunity"
    if _is_vendor_service(record):
        return "Fleet Vendor / Service Opportunity"
    if _is_replacement(record):
        return "Fleet Replacement / Upgrade Opportunity"
    return "Fleet Intelligence Opportunity"


def _likelihood(record: Dict[str, Any]) -> str:
    priority_level = str(record.get("priority_level", "")).strip()
    corroboration_level = str(record.get("corroboration_level", "")).strip()

    if priority_level == "High" or corroboration_level == "Strong":
        return "High"
    if priority_level == "Medium" or corroboration_level == "Moderate":
        return "Medium"
    return "Low"


def _why_now(record: Dict[str, Any]) -> str:
    corroboration_level = str(record.get("corroboration_level", "")).strip()
    if corroboration_level == "Strong":
        return _HIGH_CORROBORATION_MESSAGE
    if _is_expansion(record):
        return _EXPANSION_MESSAGE
    if _is_vendor_service(record):
        return _VENDOR_MESSAGE
    return _FALLBACK_MESSAGE


def _suggested_action(record: Dict[str, Any], likelihood: str) -> str:
    if likelihood == "High":
        return _HIGH_ACTION
    if _is_vendor_service(record):
        return _VENDOR_ACTION
    if _is_expansion(record):
        return _EXPANSION_ACTION
    return _FALLBACK_ACTION


def evaluate_opportunity(record: Dict[str, Any]) -> Dict[str, Any]:
    likelihood = _likelihood(record)
    return {
        "opportunity_type": _opportunity_type(record),
        "likelihood": likelihood,
        "why_now": _why_now(record),
        "suggested_action": _suggested_action(record, likelihood),
    }


def attach_opportunity(records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    output = []
    for record in records:
        enriched = dict(record)
        enriched.update(evaluate_opportunity(record))
        output.append(enriched)
    return output
