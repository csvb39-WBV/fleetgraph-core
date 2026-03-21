from typing import Any, Dict, List


_EXPANSION_REASON = (
    "The signal pattern suggests organizational growth or broader operating scope, "
    "which often precedes additional fleet demand."
)
_VENDOR_ENTRY_REASON = (
    "Shared infrastructure or partner-linked evidence suggests a service or vendor "
    "entry point into an active fleet environment."
)
_REPLACEMENT_REASON = (
    "Signal density and repeated relationship indicators suggest a likely replacement, "
    "upgrade, or modernization opportunity."
)
_OPERATIONAL_STRAIN_REASON = (
    "High-priority, strongly corroborated signals may indicate operational pressure "
    "that can create near-term fleet support needs."
)
_GENERAL_REASON = (
    "The current record suggests a relevant fleet-related opportunity, though the "
    "signal pattern is less specific than stronger categorized cases."
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


def _contains_expansion_signal_type(record: Dict[str, Any]) -> bool:
    signal_type = record.get("signal_type")
    if not signal_type:
        return False

    normalized = str(signal_type).strip().lower()
    return "hiring" in normalized or "growth" in normalized


def _has_partner_reference(record: Dict[str, Any]) -> bool:
    evidence_signals = record.get("evidence_signals")
    if not isinstance(evidence_signals, list):
        return False

    for signal in evidence_signals:
        if isinstance(signal, dict):
            signal_type = str(signal.get("type", "")).strip().lower()
            if signal_type == "partner_reference":
                return True
    return False


def _fleet_signal_category(record: Dict[str, Any]) -> str:
    organization_count = _to_int(record.get("organization_count"))
    opportunity_type = str(record.get("opportunity_type", "")).strip()
    relationship_type = str(record.get("relationship_type", "")).strip()
    link_count = _to_int(record.get("link_count"))
    shared_domain_count = _to_int(record.get("shared_domain_count"))
    corroboration_level = str(record.get("corroboration_level", "")).strip()
    priority_level = str(record.get("priority_level", "")).strip()

    if (
        organization_count >= 2
        or opportunity_type == "Fleet Expansion Opportunity"
        or _contains_expansion_signal_type(record)
    ):
        return "Expansion"

    if (
        relationship_type == "shared_domain"
        or _has_partner_reference(record)
        or opportunity_type == "Fleet Vendor / Service Opportunity"
    ):
        return "Vendor Entry"

    if (
        link_count >= 3
        or shared_domain_count >= 2
        or opportunity_type == "Fleet Replacement / Upgrade Opportunity"
    ):
        return "Replacement / Upgrade"

    if corroboration_level == "Strong" and priority_level == "High":
        return "Operational Strain"

    return "General Fleet Signal"


def _fleet_commercial_motion(category: str) -> str:
    if category == "Expansion":
        return "New Fleet Demand"
    if category == "Vendor Entry":
        return "Service / Vendor Engagement"
    if category == "Replacement / Upgrade":
        return "Replacement Cycle Opportunity"
    if category == "Operational Strain":
        return "Operational Support Opportunity"
    return "General Fleet Prospecting"


def _fleet_signal_reason(category: str) -> str:
    if category == "Expansion":
        return _EXPANSION_REASON
    if category == "Vendor Entry":
        return _VENDOR_ENTRY_REASON
    if category == "Replacement / Upgrade":
        return _REPLACEMENT_REASON
    if category == "Operational Strain":
        return _OPERATIONAL_STRAIN_REASON
    return _GENERAL_REASON


def evaluate_fleet_signal(record: Dict[str, Any]) -> Dict[str, str]:
    category = _fleet_signal_category(record)
    return {
        "fleet_signal_category": category,
        "fleet_commercial_motion": _fleet_commercial_motion(category),
        "fleet_signal_reason": _fleet_signal_reason(category),
    }


def attach_fleet_signal_mapping(records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    output = []
    for record in records:
        enriched = dict(record)
        enriched.update(evaluate_fleet_signal(record))
        output.append(enriched)
    return output
