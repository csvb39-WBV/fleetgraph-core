from typing import Any, Dict, List


STRONG_EXPLANATION = (
    "Multiple independent evidence types reinforce this relationship, increasing confidence that it is meaningful rather than incidental."
)
MODERATE_EXPLANATION = (
    "More than one evidence signal supports this relationship, suggesting it may be relevant but warrants further verification."
)
LIMITED_EXPLANATION = (
    "Only a single or limited evidence signal is present, so this relationship should be treated as a preliminary indicator."
)

_STRONG_TRIGGER_TYPES = {"legal_entity_reference", "shared_address", "shared_phone"}
_MODERATE_TRIGGER_TYPES = {"shared_email_domain", "mentioned_domain", "partner_reference"}


def _readable_type(value: str) -> str:
    return value.replace("_", " ").title()


def evaluate_corroboration(record: Dict[str, Any]) -> Dict[str, Any]:
    evidence_signals = record.get("evidence_signals", [])
    if not isinstance(evidence_signals, list):
        evidence_signals = []

    types = sorted(
        {
            item.get("type")
            for item in evidence_signals
            if isinstance(item, dict) and isinstance(item.get("type"), str) and item.get("type")
        }
    )
    count = len(types)

    if count >= 3 or (count >= 2 and any(t in _STRONG_TRIGGER_TYPES for t in types)):
        level = "Strong"
        explanation = STRONG_EXPLANATION
    elif count == 2 or any(t in _MODERATE_TRIGGER_TYPES for t in types):
        level = "Moderate"
        explanation = MODERATE_EXPLANATION
    else:
        level = "Limited"
        explanation = LIMITED_EXPLANATION

    readable_types = [_readable_type(item) for item in types]

    return {
        "corroboration_level": level,
        "corroborating_types": readable_types,
        "corroboration_explanation": explanation,
    }


def attach_corroboration(records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    output: List[Dict[str, Any]] = []

    for record in records:
        next_record = dict(record)
        evidence_signals = next_record.get("evidence_signals", [])
        if isinstance(evidence_signals, list) and len(evidence_signals) > 0:
            next_record.update(evaluate_corroboration(next_record))
        output.append(next_record)

    return output
