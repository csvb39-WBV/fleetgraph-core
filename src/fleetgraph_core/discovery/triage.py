from typing import Any, Dict, List


_LEVEL_ORDER = {"High": 0, "Medium": 1, "Low": 2}


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


def _distinct_evidence_type_count(record: Dict[str, Any]) -> int:
    corroborating_types = record.get("corroborating_types")
    if isinstance(corroborating_types, list) and corroborating_types:
        normalized = {
            str(item).strip().lower() for item in corroborating_types if str(item).strip()
        }
        return len(normalized)

    evidence_signals = record.get("evidence_signals")
    if not isinstance(evidence_signals, list):
        return 0

    normalized = set()
    for signal in evidence_signals:
        if isinstance(signal, dict):
            signal_type = signal.get("type")
            if signal_type:
                normalized.add(str(signal_type).strip().lower())
    return len(normalized)


def _compute_internal_score(record: Dict[str, Any]) -> int:
    score = 0

    corroboration_level = str(record.get("corroboration_level", "")).strip()
    if corroboration_level == "Strong":
        score += 4
    elif corroboration_level == "Moderate":
        score += 2
    elif corroboration_level == "Limited":
        score += 1

    relationship_type = str(record.get("relationship_type", "")).strip()
    if relationship_type == "shared_domain":
        score += 2
    elif relationship_type:
        score += 1

    link_count = _to_int(record.get("link_count"))
    if link_count >= 4:
        score += 2
    elif link_count in (2, 3):
        score += 1

    organization_count = _to_int(record.get("organization_count"))
    if organization_count >= 3:
        score += 2
    elif organization_count == 2:
        score += 1

    shared_domain_count = _to_int(record.get("shared_domain_count"))
    if shared_domain_count >= 2:
        score += 2
    elif shared_domain_count == 1:
        score += 1

    evidence_type_count = _distinct_evidence_type_count(record)
    if evidence_type_count >= 3:
        score += 2
    elif evidence_type_count == 2:
        score += 1

    return score


def _priority_level_from_score(score: int) -> str:
    if score >= 7:
        return "High"
    if score >= 4:
        return "Medium"
    return "Low"


def _priority_reason(record: Dict[str, Any]) -> str:
    corroboration_level = str(record.get("corroboration_level", "")).strip()
    relationship_type = str(record.get("relationship_type", "")).strip()
    organization_count = _to_int(record.get("organization_count"))
    link_count = _to_int(record.get("link_count"))

    if corroboration_level == "Strong":
        return (
            "Multiple independent evidence types reinforce this record, "
            "making it a high-priority relationship candidate."
        )
    if relationship_type == "shared_domain" and organization_count >= 2:
        return (
            "Shared domain evidence involving multiple organizations makes "
            "this record worth early review."
        )
    if link_count >= 4:
        return (
            "A higher number of links makes this record more likely to "
            "reflect a meaningful relationship."
        )
    if organization_count >= 2:
        return "Multi-organization involvement increases the relevance of this record."
    return (
        "This record remains useful but currently has fewer reinforcing "
        "indicators than higher-priority records."
    )


def evaluate_priority(record: Dict[str, Any]) -> Dict[str, Any]:
    score = _compute_internal_score(record)
    return {
        "priority_level": _priority_level_from_score(score),
        "priority_reason": _priority_reason(record),
        "_priority_score": score,
    }


def attach_triage(records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    if not records:
        return []

    evaluated = []
    for index, record in enumerate(records):
        result = evaluate_priority(record)
        evaluated.append(
            {
                "index": index,
                "record": record,
                "score": result["_priority_score"],
                "priority_level": result["priority_level"],
                "priority_reason": result["priority_reason"],
                "link_count": _to_int(record.get("link_count")),
                "organization_count": _to_int(record.get("organization_count")),
            }
        )

    ranked = sorted(
        evaluated,
        key=lambda item: (
            -item["score"],
            _LEVEL_ORDER[item["priority_level"]],
            -item["link_count"],
            -item["organization_count"],
            item["index"],
        ),
    )
    rank_by_index = {item["index"]: rank for rank, item in enumerate(ranked, start=1)}

    output = []
    for item in evaluated:
        enriched = dict(item["record"])
        enriched["priority_level"] = item["priority_level"]
        enriched["priority_rank"] = rank_by_index[item["index"]]
        enriched["priority_reason"] = item["priority_reason"]
        output.append(enriched)

    return output
