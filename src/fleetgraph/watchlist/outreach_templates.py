from __future__ import annotations


_SUBJECT_PREFIXES = {
    "legal_risk": "Question about",
    "finance": "Regarding",
    "operations": "Noticed",
    "general": "Following up on",
}


def build_subject_line(*, company_name: str, signal_summary: str, target_role_guess: str) -> str:
    normalized_company = str(company_name).strip()
    normalized_signal = str(signal_summary).strip()
    role_key = str(target_role_guess or "general").strip().lower()
    prefix = _SUBJECT_PREFIXES.get(role_key, _SUBJECT_PREFIXES["general"])
    if normalized_signal == "":
        return f"{prefix} {normalized_company}".strip()
    return f"{prefix} {normalized_company} {normalized_signal}".strip()


def build_why_now(*, signal_summary: str, qualification_reasons: list[str]) -> str:
    normalized_signal = str(signal_summary).strip()
    if normalized_signal != "":
        return f"Recent public signal: {normalized_signal}."
    if len(qualification_reasons) > 0:
        return f"Review triggered by {qualification_reasons[0].replace('_', ' ')}."
    return "Recent public activity suggests an active review window."


def build_why_this_company(*, company_name: str, target_role_guess: str, contact_type: str) -> str:
    normalized_company = str(company_name).strip()
    normalized_role = str(target_role_guess).strip().lower()
    normalized_contact_type = str(contact_type).strip().lower()
    if normalized_role == "legal_risk":
        role_message = "the legal and risk workflow may be under pressure"
    elif normalized_role == "finance":
        role_message = "finance stakeholders may need cleaner document turnaround"
    elif normalized_role == "operations":
        role_message = "operations may be absorbing avoidable follow-up work"
    else:
        role_message = "the team may benefit from faster document response workflows"
    if normalized_contact_type == "direct_email":
        contact_message = "We found a direct contact path"
    elif normalized_contact_type == "general_email":
        contact_message = "We found a public inbox path"
    elif normalized_contact_type == "phone":
        contact_message = "We found a reachable phone path"
    else:
        contact_message = "We found a public contact path"
    return f"{contact_message} and {normalized_company} appears timely because {role_message}."


def build_email_body(
    *,
    contact_name: str | None,
    company_name: str,
    signal_summary: str,
    why_now: str,
    why_this_company: str,
) -> str:
    salutation_name = str(contact_name or "there").strip() or "there"
    normalized_company = str(company_name).strip()
    normalized_signal = str(signal_summary).strip()
    body_lines = [
        f"Hi {salutation_name},",
        "",
        f"I noticed {normalized_company} was tied to {normalized_signal}." if normalized_signal != "" else f"I wanted to reach out regarding {normalized_company}.",
        why_now,
        why_this_company,
        "FactLedger helps teams cut document chasing, organize counterparties, and move faster when public issues create extra operational drag.",
        "If it would help, I can share a short overview of how teams use it to tighten up follow-up and response workflows.",
        "",
        "Best,",
        "FactLedger",
    ]
    return "\n".join(body_lines)
