GENERIC_DOMAINS = {
    "gmail.com",
    "yahoo.com",
    "outlook.com",
    "hotmail.com",
    "icloud.com",
    "aol.com",
    "proton.me",
    "protonmail.com",
}


def validate_domain_normalized_organizations(domain_normalized_organizations):
    if not isinstance(domain_normalized_organizations, list):
        raise ValueError("domain_normalized_organizations must be a list")
    for org in domain_normalized_organizations:
        if not isinstance(org, dict):
            raise ValueError("each domain_normalized_organization must be a dict")
        required_fields = [
            'unified_organization_id',
            'canonical_organization_ids',
            'canonical_organization_name',
            'canonical_organization_key',
            'domain_candidate',
            'source_ids',
            'candidate_state'
        ]
        for field in required_fields:
            if field not in org:
                raise ValueError(f"missing field: {field}")
        if set(org.keys()) != set(required_fields):
            raise ValueError("extra or missing fields")
        if not isinstance(org['unified_organization_id'], str) or not org['unified_organization_id']:
            raise ValueError("unified_organization_id must be non-empty string")
        if not isinstance(org['canonical_organization_ids'], list) or not org['canonical_organization_ids'] or not all(isinstance(x, str) and x for x in org['canonical_organization_ids']):
            raise ValueError("canonical_organization_ids must be non-empty list of non-empty strings")
        if not isinstance(org['canonical_organization_name'], str) or not org['canonical_organization_name']:
            raise ValueError("canonical_organization_name must be non-empty string")
        if not isinstance(org['canonical_organization_key'], str) or not org['canonical_organization_key']:
            raise ValueError("canonical_organization_key must be non-empty string")
        if not isinstance(org['domain_candidate'], str) or not org['domain_candidate']:
            raise ValueError("domain_candidate must be non-empty string")
        if not isinstance(org['source_ids'], list) or not org['source_ids'] or not all(isinstance(x, str) and x for x in org['source_ids']):
            raise ValueError("source_ids must be non-empty list of non-empty strings")
        if org['candidate_state'] != 'domain_normalized':
            raise ValueError("candidate_state must be 'domain_normalized'")


def classify_domain(domain):
    if not isinstance(domain, str):
        raise ValueError("domain must be string")
    if domain in GENERIC_DOMAINS:
        return "generic"
    else:
        return "corporate"


def build_domain_classified_record(domain_normalized_organization):
    classification = classify_domain(domain_normalized_organization['domain_candidate'])
    record = domain_normalized_organization.copy()
    record['domain_classification'] = classification
    record['candidate_state'] = 'domain_classified'
    return record


def assemble_domain_classified_organizations(domain_normalized_organizations):
    validate_domain_normalized_organizations(domain_normalized_organizations)
    result = []
    for org in domain_normalized_organizations:
        classified = build_domain_classified_record(org)
        result.append(classified)
    return result