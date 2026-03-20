import re


def validate_unified_organizations(unified_organizations):
    if not isinstance(unified_organizations, list):
        raise ValueError("unified_organizations must be a list")
    for org in unified_organizations:
        if not isinstance(org, dict):
            raise ValueError("each unified_organization must be a dict")
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
        if org['candidate_state'] != 'unified':
            raise ValueError("candidate_state must be 'unified'")


def normalize_domain(domain):
    if not isinstance(domain, str):
        raise ValueError("domain must be string")
    # 1. lowercase
    domain = domain.lower()
    # 2. strip leading/trailing whitespace
    domain = domain.strip()
    # 3. remove "http://"
    domain = domain.replace("http://", "")
    # 4. remove "https://"
    domain = domain.replace("https://", "")
    # 5. remove "www."
    domain = domain.replace("www.", "")
    # 6. remove trailing "/"
    if domain.endswith("/"):
        domain = domain[:-1]
    return domain


def validate_domain_format(domain):
    if not isinstance(domain, str):
        raise ValueError("domain must be string")
    # reject empty after normalization
    if not domain:
        raise ValueError("domain cannot be empty")
    # reject spaces
    if " " in domain:
        raise ValueError("domain cannot contain spaces")
    # reject values without "."
    if "." not in domain:
        raise ValueError("domain must contain '.'")
    # reject invalid characters outside a-z, 0-9, "-", "."
    if not re.match(r'^[a-z0-9.-]+$', domain):
        raise ValueError("domain contains invalid characters")
    return True


def build_domain_normalized_record(unified_organization):
    # First, normalize the domain
    normalized = normalize_domain(unified_organization['domain_candidate'])
    # Validate the normalized domain
    validate_domain_format(normalized)
    # Build new record: copy all fields, update domain_candidate and candidate_state
    record = unified_organization.copy()
    record['domain_candidate'] = normalized
    record['candidate_state'] = 'domain_normalized'
    return record


def assemble_domain_normalized_organizations(unified_organizations):
    # Validate input
    validate_unified_organizations(unified_organizations)
    # For each, build normalized record
    result = []
    for org in unified_organizations:
        normalized = build_domain_normalized_record(org)
        result.append(normalized)
    return result