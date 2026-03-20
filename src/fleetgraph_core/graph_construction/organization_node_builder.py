def validate_domain_classified_organizations(domain_classified_organizations):
    if not isinstance(domain_classified_organizations, list):
        raise ValueError("domain_classified_organizations must be a list")
    for org in domain_classified_organizations:
        if not isinstance(org, dict):
            raise ValueError("each domain_classified_organization must be a dict")
        required_fields = [
            'unified_organization_id',
            'canonical_organization_ids',
            'canonical_organization_name',
            'canonical_organization_key',
            'domain_candidate',
            'source_ids',
            'domain_classification',
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
        if org['domain_classification'] not in ['corporate', 'generic']:
            raise ValueError("domain_classification must be 'corporate' or 'generic'")
        if org['candidate_state'] != 'domain_classified':
            raise ValueError("candidate_state must be 'domain_classified'")


def build_node_id(canonical_organization_key):
    if not isinstance(canonical_organization_key, str):
        raise ValueError("canonical_organization_key must be string")
    return "orgnode:" + canonical_organization_key


def build_organization_node_record(domain_classified_organization):
    record = domain_classified_organization.copy()
    record['node_id'] = build_node_id(record['canonical_organization_key'])
    record['node_type'] = "organization"
    record['node_label'] = record['canonical_organization_name']
    record['candidate_state'] = 'organization_node_built'
    return record


def assemble_organization_nodes(domain_classified_organizations):
    validate_domain_classified_organizations(domain_classified_organizations)
    result = []
    for org in domain_classified_organizations:
        node = build_organization_node_record(org)
        result.append(node)
    return result