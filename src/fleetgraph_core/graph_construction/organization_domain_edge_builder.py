def validate_organization_node_records(organization_node_records):
    if not isinstance(organization_node_records, list):
        raise ValueError("organization_node_records must be a list")
    for rec in organization_node_records:
        if not isinstance(rec, dict):
            raise ValueError("each organization_node_record must be a dict")
        required_fields = [
            'unified_organization_id',
            'canonical_organization_ids',
            'canonical_organization_name',
            'canonical_organization_key',
            'domain_candidate',
            'source_ids',
            'domain_classification',
            'node_id',
            'node_type',
            'node_label',
            'candidate_state'
        ]
        for field in required_fields:
            if field not in rec:
                raise ValueError(f"missing field: {field}")
        if set(rec.keys()) != set(required_fields):
            raise ValueError("extra or missing fields")
        if not isinstance(rec['unified_organization_id'], str) or not rec['unified_organization_id']:
            raise ValueError("unified_organization_id must be non-empty string")
        if not isinstance(rec['canonical_organization_ids'], list) or not rec['canonical_organization_ids'] or not all(isinstance(x, str) and x for x in rec['canonical_organization_ids']):
            raise ValueError("canonical_organization_ids must be non-empty list of non-empty strings")
        if not isinstance(rec['canonical_organization_name'], str) or not rec['canonical_organization_name']:
            raise ValueError("canonical_organization_name must be non-empty string")
        if not isinstance(rec['canonical_organization_key'], str) or not rec['canonical_organization_key']:
            raise ValueError("canonical_organization_key must be non-empty string")
        if not isinstance(rec['domain_candidate'], str) or not rec['domain_candidate']:
            raise ValueError("domain_candidate must be non-empty string")
        if not isinstance(rec['source_ids'], list) or not rec['source_ids'] or not all(isinstance(x, str) and x for x in rec['source_ids']):
            raise ValueError("source_ids must be non-empty list of non-empty strings")
        if rec['domain_classification'] not in ['corporate', 'generic']:
            raise ValueError("domain_classification must be 'corporate' or 'generic'")
        if not isinstance(rec['node_id'], str) or not rec['node_id']:
            raise ValueError("node_id must be non-empty string")
        if rec['node_type'] != 'organization':
            raise ValueError("node_type must be 'organization'")
        if not isinstance(rec['node_label'], str) or not rec['node_label']:
            raise ValueError("node_label must be non-empty string")
        if rec['candidate_state'] != 'organization_node_built':
            raise ValueError("candidate_state must be 'organization_node_built'")


def build_edge_id(canonical_organization_key):
    if not isinstance(canonical_organization_key, str):
        raise ValueError("canonical_organization_key must be string")
    if not canonical_organization_key:
        raise ValueError("canonical_organization_key must be non-empty string")
    return "orgdomainedge:" + canonical_organization_key


def build_edge_to(domain_candidate):
    if not isinstance(domain_candidate, str):
        raise ValueError("domain_candidate must be string")
    if not domain_candidate:
        raise ValueError("domain_candidate must be non-empty string")
    return "domainnode:" + domain_candidate


def build_organization_domain_edge_record(organization_node_record):
    record = organization_node_record.copy()
    record['edge_id'] = build_edge_id(record['canonical_organization_key'])
    record['edge_type'] = "has_domain"
    record['edge_from'] = record['node_id']
    record['edge_to'] = build_edge_to(record['domain_candidate'])
    record['candidate_state'] = 'organization_domain_edge_built'
    return record


def assemble_organization_domain_edges(organization_node_records):
    validate_organization_node_records(organization_node_records)
    result = []
    for rec in organization_node_records:
        edge = build_organization_domain_edge_record(rec)
        result.append(edge)
    return result