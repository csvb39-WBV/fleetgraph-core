import pytest
import pathlib
import sys


PROJECT_ROOT = pathlib.Path(__file__).resolve().parents[2]
SRC_ROOT = PROJECT_ROOT / "src"

if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from fleetgraph_core.graph_construction.organization_node_builder import (
    validate_domain_classified_organizations,
    build_node_id,
    build_organization_node_record,
    assemble_organization_nodes,
)


class TestValidateDomainClassifiedOrganizations:
    def test_valid_domain_classified_organizations(self):
        domain_classified_organizations = [
            {
                'unified_organization_id': 'u1',
                'canonical_organization_ids': ['c1', 'c2'],
                'canonical_organization_name': 'Org1',
                'canonical_organization_key': 'org1-key',
                'domain_candidate': 'example.com',
                'source_ids': ['s1'],
                'domain_classification': 'corporate',
                'candidate_state': 'domain_classified'
            }
        ]
        # Should not raise
        validate_domain_classified_organizations(domain_classified_organizations)

    def test_invalid_type_not_list(self):
        with pytest.raises(ValueError, match="domain_classified_organizations must be a list"):
            validate_domain_classified_organizations({})

    def test_invalid_org_not_dict(self):
        with pytest.raises(ValueError, match="each domain_classified_organization must be a dict"):
            validate_domain_classified_organizations([[]])

    def test_missing_field(self):
        org = {
            'unified_organization_id': 'u1',
            'canonical_organization_ids': ['c1'],
            'canonical_organization_name': 'Org1',
            'canonical_organization_key': 'org1-key',
            'domain_candidate': 'example.com',
            'source_ids': ['s1'],
            'domain_classification': 'corporate'
            # missing candidate_state
        }
        with pytest.raises(ValueError, match="missing field: candidate_state"):
            validate_domain_classified_organizations([org])

    def test_extra_field(self):
        org = {
            'unified_organization_id': 'u1',
            'canonical_organization_ids': ['c1'],
            'canonical_organization_name': 'Org1',
            'canonical_organization_key': 'org1-key',
            'domain_candidate': 'example.com',
            'source_ids': ['s1'],
            'domain_classification': 'corporate',
            'candidate_state': 'domain_classified',
            'extra': 'field'
        }
        with pytest.raises(ValueError, match="extra or missing fields"):
            validate_domain_classified_organizations([org])

    def test_empty_unified_organization_id(self):
        org = {
            'unified_organization_id': 'u1',
            'canonical_organization_ids': ['c1'],
            'canonical_organization_name': 'Org1',
            'canonical_organization_key': 'org1-key',
            'domain_candidate': 'example.com',
            'source_ids': ['s1'],
            'domain_classification': 'corporate',
            'candidate_state': 'domain_classified'
        }
        org['unified_organization_id'] = ''
        with pytest.raises(ValueError, match="unified_organization_id must be non-empty string"):
            validate_domain_classified_organizations([org])

    def test_invalid_canonical_organization_ids_empty_list(self):
        org = {
            'unified_organization_id': 'u1',
            'canonical_organization_ids': [],
            'canonical_organization_name': 'Org1',
            'canonical_organization_key': 'org1-key',
            'domain_candidate': 'example.com',
            'source_ids': ['s1'],
            'domain_classification': 'corporate',
            'candidate_state': 'domain_classified'
        }
        with pytest.raises(ValueError, match="canonical_organization_ids must be non-empty list of non-empty strings"):
            validate_domain_classified_organizations([org])

    def test_invalid_canonical_organization_ids_empty_string(self):
        org = {
            'unified_organization_id': 'u1',
            'canonical_organization_ids': [''],
            'canonical_organization_name': 'Org1',
            'canonical_organization_key': 'org1-key',
            'domain_candidate': 'example.com',
            'source_ids': ['s1'],
            'domain_classification': 'corporate',
            'candidate_state': 'domain_classified'
        }
        with pytest.raises(ValueError, match="canonical_organization_ids must be non-empty list of non-empty strings"):
            validate_domain_classified_organizations([org])

    def test_empty_canonical_organization_name(self):
        org = {
            'unified_organization_id': 'u1',
            'canonical_organization_ids': ['c1'],
            'canonical_organization_name': '',
            'canonical_organization_key': 'org1-key',
            'domain_candidate': 'example.com',
            'source_ids': ['s1'],
            'domain_classification': 'corporate',
            'candidate_state': 'domain_classified'
        }
        with pytest.raises(ValueError, match="canonical_organization_name must be non-empty string"):
            validate_domain_classified_organizations([org])

    def test_empty_canonical_organization_key(self):
        org = {
            'unified_organization_id': 'u1',
            'canonical_organization_ids': ['c1'],
            'canonical_organization_name': 'Org1',
            'canonical_organization_key': '',
            'domain_candidate': 'example.com',
            'source_ids': ['s1'],
            'domain_classification': 'corporate',
            'candidate_state': 'domain_classified'
        }
        with pytest.raises(ValueError, match="canonical_organization_key must be non-empty string"):
            validate_domain_classified_organizations([org])

    def test_empty_domain_candidate(self):
        org = {
            'unified_organization_id': 'u1',
            'canonical_organization_ids': ['c1'],
            'canonical_organization_name': 'Org1',
            'canonical_organization_key': 'org1-key',
            'domain_candidate': '',
            'source_ids': ['s1'],
            'domain_classification': 'corporate',
            'candidate_state': 'domain_classified'
        }
        with pytest.raises(ValueError, match="domain_candidate must be non-empty string"):
            validate_domain_classified_organizations([org])

    def test_invalid_source_ids_empty_list(self):
        org = {
            'unified_organization_id': 'u1',
            'canonical_organization_ids': ['c1'],
            'canonical_organization_name': 'Org1',
            'canonical_organization_key': 'org1-key',
            'domain_candidate': 'example.com',
            'source_ids': [],
            'domain_classification': 'corporate',
            'candidate_state': 'domain_classified'
        }
        with pytest.raises(ValueError, match="source_ids must be non-empty list of non-empty strings"):
            validate_domain_classified_organizations([org])

    def test_invalid_source_ids_empty_string(self):
        org = {
            'unified_organization_id': 'u1',
            'canonical_organization_ids': ['c1'],
            'canonical_organization_name': 'Org1',
            'canonical_organization_key': 'org1-key',
            'domain_candidate': 'example.com',
            'source_ids': [''],
            'domain_classification': 'corporate',
            'candidate_state': 'domain_classified'
        }
        with pytest.raises(ValueError, match="source_ids must be non-empty list of non-empty strings"):
            validate_domain_classified_organizations([org])

    def test_invalid_domain_classification(self):
        org = {
            'unified_organization_id': 'u1',
            'canonical_organization_ids': ['c1'],
            'canonical_organization_name': 'Org1',
            'canonical_organization_key': 'org1-key',
            'domain_candidate': 'example.com',
            'source_ids': ['s1'],
            'domain_classification': 'other',
            'candidate_state': 'domain_classified'
        }
        with pytest.raises(ValueError, match="domain_classification must be 'corporate' or 'generic'"):
            validate_domain_classified_organizations([org])

    def test_invalid_candidate_state(self):
        org = {
            'unified_organization_id': 'u1',
            'canonical_organization_ids': ['c1'],
            'canonical_organization_name': 'Org1',
            'canonical_organization_key': 'org1-key',
            'domain_candidate': 'example.com',
            'source_ids': ['s1'],
            'domain_classification': 'corporate',
            'candidate_state': 'other'
        }
        with pytest.raises(ValueError, match="candidate_state must be 'domain_classified'"):
            validate_domain_classified_organizations([org])


class TestBuildNodeId:
    def test_build_node_id(self):
        assert build_node_id("org1-key") == "orgnode:org1-key"

    def test_invalid_type(self):
        with pytest.raises(ValueError, match="canonical_organization_key must be string"):
            build_node_id(123)


class TestBuildOrganizationNodeRecord:
    def test_build_record(self):
        org = {
            'unified_organization_id': 'u1',
            'canonical_organization_ids': ['c1'],
            'canonical_organization_name': 'Org1',
            'canonical_organization_key': 'org1-key',
            'domain_candidate': 'example.com',
            'source_ids': ['s1'],
            'domain_classification': 'corporate',
            'candidate_state': 'domain_classified'
        }
        result = build_organization_node_record(org)
        expected = org.copy()
        expected['node_id'] = 'orgnode:org1-key'
        expected['node_type'] = 'organization'
        expected['node_label'] = 'Org1'
        expected['candidate_state'] = 'organization_node_built'
        assert result == expected


class TestAssembleOrganizationNodes:
    def test_assemble_valid(self):
        domain_classified_organizations = [
            {
                'unified_organization_id': 'u1',
                'canonical_organization_ids': ['c1'],
                'canonical_organization_name': 'Org1',
                'canonical_organization_key': 'org1-key',
                'domain_candidate': 'example.com',
                'source_ids': ['s1'],
                'domain_classification': 'corporate',
                'candidate_state': 'domain_classified'
            },
            {
                'unified_organization_id': 'u2',
                'canonical_organization_ids': ['c2'],
                'canonical_organization_name': 'Org2',
                'canonical_organization_key': 'org2-key',
                'domain_candidate': 'gmail.com',
                'source_ids': ['s2'],
                'domain_classification': 'generic',
                'candidate_state': 'domain_classified'
            }
        ]
        result = assemble_organization_nodes(domain_classified_organizations)
        expected = [
            {
                'unified_organization_id': 'u1',
                'canonical_organization_ids': ['c1'],
                'canonical_organization_name': 'Org1',
                'canonical_organization_key': 'org1-key',
                'domain_candidate': 'example.com',
                'source_ids': ['s1'],
                'domain_classification': 'corporate',
                'candidate_state': 'organization_node_built',
                'node_id': 'orgnode:org1-key',
                'node_type': 'organization',
                'node_label': 'Org1'
            },
            {
                'unified_organization_id': 'u2',
                'canonical_organization_ids': ['c2'],
                'canonical_organization_name': 'Org2',
                'canonical_organization_key': 'org2-key',
                'domain_candidate': 'gmail.com',
                'source_ids': ['s2'],
                'domain_classification': 'generic',
                'candidate_state': 'organization_node_built',
                'node_id': 'orgnode:org2-key',
                'node_type': 'organization',
                'node_label': 'Org2'
            }
        ]
        assert result == expected

    def test_assemble_invalid_input(self):
        with pytest.raises(ValueError, match="domain_classified_organizations must be a list"):
            assemble_organization_nodes({})

    def test_assemble_preserves_order(self):
        domain_classified_organizations = [
            {
                'unified_organization_id': 'u2',
                'canonical_organization_ids': ['c2'],
                'canonical_organization_name': 'Org2',
                'canonical_organization_key': 'org2-key',
                'domain_candidate': 'gmail.com',
                'source_ids': ['s2'],
                'domain_classification': 'generic',
                'candidate_state': 'domain_classified'
            },
            {
                'unified_organization_id': 'u1',
                'canonical_organization_ids': ['c1'],
                'canonical_organization_name': 'Org1',
                'canonical_organization_key': 'org1-key',
                'domain_candidate': 'example.com',
                'source_ids': ['s1'],
                'domain_classification': 'corporate',
                'candidate_state': 'domain_classified'
            }
        ]
        result = assemble_organization_nodes(domain_classified_organizations)
        assert result[0]['unified_organization_id'] == 'u2'
        assert result[1]['unified_organization_id'] == 'u1'