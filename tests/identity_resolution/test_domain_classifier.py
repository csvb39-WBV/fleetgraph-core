import pytest
import pathlib
import sys


PROJECT_ROOT = pathlib.Path(__file__).resolve().parents[2]
SRC_ROOT = PROJECT_ROOT / "src"

if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from fleetgraph_core.identity_resolution.domain_classifier import (
    validate_domain_normalized_organizations,
    classify_domain,
    build_domain_classified_record,
    assemble_domain_classified_organizations,
)


class TestValidateDomainNormalizedOrganizations:
    def test_valid_domain_normalized_organizations(self):
        domain_normalized_organizations = [
            {
                'unified_organization_id': 'u1',
                'canonical_organization_ids': ['c1', 'c2'],
                'canonical_organization_name': 'Org1',
                'canonical_organization_key': 'org1-key',
                'domain_candidate': 'example.com',
                'source_ids': ['s1'],
                'candidate_state': 'domain_normalized'
            }
        ]
        # Should not raise
        validate_domain_normalized_organizations(domain_normalized_organizations)

    def test_invalid_type_not_list(self):
        with pytest.raises(ValueError, match="domain_normalized_organizations must be a list"):
            validate_domain_normalized_organizations({})

    def test_invalid_org_not_dict(self):
        with pytest.raises(ValueError, match="each domain_normalized_organization must be a dict"):
            validate_domain_normalized_organizations([[]])

    def test_missing_field(self):
        org = {
            'unified_organization_id': 'u1',
            'canonical_organization_ids': ['c1'],
            'canonical_organization_name': 'Org1',
            'canonical_organization_key': 'org1-key',
            'domain_candidate': 'example.com',
            'source_ids': ['s1']
            # missing candidate_state
        }
        with pytest.raises(ValueError, match="missing field: candidate_state"):
            validate_domain_normalized_organizations([org])

    def test_extra_field(self):
        org = {
            'unified_organization_id': 'u1',
            'canonical_organization_ids': ['c1'],
            'canonical_organization_name': 'Org1',
            'canonical_organization_key': 'org1-key',
            'domain_candidate': 'example.com',
            'source_ids': ['s1'],
            'candidate_state': 'domain_normalized',
            'extra': 'field'
        }
        with pytest.raises(ValueError, match="extra or missing fields"):
            validate_domain_normalized_organizations([org])

    def test_empty_unified_organization_id(self):
        org = {
            'unified_organization_id': 'u1',
            'canonical_organization_ids': ['c1'],
            'canonical_organization_name': 'Org1',
            'canonical_organization_key': 'org1-key',
            'domain_candidate': 'example.com',
            'source_ids': ['s1'],
            'candidate_state': 'domain_normalized'
        }
        org['unified_organization_id'] = ''
        with pytest.raises(ValueError, match="unified_organization_id must be non-empty string"):
            validate_domain_normalized_organizations([org])

    def test_invalid_canonical_organization_ids_empty_list(self):
        org = {
            'unified_organization_id': 'u1',
            'canonical_organization_ids': [],
            'canonical_organization_name': 'Org1',
            'canonical_organization_key': 'org1-key',
            'domain_candidate': 'example.com',
            'source_ids': ['s1'],
            'candidate_state': 'domain_normalized'
        }
        with pytest.raises(ValueError, match="canonical_organization_ids must be non-empty list of non-empty strings"):
            validate_domain_normalized_organizations([org])

    def test_invalid_canonical_organization_ids_empty_string(self):
        org = {
            'unified_organization_id': 'u1',
            'canonical_organization_ids': [''],
            'canonical_organization_name': 'Org1',
            'canonical_organization_key': 'org1-key',
            'domain_candidate': 'example.com',
            'source_ids': ['s1'],
            'candidate_state': 'domain_normalized'
        }
        with pytest.raises(ValueError, match="canonical_organization_ids must be non-empty list of non-empty strings"):
            validate_domain_normalized_organizations([org])

    def test_empty_canonical_organization_name(self):
        org = {
            'unified_organization_id': 'u1',
            'canonical_organization_ids': ['c1'],
            'canonical_organization_name': '',
            'canonical_organization_key': 'org1-key',
            'domain_candidate': 'example.com',
            'source_ids': ['s1'],
            'candidate_state': 'domain_normalized'
        }
        with pytest.raises(ValueError, match="canonical_organization_name must be non-empty string"):
            validate_domain_normalized_organizations([org])

    def test_empty_canonical_organization_key(self):
        org = {
            'unified_organization_id': 'u1',
            'canonical_organization_ids': ['c1'],
            'canonical_organization_name': 'Org1',
            'canonical_organization_key': '',
            'domain_candidate': 'example.com',
            'source_ids': ['s1'],
            'candidate_state': 'domain_normalized'
        }
        with pytest.raises(ValueError, match="canonical_organization_key must be non-empty string"):
            validate_domain_normalized_organizations([org])

    def test_empty_domain_candidate(self):
        org = {
            'unified_organization_id': 'u1',
            'canonical_organization_ids': ['c1'],
            'canonical_organization_name': 'Org1',
            'canonical_organization_key': 'org1-key',
            'domain_candidate': '',
            'source_ids': ['s1'],
            'candidate_state': 'domain_normalized'
        }
        with pytest.raises(ValueError, match="domain_candidate must be non-empty string"):
            validate_domain_normalized_organizations([org])

    def test_invalid_source_ids_empty_list(self):
        org = {
            'unified_organization_id': 'u1',
            'canonical_organization_ids': ['c1'],
            'canonical_organization_name': 'Org1',
            'canonical_organization_key': 'org1-key',
            'domain_candidate': 'example.com',
            'source_ids': [],
            'candidate_state': 'domain_normalized'
        }
        with pytest.raises(ValueError, match="source_ids must be non-empty list of non-empty strings"):
            validate_domain_normalized_organizations([org])

    def test_invalid_source_ids_empty_string(self):
        org = {
            'unified_organization_id': 'u1',
            'canonical_organization_ids': ['c1'],
            'canonical_organization_name': 'Org1',
            'canonical_organization_key': 'org1-key',
            'domain_candidate': 'example.com',
            'source_ids': [''],
            'candidate_state': 'domain_normalized'
        }
        with pytest.raises(ValueError, match="source_ids must be non-empty list of non-empty strings"):
            validate_domain_normalized_organizations([org])

    def test_invalid_candidate_state(self):
        org = {
            'unified_organization_id': 'u1',
            'canonical_organization_ids': ['c1'],
            'canonical_organization_name': 'Org1',
            'canonical_organization_key': 'org1-key',
            'domain_candidate': 'example.com',
            'source_ids': ['s1'],
            'candidate_state': 'other'
        }
        with pytest.raises(ValueError, match="candidate_state must be 'domain_normalized'"):
            validate_domain_normalized_organizations([org])


class TestClassifyDomain:
    def test_classify_generic_gmail(self):
        assert classify_domain("gmail.com") == "generic"

    def test_classify_generic_yahoo(self):
        assert classify_domain("yahoo.com") == "generic"

    def test_classify_generic_outlook(self):
        assert classify_domain("outlook.com") == "generic"

    def test_classify_generic_hotmail(self):
        assert classify_domain("hotmail.com") == "generic"

    def test_classify_generic_icloud(self):
        assert classify_domain("icloud.com") == "generic"

    def test_classify_generic_aol(self):
        assert classify_domain("aol.com") == "generic"

    def test_classify_generic_proton_me(self):
        assert classify_domain("proton.me") == "generic"

    def test_classify_generic_protonmail(self):
        assert classify_domain("protonmail.com") == "generic"

    def test_classify_corporate_example(self):
        assert classify_domain("example.com") == "corporate"

    def test_classify_corporate_subdomain(self):
        assert classify_domain("sub.example.com") == "corporate"

    def test_invalid_type(self):
        with pytest.raises(ValueError, match="domain must be string"):
            classify_domain(123)


class TestBuildDomainClassifiedRecord:
    def test_build_generic_record(self):
        org = {
            'unified_organization_id': 'u1',
            'canonical_organization_ids': ['c1'],
            'canonical_organization_name': 'Org1',
            'canonical_organization_key': 'org1-key',
            'domain_candidate': 'gmail.com',
            'source_ids': ['s1'],
            'candidate_state': 'domain_normalized'
        }
        result = build_domain_classified_record(org)
        expected = org.copy()
        expected['domain_classification'] = 'generic'
        expected['candidate_state'] = 'domain_classified'
        assert result == expected

    def test_build_corporate_record(self):
        org = {
            'unified_organization_id': 'u1',
            'canonical_organization_ids': ['c1'],
            'canonical_organization_name': 'Org1',
            'canonical_organization_key': 'org1-key',
            'domain_candidate': 'example.com',
            'source_ids': ['s1'],
            'candidate_state': 'domain_normalized'
        }
        result = build_domain_classified_record(org)
        expected = org.copy()
        expected['domain_classification'] = 'corporate'
        expected['candidate_state'] = 'domain_classified'
        assert result == expected


class TestAssembleDomainClassifiedOrganizations:
    def test_assemble_valid(self):
        domain_normalized_organizations = [
            {
                'unified_organization_id': 'u1',
                'canonical_organization_ids': ['c1'],
                'canonical_organization_name': 'Org1',
                'canonical_organization_key': 'org1-key',
                'domain_candidate': 'gmail.com',
                'source_ids': ['s1'],
                'candidate_state': 'domain_normalized'
            },
            {
                'unified_organization_id': 'u2',
                'canonical_organization_ids': ['c2'],
                'canonical_organization_name': 'Org2',
                'canonical_organization_key': 'org2-key',
                'domain_candidate': 'example.com',
                'source_ids': ['s2'],
                'candidate_state': 'domain_normalized'
            }
        ]
        result = assemble_domain_classified_organizations(domain_normalized_organizations)
        expected = [
            {
                'unified_organization_id': 'u1',
                'canonical_organization_ids': ['c1'],
                'canonical_organization_name': 'Org1',
                'canonical_organization_key': 'org1-key',
                'domain_candidate': 'gmail.com',
                'source_ids': ['s1'],
                'candidate_state': 'domain_classified',
                'domain_classification': 'generic'
            },
            {
                'unified_organization_id': 'u2',
                'canonical_organization_ids': ['c2'],
                'canonical_organization_name': 'Org2',
                'canonical_organization_key': 'org2-key',
                'domain_candidate': 'example.com',
                'source_ids': ['s2'],
                'candidate_state': 'domain_classified',
                'domain_classification': 'corporate'
            }
        ]
        assert result == expected

    def test_assemble_invalid_input(self):
        with pytest.raises(ValueError, match="domain_normalized_organizations must be a list"):
            assemble_domain_classified_organizations({})

    def test_assemble_preserves_order(self):
        domain_normalized_organizations = [
            {
                'unified_organization_id': 'u2',
                'canonical_organization_ids': ['c2'],
                'canonical_organization_name': 'Org2',
                'canonical_organization_key': 'org2-key',
                'domain_candidate': 'example.com',
                'source_ids': ['s2'],
                'candidate_state': 'domain_normalized'
            },
            {
                'unified_organization_id': 'u1',
                'canonical_organization_ids': ['c1'],
                'canonical_organization_name': 'Org1',
                'canonical_organization_key': 'org1-key',
                'domain_candidate': 'gmail.com',
                'source_ids': ['s1'],
                'candidate_state': 'domain_normalized'
            }
        ]
        result = assemble_domain_classified_organizations(domain_normalized_organizations)
        assert result[0]['unified_organization_id'] == 'u2'
        assert result[1]['unified_organization_id'] == 'u1'