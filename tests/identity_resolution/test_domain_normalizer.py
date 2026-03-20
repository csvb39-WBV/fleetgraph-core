import pytest
import pathlib
import sys


PROJECT_ROOT = pathlib.Path(__file__).resolve().parents[2]
SRC_ROOT = PROJECT_ROOT / "src"

if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from fleetgraph_core.identity_resolution.domain_normalizer import (
    validate_unified_organizations,
    normalize_domain,
    validate_domain_format,
    build_domain_normalized_record,
    assemble_domain_normalized_organizations,
)


class TestValidateUnifiedOrganizations:
    def test_valid_unified_organizations(self):
        unified_organizations = [
            {
                'unified_organization_id': 'u1',
                'canonical_organization_ids': ['c1', 'c2'],
                'canonical_organization_name': 'Org1',
                'canonical_organization_key': 'org1-key',
                'domain_candidate': 'example.com',
                'source_ids': ['s1'],
                'candidate_state': 'unified'
            }
        ]
        # Should not raise
        validate_unified_organizations(unified_organizations)

    def test_invalid_type_not_list(self):
        with pytest.raises(ValueError, match="unified_organizations must be a list"):
            validate_unified_organizations({})

    def test_invalid_org_not_dict(self):
        with pytest.raises(ValueError, match="each unified_organization must be a dict"):
            validate_unified_organizations([[]])

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
            validate_unified_organizations([org])

    def test_extra_field(self):
        org = {
            'unified_organization_id': 'u1',
            'canonical_organization_ids': ['c1'],
            'canonical_organization_name': 'Org1',
            'canonical_organization_key': 'org1-key',
            'domain_candidate': 'example.com',
            'source_ids': ['s1'],
            'candidate_state': 'unified',
            'extra': 'field'
        }
        with pytest.raises(ValueError, match="extra or missing fields"):
            validate_unified_organizations([org])

    def test_empty_unified_organization_id(self):
        org = {
            'unified_organization_id': '',
            'canonical_organization_ids': ['c1'],
            'canonical_organization_name': 'Org1',
            'canonical_organization_key': 'org1-key',
            'domain_candidate': 'example.com',
            'source_ids': ['s1'],
            'candidate_state': 'unified'
        }
        with pytest.raises(ValueError, match="unified_organization_id must be non-empty string"):
            validate_unified_organizations([org])

    def test_invalid_canonical_organization_ids_empty_list(self):
        org = {
            'unified_organization_id': 'u1',
            'canonical_organization_ids': [],
            'canonical_organization_name': 'Org1',
            'canonical_organization_key': 'org1-key',
            'domain_candidate': 'example.com',
            'source_ids': ['s1'],
            'candidate_state': 'unified'
        }
        with pytest.raises(ValueError, match="canonical_organization_ids must be non-empty list of non-empty strings"):
            validate_unified_organizations([org])

    def test_invalid_canonical_organization_ids_empty_string(self):
        org = {
            'unified_organization_id': 'u1',
            'canonical_organization_ids': [''],
            'canonical_organization_name': 'Org1',
            'canonical_organization_key': 'org1-key',
            'domain_candidate': 'example.com',
            'source_ids': ['s1'],
            'candidate_state': 'unified'
        }
        with pytest.raises(ValueError, match="canonical_organization_ids must be non-empty list of non-empty strings"):
            validate_unified_organizations([org])

    def test_empty_canonical_organization_name(self):
        org = {
            'unified_organization_id': 'u1',
            'canonical_organization_ids': ['c1'],
            'canonical_organization_name': '',
            'canonical_organization_key': 'org1-key',
            'domain_candidate': 'example.com',
            'source_ids': ['s1'],
            'candidate_state': 'unified'
        }
        with pytest.raises(ValueError, match="canonical_organization_name must be non-empty string"):
            validate_unified_organizations([org])

    def test_empty_canonical_organization_key(self):
        org = {
            'unified_organization_id': 'u1',
            'canonical_organization_ids': ['c1'],
            'canonical_organization_name': 'Org1',
            'canonical_organization_key': '',
            'domain_candidate': 'example.com',
            'source_ids': ['s1'],
            'candidate_state': 'unified'
        }
        with pytest.raises(ValueError, match="canonical_organization_key must be non-empty string"):
            validate_unified_organizations([org])

    def test_empty_domain_candidate(self):
        org = {
            'unified_organization_id': 'u1',
            'canonical_organization_ids': ['c1'],
            'canonical_organization_name': 'Org1',
            'canonical_organization_key': 'org1-key',
            'domain_candidate': '',
            'source_ids': ['s1'],
            'candidate_state': 'unified'
        }
        with pytest.raises(ValueError, match="domain_candidate must be non-empty string"):
            validate_unified_organizations([org])

    def test_invalid_source_ids_empty_list(self):
        org = {
            'unified_organization_id': 'u1',
            'canonical_organization_ids': ['c1'],
            'canonical_organization_name': 'Org1',
            'canonical_organization_key': 'org1-key',
            'domain_candidate': 'example.com',
            'source_ids': [],
            'candidate_state': 'unified'
        }
        with pytest.raises(ValueError, match="source_ids must be non-empty list of non-empty strings"):
            validate_unified_organizations([org])

    def test_invalid_source_ids_empty_string(self):
        org = {
            'unified_organization_id': 'u1',
            'canonical_organization_ids': ['c1'],
            'canonical_organization_name': 'Org1',
            'canonical_organization_key': 'org1-key',
            'domain_candidate': 'example.com',
            'source_ids': [''],
            'candidate_state': 'unified'
        }
        with pytest.raises(ValueError, match="source_ids must be non-empty list of non-empty strings"):
            validate_unified_organizations([org])

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
        with pytest.raises(ValueError, match="candidate_state must be 'unified'"):
            validate_unified_organizations([org])


class TestNormalizeDomain:
    def test_normalize_basic(self):
        assert normalize_domain("EXAMPLE.COM") == "example.com"

    def test_normalize_whitespace(self):
        assert normalize_domain("  example.com  ") == "example.com"

    def test_normalize_http(self):
        assert normalize_domain("http://example.com") == "example.com"

    def test_normalize_https(self):
        assert normalize_domain("https://example.com") == "example.com"

    def test_normalize_www(self):
        assert normalize_domain("www.example.com") == "example.com"

    def test_normalize_trailing_slash(self):
        assert normalize_domain("example.com/") == "example.com"

    def test_normalize_combined(self):
        assert normalize_domain("  HTTPS://WWW.EXAMPLE.COM/  ") == "example.com"

    def test_normalize_no_change(self):
        assert normalize_domain("example.com") == "example.com"

    def test_invalid_type(self):
        with pytest.raises(ValueError, match="domain must be string"):
            normalize_domain(123)


class TestValidateDomainFormat:
    def test_valid_domain(self):
        assert validate_domain_format("example.com") is True

    def test_valid_domain_with_dash(self):
        assert validate_domain_format("sub-domain.example.com") is True

    def test_valid_domain_with_numbers(self):
        assert validate_domain_format("example123.com") is True

    def test_empty_domain(self):
        with pytest.raises(ValueError, match="domain cannot be empty"):
            validate_domain_format("")

    def test_domain_with_space(self):
        with pytest.raises(ValueError, match="domain cannot contain spaces"):
            validate_domain_format("example .com")

    def test_domain_without_dot(self):
        with pytest.raises(ValueError, match="domain must contain '.'"):
            validate_domain_format("examplecom")

    def test_domain_with_invalid_char(self):
        with pytest.raises(ValueError, match="domain contains invalid characters"):
            validate_domain_format("example@.com")

    def test_invalid_type(self):
        with pytest.raises(ValueError, match="domain must be string"):
            validate_domain_format(123)


class TestBuildDomainNormalizedRecord:
    def test_build_valid_record(self):
        org = {
            'unified_organization_id': 'u1',
            'canonical_organization_ids': ['c1'],
            'canonical_organization_name': 'Org1',
            'canonical_organization_key': 'org1-key',
            'domain_candidate': 'EXAMPLE.COM',
            'source_ids': ['s1'],
            'candidate_state': 'unified'
        }
        result = build_domain_normalized_record(org)
        expected = org.copy()
        expected['domain_candidate'] = 'example.com'
        expected['candidate_state'] = 'domain_normalized'
        assert result == expected

    def test_build_invalid_domain_empty_after_normalize(self):
        org = {
            'unified_organization_id': 'u1',
            'canonical_organization_ids': ['c1'],
            'canonical_organization_name': 'Org1',
            'canonical_organization_key': 'org1-key',
            'domain_candidate': '   ',
            'source_ids': ['s1'],
            'candidate_state': 'unified'
        }
        with pytest.raises(ValueError, match="domain cannot be empty"):
            build_domain_normalized_record(org)


class TestAssembleDomainNormalizedOrganizations:
    def test_assemble_valid(self):
        unified_organizations = [
            {
                'unified_organization_id': 'u1',
                'canonical_organization_ids': ['c1'],
                'canonical_organization_name': 'Org1',
                'canonical_organization_key': 'org1-key',
                'domain_candidate': 'EXAMPLE.COM',
                'source_ids': ['s1'],
                'candidate_state': 'unified'
            },
            {
                'unified_organization_id': 'u2',
                'canonical_organization_ids': ['c2'],
                'canonical_organization_name': 'Org2',
                'canonical_organization_key': 'org2-key',
                'domain_candidate': 'HTTPS://WWW.TEST.ORG/',
                'source_ids': ['s2'],
                'candidate_state': 'unified'
            }
        ]
        result = assemble_domain_normalized_organizations(unified_organizations)
        expected = [
            {
                'unified_organization_id': 'u1',
                'canonical_organization_ids': ['c1'],
                'canonical_organization_name': 'Org1',
                'canonical_organization_key': 'org1-key',
                'domain_candidate': 'example.com',
                'source_ids': ['s1'],
                'candidate_state': 'domain_normalized'
            },
            {
                'unified_organization_id': 'u2',
                'canonical_organization_ids': ['c2'],
                'canonical_organization_name': 'Org2',
                'canonical_organization_key': 'org2-key',
                'domain_candidate': 'test.org',
                'source_ids': ['s2'],
                'candidate_state': 'domain_normalized'
            }
        ]
        assert result == expected

    def test_assemble_invalid_input(self):
        with pytest.raises(ValueError, match="unified_organizations must be a list"):
            assemble_domain_normalized_organizations({})

    def test_assemble_preserves_order(self):
        unified_organizations = [
            {
                'unified_organization_id': 'u2',
                'canonical_organization_ids': ['c2'],
                'canonical_organization_name': 'Org2',
                'canonical_organization_key': 'org2-key',
                'domain_candidate': 'test.org',
                'source_ids': ['s2'],
                'candidate_state': 'unified'
            },
            {
                'unified_organization_id': 'u1',
                'canonical_organization_ids': ['c1'],
                'canonical_organization_name': 'Org1',
                'canonical_organization_key': 'org1-key',
                'domain_candidate': 'example.com',
                'source_ids': ['s1'],
                'candidate_state': 'unified'
            }
        ]
        result = assemble_domain_normalized_organizations(unified_organizations)
        assert result[0]['unified_organization_id'] == 'u2'
        assert result[1]['unified_organization_id'] == 'u1'