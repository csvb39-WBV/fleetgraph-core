import pytest
import pathlib
import sys


PROJECT_ROOT = pathlib.Path(__file__).resolve().parents[2]
SRC_ROOT = PROJECT_ROOT / "src"

if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from fleetgraph_core.graph_construction.organization_domain_edge_builder import (
    validate_organization_node_records,
    build_edge_id,
    build_edge_to,
    build_organization_domain_edge_record,
    assemble_organization_domain_edges,
)


class TestValidateOrganizationNodeRecords:
    def test_valid_organization_node_records(self):
        organization_node_records = [
            {
                'unified_organization_id': 'u1',
                'canonical_organization_ids': ['c1', 'c2'],
                'canonical_organization_name': 'Org1',
                'canonical_organization_key': 'org1-key',
                'domain_candidate': 'example.com',
                'source_ids': ['s1'],
                'domain_classification': 'corporate',
                'node_id': 'orgnode:org1-key',
                'node_type': 'organization',
                'node_label': 'Org1',
                'candidate_state': 'organization_node_built'
            }
        ]
        # Should not raise
        validate_organization_node_records(organization_node_records)

    def test_invalid_type_not_list(self):
        with pytest.raises(ValueError, match="organization_node_records must be a list"):
            validate_organization_node_records({})

    def test_invalid_rec_not_dict(self):
        with pytest.raises(ValueError, match="each organization_node_record must be a dict"):
            validate_organization_node_records([[]])

    def test_missing_field(self):
        rec = {
            'unified_organization_id': 'u1',
            'canonical_organization_ids': ['c1'],
            'canonical_organization_name': 'Org1',
            'canonical_organization_key': 'org1-key',
            'domain_candidate': 'example.com',
            'source_ids': ['s1'],
            'domain_classification': 'corporate',
            'node_id': 'orgnode:org1-key',
            'node_type': 'organization',
            'node_label': 'Org1'
            # missing candidate_state
        }
        with pytest.raises(ValueError, match="missing field: candidate_state"):
            validate_organization_node_records([rec])

    def test_extra_field(self):
        rec = {
            'unified_organization_id': 'u1',
            'canonical_organization_ids': ['c1'],
            'canonical_organization_name': 'Org1',
            'canonical_organization_key': 'org1-key',
            'domain_candidate': 'example.com',
            'source_ids': ['s1'],
            'domain_classification': 'corporate',
            'node_id': 'orgnode:org1-key',
            'node_type': 'organization',
            'node_label': 'Org1',
            'candidate_state': 'organization_node_built',
            'extra': 'field'
        }
        with pytest.raises(ValueError, match="extra or missing fields"):
            validate_organization_node_records([rec])

    def test_empty_unified_organization_id(self):
        rec = {
            'unified_organization_id': '',
            'canonical_organization_ids': ['c1'],
            'canonical_organization_name': 'Org1',
            'canonical_organization_key': 'org1-key',
            'domain_candidate': 'example.com',
            'source_ids': ['s1'],
            'domain_classification': 'corporate',
            'node_id': 'orgnode:org1-key',
            'node_type': 'organization',
            'node_label': 'Org1',
            'candidate_state': 'organization_node_built'
        }
        with pytest.raises(ValueError, match="unified_organization_id must be non-empty string"):
            validate_organization_node_records([rec])

    def test_invalid_canonical_organization_ids_empty_list(self):
        rec = {
            'unified_organization_id': 'u1',
            'canonical_organization_ids': [],
            'canonical_organization_name': 'Org1',
            'canonical_organization_key': 'org1-key',
            'domain_candidate': 'example.com',
            'source_ids': ['s1'],
            'domain_classification': 'corporate',
            'node_id': 'orgnode:org1-key',
            'node_type': 'organization',
            'node_label': 'Org1',
            'candidate_state': 'organization_node_built'
        }
        with pytest.raises(ValueError, match="canonical_organization_ids must be non-empty list of non-empty strings"):
            validate_organization_node_records([rec])

    def test_invalid_canonical_organization_ids_empty_string(self):
        rec = {
            'unified_organization_id': 'u1',
            'canonical_organization_ids': [''],
            'canonical_organization_name': 'Org1',
            'canonical_organization_key': 'org1-key',
            'domain_candidate': 'example.com',
            'source_ids': ['s1'],
            'domain_classification': 'corporate',
            'node_id': 'orgnode:org1-key',
            'node_type': 'organization',
            'node_label': 'Org1',
            'candidate_state': 'organization_node_built'
        }
        with pytest.raises(ValueError, match="canonical_organization_ids must be non-empty list of non-empty strings"):
            validate_organization_node_records([rec])

    def test_empty_canonical_organization_name(self):
        rec = {
            'unified_organization_id': 'u1',
            'canonical_organization_ids': ['c1'],
            'canonical_organization_name': '',
            'canonical_organization_key': 'org1-key',
            'domain_candidate': 'example.com',
            'source_ids': ['s1'],
            'domain_classification': 'corporate',
            'node_id': 'orgnode:org1-key',
            'node_type': 'organization',
            'node_label': 'Org1',
            'candidate_state': 'organization_node_built'
        }
        with pytest.raises(ValueError, match="canonical_organization_name must be non-empty string"):
            validate_organization_node_records([rec])

    def test_empty_canonical_organization_key(self):
        rec = {
            'unified_organization_id': 'u1',
            'canonical_organization_ids': ['c1'],
            'canonical_organization_name': 'Org1',
            'canonical_organization_key': '',
            'domain_candidate': 'example.com',
            'source_ids': ['s1'],
            'domain_classification': 'corporate',
            'node_id': 'orgnode:org1-key',
            'node_type': 'organization',
            'node_label': 'Org1',
            'candidate_state': 'organization_node_built'
        }
        with pytest.raises(ValueError, match="canonical_organization_key must be non-empty string"):
            validate_organization_node_records([rec])

    def test_empty_domain_candidate(self):
        rec = {
            'unified_organization_id': 'u1',
            'canonical_organization_ids': ['c1'],
            'canonical_organization_name': 'Org1',
            'canonical_organization_key': 'org1-key',
            'domain_candidate': '',
            'source_ids': ['s1'],
            'domain_classification': 'corporate',
            'node_id': 'orgnode:org1-key',
            'node_type': 'organization',
            'node_label': 'Org1',
            'candidate_state': 'organization_node_built'
        }
        with pytest.raises(ValueError, match="domain_candidate must be non-empty string"):
            validate_organization_node_records([rec])

    def test_invalid_source_ids_empty_list(self):
        rec = {
            'unified_organization_id': 'u1',
            'canonical_organization_ids': ['c1'],
            'canonical_organization_name': 'Org1',
            'canonical_organization_key': 'org1-key',
            'domain_candidate': 'example.com',
            'source_ids': [],
            'domain_classification': 'corporate',
            'node_id': 'orgnode:org1-key',
            'node_type': 'organization',
            'node_label': 'Org1',
            'candidate_state': 'organization_node_built'
        }
        with pytest.raises(ValueError, match="source_ids must be non-empty list of non-empty strings"):
            validate_organization_node_records([rec])

    def test_invalid_source_ids_empty_string(self):
        rec = {
            'unified_organization_id': 'u1',
            'canonical_organization_ids': ['c1'],
            'canonical_organization_name': 'Org1',
            'canonical_organization_key': 'org1-key',
            'domain_candidate': 'example.com',
            'source_ids': [''],
            'domain_classification': 'corporate',
            'node_id': 'orgnode:org1-key',
            'node_type': 'organization',
            'node_label': 'Org1',
            'candidate_state': 'organization_node_built'
        }
        with pytest.raises(ValueError, match="source_ids must be non-empty list of non-empty strings"):
            validate_organization_node_records([rec])

    def test_invalid_domain_classification(self):
        rec = {
            'unified_organization_id': 'u1',
            'canonical_organization_ids': ['c1'],
            'canonical_organization_name': 'Org1',
            'canonical_organization_key': 'org1-key',
            'domain_candidate': 'example.com',
            'source_ids': ['s1'],
            'domain_classification': 'other',
            'node_id': 'orgnode:org1-key',
            'node_type': 'organization',
            'node_label': 'Org1',
            'candidate_state': 'organization_node_built'
        }
        with pytest.raises(ValueError, match="domain_classification must be 'corporate' or 'generic'"):
            validate_organization_node_records([rec])

    def test_empty_node_id(self):
        rec = {
            'unified_organization_id': 'u1',
            'canonical_organization_ids': ['c1'],
            'canonical_organization_name': 'Org1',
            'canonical_organization_key': 'org1-key',
            'domain_candidate': 'example.com',
            'source_ids': ['s1'],
            'domain_classification': 'corporate',
            'node_id': '',
            'node_type': 'organization',
            'node_label': 'Org1',
            'candidate_state': 'organization_node_built'
        }
        with pytest.raises(ValueError, match="node_id must be non-empty string"):
            validate_organization_node_records([rec])

    def test_invalid_node_type(self):
        rec = {
            'unified_organization_id': 'u1',
            'canonical_organization_ids': ['c1'],
            'canonical_organization_name': 'Org1',
            'canonical_organization_key': 'org1-key',
            'domain_candidate': 'example.com',
            'source_ids': ['s1'],
            'domain_classification': 'corporate',
            'node_id': 'orgnode:org1-key',
            'node_type': 'other',
            'node_label': 'Org1',
            'candidate_state': 'organization_node_built'
        }
        with pytest.raises(ValueError, match="node_type must be 'organization'"):
            validate_organization_node_records([rec])

    def test_empty_node_label(self):
        rec = {
            'unified_organization_id': 'u1',
            'canonical_organization_ids': ['c1'],
            'canonical_organization_name': 'Org1',
            'canonical_organization_key': 'org1-key',
            'domain_candidate': 'example.com',
            'source_ids': ['s1'],
            'domain_classification': 'corporate',
            'node_id': 'orgnode:org1-key',
            'node_type': 'organization',
            'node_label': '',
            'candidate_state': 'organization_node_built'
        }
        with pytest.raises(ValueError, match="node_label must be non-empty string"):
            validate_organization_node_records([rec])

    def test_invalid_candidate_state(self):
        rec = {
            'unified_organization_id': 'u1',
            'canonical_organization_ids': ['c1'],
            'canonical_organization_name': 'Org1',
            'canonical_organization_key': 'org1-key',
            'domain_candidate': 'example.com',
            'source_ids': ['s1'],
            'domain_classification': 'corporate',
            'node_id': 'orgnode:org1-key',
            'node_type': 'organization',
            'node_label': 'Org1',
            'candidate_state': 'other'
        }
        with pytest.raises(ValueError, match="candidate_state must be 'organization_node_built'"):
            validate_organization_node_records([rec])


class TestBuildEdgeId:
    def test_build_edge_id(self):
        assert build_edge_id("org1-key") == "orgdomainedge:org1-key"

    def test_invalid_type(self):
        with pytest.raises(ValueError, match="canonical_organization_key must be string"):
            build_edge_id(123)


class TestBuildEdgeTo:
    def test_build_edge_to(self):
        assert build_edge_to("example.com") == "domainnode:example.com"

    def test_invalid_type(self):
        with pytest.raises(ValueError, match="domain_candidate must be string"):
            build_edge_to(123)


class TestBuildOrganizationDomainEdgeRecord:
    def test_build_record(self):
        rec = {
            'unified_organization_id': 'u1',
            'canonical_organization_ids': ['c1'],
            'canonical_organization_name': 'Org1',
            'canonical_organization_key': 'org1-key',
            'domain_candidate': 'example.com',
            'source_ids': ['s1'],
            'domain_classification': 'corporate',
            'node_id': 'orgnode:org1-key',
            'node_type': 'organization',
            'node_label': 'Org1',
            'candidate_state': 'organization_node_built'
        }
        result = build_organization_domain_edge_record(rec)
        expected = rec.copy()
        expected['edge_id'] = 'orgdomainedge:org1-key'
        expected['edge_type'] = 'has_domain'
        expected['edge_from'] = 'orgnode:org1-key'
        expected['edge_to'] = 'domainnode:example.com'
        expected['candidate_state'] = 'organization_domain_edge_built'
        assert result == expected


class TestAssembleOrganizationDomainEdges:
    def test_assemble_valid(self):
        organization_node_records = [
            {
                'unified_organization_id': 'u1',
                'canonical_organization_ids': ['c1'],
                'canonical_organization_name': 'Org1',
                'canonical_organization_key': 'org1-key',
                'domain_candidate': 'example.com',
                'source_ids': ['s1'],
                'domain_classification': 'corporate',
                'node_id': 'orgnode:org1-key',
                'node_type': 'organization',
                'node_label': 'Org1',
                'candidate_state': 'organization_node_built'
            },
            {
                'unified_organization_id': 'u2',
                'canonical_organization_ids': ['c2'],
                'canonical_organization_name': 'Org2',
                'canonical_organization_key': 'org2-key',
                'domain_candidate': 'gmail.com',
                'source_ids': ['s2'],
                'domain_classification': 'generic',
                'node_id': 'orgnode:org2-key',
                'node_type': 'organization',
                'node_label': 'Org2',
                'candidate_state': 'organization_node_built'
            }
        ]
        result = assemble_organization_domain_edges(organization_node_records)
        expected = [
            {
                'unified_organization_id': 'u1',
                'canonical_organization_ids': ['c1'],
                'canonical_organization_name': 'Org1',
                'canonical_organization_key': 'org1-key',
                'domain_candidate': 'example.com',
                'source_ids': ['s1'],
                'domain_classification': 'corporate',
                'node_id': 'orgnode:org1-key',
                'node_type': 'organization',
                'node_label': 'Org1',
                'candidate_state': 'organization_domain_edge_built',
                'edge_id': 'orgdomainedge:org1-key',
                'edge_type': 'has_domain',
                'edge_from': 'orgnode:org1-key',
                'edge_to': 'domainnode:example.com'
            },
            {
                'unified_organization_id': 'u2',
                'canonical_organization_ids': ['c2'],
                'canonical_organization_name': 'Org2',
                'canonical_organization_key': 'org2-key',
                'domain_candidate': 'gmail.com',
                'source_ids': ['s2'],
                'domain_classification': 'generic',
                'node_id': 'orgnode:org2-key',
                'node_type': 'organization',
                'node_label': 'Org2',
                'candidate_state': 'organization_domain_edge_built',
                'edge_id': 'orgdomainedge:org2-key',
                'edge_type': 'has_domain',
                'edge_from': 'orgnode:org2-key',
                'edge_to': 'domainnode:gmail.com'
            }
        ]
        assert result == expected

    def test_assemble_invalid_input(self):
        with pytest.raises(ValueError, match="organization_node_records must be a list"):
            assemble_organization_domain_edges({})

    def test_assemble_preserves_order(self):
        organization_node_records = [
            {
                'unified_organization_id': 'u2',
                'canonical_organization_ids': ['c2'],
                'canonical_organization_name': 'Org2',
                'canonical_organization_key': 'org2-key',
                'domain_candidate': 'gmail.com',
                'source_ids': ['s2'],
                'domain_classification': 'generic',
                'node_id': 'orgnode:org2-key',
                'node_type': 'organization',
                'node_label': 'Org2',
                'candidate_state': 'organization_node_built'
            },
            {
                'unified_organization_id': 'u1',
                'canonical_organization_ids': ['c1'],
                'canonical_organization_name': 'Org1',
                'canonical_organization_key': 'org1-key',
                'domain_candidate': 'example.com',
                'source_ids': ['s1'],
                'domain_classification': 'corporate',
                'node_id': 'orgnode:org1-key',
                'node_type': 'organization',
                'node_label': 'Org1',
                'candidate_state': 'organization_node_built'
            }
        ]
        result = assemble_organization_domain_edges(organization_node_records)
        assert result[0]['unified_organization_id'] == 'u2'
        assert result[1]['unified_organization_id'] == 'u1'