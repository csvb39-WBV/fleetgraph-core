import pytest
from src.fleetgraph_core.graph_construction.domain_node_builder import (
    validate_organization_domain_edge_records,
    build_domain_node_id,
    build_domain_node_record,
    assemble_domain_nodes,
)


class TestValidateOrganizationDomainEdgeRecords:
    def test_valid_records(self):
        records = [
            {
                "unified_organization_id": "org1",
                "canonical_organization_ids": ["id1", "id2"],
                "canonical_organization_name": "Org Name",
                "canonical_organization_key": "key1",
                "domain_candidate": "example.com",
                "source_ids": ["src1"],
                "domain_classification": "corporate",
                "node_id": "node1",
                "node_type": "organization",
                "node_label": "Org Label",
                "edge_id": "edge1",
                "edge_type": "has_domain",
                "edge_from": "from1",
                "edge_to": "domainnode:example.com",
                "candidate_state": "organization_domain_edge_built",
            }
        ]
        validate_organization_domain_edge_records(records)  # Should not raise

    def test_missing_field(self):
        records = [
            {
                "unified_organization_id": "org1",
                # missing canonical_organization_ids
                "canonical_organization_name": "Org Name",
                "canonical_organization_key": "key1",
                "domain_candidate": "example.com",
                "source_ids": ["src1"],
                "domain_classification": "corporate",
                "node_id": "node1",
                "node_type": "organization",
                "node_label": "Org Label",
                "edge_id": "edge1",
                "edge_type": "has_domain",
                "edge_from": "from1",
                "edge_to": "domainnode:example.com",
                "candidate_state": "organization_domain_edge_built",
            }
        ]
        with pytest.raises(ValueError, match="record has missing or extra fields"):
            validate_organization_domain_edge_records(records)

    def test_extra_field(self):
        records = [
            {
                "unified_organization_id": "org1",
                "canonical_organization_ids": ["id1", "id2"],
                "canonical_organization_name": "Org Name",
                "canonical_organization_key": "key1",
                "domain_candidate": "example.com",
                "source_ids": ["src1"],
                "domain_classification": "corporate",
                "node_id": "node1",
                "node_type": "organization",
                "node_label": "Org Label",
                "edge_id": "edge1",
                "edge_type": "has_domain",
                "edge_from": "from1",
                "edge_to": "domainnode:example.com",
                "candidate_state": "organization_domain_edge_built",
                "extra_field": "value",
            }
        ]
        with pytest.raises(ValueError, match="record has missing or extra fields"):
            validate_organization_domain_edge_records(records)

    def test_wrong_type(self):
        records = [
            {
                "unified_organization_id": 123,  # should be str
                "canonical_organization_ids": ["id1", "id2"],
                "canonical_organization_name": "Org Name",
                "canonical_organization_key": "key1",
                "domain_candidate": "example.com",
                "source_ids": ["src1"],
                "domain_classification": "corporate",
                "node_id": "node1",
                "node_type": "organization",
                "node_label": "Org Label",
                "edge_id": "edge1",
                "edge_type": "has_domain",
                "edge_from": "from1",
                "edge_to": "domainnode:example.com",
                "candidate_state": "organization_domain_edge_built",
            }
        ]
        with pytest.raises(ValueError, match="unified_organization_id must be str"):
            validate_organization_domain_edge_records(records)

    def test_empty_string(self):
        records = [
            {
                "unified_organization_id": "",
                "canonical_organization_ids": ["id1", "id2"],
                "canonical_organization_name": "Org Name",
                "canonical_organization_key": "key1",
                "domain_candidate": "example.com",
                "source_ids": ["src1"],
                "domain_classification": "corporate",
                "node_id": "node1",
                "node_type": "organization",
                "node_label": "Org Label",
                "edge_id": "edge1",
                "edge_type": "has_domain",
                "edge_from": "from1",
                "edge_to": "domainnode:example.com",
                "candidate_state": "organization_domain_edge_built",
            }
        ]
        with pytest.raises(ValueError, match="unified_organization_id must be non-empty string"):
            validate_organization_domain_edge_records(records)

    def test_invalid_domain_classification(self):
        records = [
            {
                "unified_organization_id": "org1",
                "canonical_organization_ids": ["id1", "id2"],
                "canonical_organization_name": "Org Name",
                "canonical_organization_key": "key1",
                "domain_candidate": "example.com",
                "source_ids": ["src1"],
                "domain_classification": "invalid",
                "node_id": "node1",
                "node_type": "organization",
                "node_label": "Org Label",
                "edge_id": "edge1",
                "edge_type": "has_domain",
                "edge_from": "from1",
                "edge_to": "domainnode:example.com",
                "candidate_state": "organization_domain_edge_built",
            }
        ]
        with pytest.raises(ValueError, match="domain_classification must be exactly 'corporate' or 'generic'"):
            validate_organization_domain_edge_records(records)

    def test_invalid_node_type(self):
        records = [
            {
                "unified_organization_id": "org1",
                "canonical_organization_ids": ["id1", "id2"],
                "canonical_organization_name": "Org Name",
                "canonical_organization_key": "key1",
                "domain_candidate": "example.com",
                "source_ids": ["src1"],
                "domain_classification": "corporate",
                "node_id": "node1",
                "node_type": "invalid",
                "node_label": "Org Label",
                "edge_id": "edge1",
                "edge_type": "has_domain",
                "edge_from": "from1",
                "edge_to": "domainnode:example.com",
                "candidate_state": "organization_domain_edge_built",
            }
        ]
        with pytest.raises(ValueError, match="node_type must be exactly 'organization'"):
            validate_organization_domain_edge_records(records)

    def test_invalid_edge_type(self):
        records = [
            {
                "unified_organization_id": "org1",
                "canonical_organization_ids": ["id1", "id2"],
                "canonical_organization_name": "Org Name",
                "canonical_organization_key": "key1",
                "domain_candidate": "example.com",
                "source_ids": ["src1"],
                "domain_classification": "corporate",
                "node_id": "node1",
                "node_type": "organization",
                "node_label": "Org Label",
                "edge_id": "edge1",
                "edge_type": "invalid",
                "edge_from": "from1",
                "edge_to": "domainnode:example.com",
                "candidate_state": "organization_domain_edge_built",
            }
        ]
        with pytest.raises(ValueError, match="edge_type must be exactly 'has_domain'"):
            validate_organization_domain_edge_records(records)

    def test_invalid_edge_to(self):
        records = [
            {
                "unified_organization_id": "org1",
                "canonical_organization_ids": ["id1", "id2"],
                "canonical_organization_name": "Org Name",
                "canonical_organization_key": "key1",
                "domain_candidate": "example.com",
                "source_ids": ["src1"],
                "domain_classification": "corporate",
                "node_id": "node1",
                "node_type": "organization",
                "node_label": "Org Label",
                "edge_id": "edge1",
                "edge_type": "has_domain",
                "edge_from": "from1",
                "edge_to": "invalid",
                "candidate_state": "organization_domain_edge_built",
            }
        ]
        with pytest.raises(ValueError, match="edge_to must be exactly 'domainnode:' \\+ domain_candidate"):
            validate_organization_domain_edge_records(records)

    def test_invalid_candidate_state(self):
        records = [
            {
                "unified_organization_id": "org1",
                "canonical_organization_ids": ["id1", "id2"],
                "canonical_organization_name": "Org Name",
                "canonical_organization_key": "key1",
                "domain_candidate": "example.com",
                "source_ids": ["src1"],
                "domain_classification": "corporate",
                "node_id": "node1",
                "node_type": "organization",
                "node_label": "Org Label",
                "edge_id": "edge1",
                "edge_type": "has_domain",
                "edge_from": "from1",
                "edge_to": "domainnode:example.com",
                "candidate_state": "invalid",
            }
        ]
        with pytest.raises(ValueError, match="candidate_state must be exactly 'organization_domain_edge_built'"):
            validate_organization_domain_edge_records(records)

    def test_empty_list(self):
        records = [
            {
                "unified_organization_id": "org1",
                "canonical_organization_ids": [],
                "canonical_organization_name": "Org Name",
                "canonical_organization_key": "key1",
                "domain_candidate": "example.com",
                "source_ids": ["src1"],
                "domain_classification": "corporate",
                "node_id": "node1",
                "node_type": "organization",
                "node_label": "Org Label",
                "edge_id": "edge1",
                "edge_type": "has_domain",
                "edge_from": "from1",
                "edge_to": "domainnode:example.com",
                "candidate_state": "organization_domain_edge_built",
            }
        ]
        with pytest.raises(ValueError, match="canonical_organization_ids must be non-empty list"):
            validate_organization_domain_edge_records(records)

    def test_empty_string_in_list(self):
        records = [
            {
                "unified_organization_id": "org1",
                "canonical_organization_ids": ["", "id2"],
                "canonical_organization_name": "Org Name",
                "canonical_organization_key": "key1",
                "domain_candidate": "example.com",
                "source_ids": ["src1"],
                "domain_classification": "corporate",
                "node_id": "node1",
                "node_type": "organization",
                "node_label": "Org Label",
                "edge_id": "edge1",
                "edge_type": "has_domain",
                "edge_from": "from1",
                "edge_to": "domainnode:example.com",
                "candidate_state": "organization_domain_edge_built",
            }
        ]
        with pytest.raises(ValueError, match="all elements in canonical_organization_ids must be non-empty strings"):
            validate_organization_domain_edge_records(records)


class TestBuildDomainNodeId:
    def test_build_domain_node_id(self):
        assert build_domain_node_id("example.com") == "domainnode:example.com"
        assert build_domain_node_id("test.org") == "domainnode:test.org"


class TestBuildDomainNodeRecord:
    def test_build_domain_node_record(self):
        record = {
            "unified_organization_id": "org1",
            "canonical_organization_ids": ["id1", "id2"],
            "canonical_organization_name": "Org Name",
            "canonical_organization_key": "key1",
            "domain_candidate": "example.com",
            "source_ids": ["src1"],
            "domain_classification": "corporate",
            "node_id": "node1",
            "node_type": "organization",
            "node_label": "Org Label",
            "edge_id": "edge1",
            "edge_type": "has_domain",
            "edge_from": "from1",
            "edge_to": "domainnode:example.com",
            "candidate_state": "organization_domain_edge_built",
        }
        result = build_domain_node_record(record)
        expected = record.copy()
        expected["domain_node_id"] = "domainnode:example.com"
        expected["domain_node_type"] = "domain"
        expected["domain_node_label"] = "example.com"
        expected["candidate_state"] = "domain_node_built"
        assert result == expected


class TestAssembleDomainNodes:
    def test_assemble_domain_nodes(self):
        records = [
            {
                "unified_organization_id": "org1",
                "canonical_organization_ids": ["id1", "id2"],
                "canonical_organization_name": "Org Name",
                "canonical_organization_key": "key1",
                "domain_candidate": "example.com",
                "source_ids": ["src1"],
                "domain_classification": "corporate",
                "node_id": "node1",
                "node_type": "organization",
                "node_label": "Org Label",
                "edge_id": "edge1",
                "edge_type": "has_domain",
                "edge_from": "from1",
                "edge_to": "domainnode:example.com",
                "candidate_state": "organization_domain_edge_built",
            },
            {
                "unified_organization_id": "org2",
                "canonical_organization_ids": ["id3"],
                "canonical_organization_name": "Org Name 2",
                "canonical_organization_key": "key2",
                "domain_candidate": "test.org",
                "source_ids": ["src2"],
                "domain_classification": "generic",
                "node_id": "node2",
                "node_type": "organization",
                "node_label": "Org Label 2",
                "edge_id": "edge2",
                "edge_type": "has_domain",
                "edge_from": "from2",
                "edge_to": "domainnode:test.org",
                "candidate_state": "organization_domain_edge_built",
            }
        ]
        result = assemble_domain_nodes(records)
        assert len(result) == 2
        assert result[0]["domain_node_id"] == "domainnode:example.com"
        assert result[0]["domain_node_type"] == "domain"
        assert result[0]["domain_node_label"] == "example.com"
        assert result[0]["candidate_state"] == "domain_node_built"
        assert result[1]["domain_node_id"] == "domainnode:test.org"
        assert result[1]["domain_node_type"] == "domain"
        assert result[1]["domain_node_label"] == "test.org"
        assert result[1]["candidate_state"] == "domain_node_built"
        # Check order preserved
        assert result[0]["unified_organization_id"] == "org1"
        assert result[1]["unified_organization_id"] == "org2"