import pytest

from src.fleetgraph_core.graph_construction.domain_node_deduper import (
    assemble_unified_domain_nodes,
    build_domain_group_key,
    build_unified_domain_identity,
    merge_domain_node_group,
    validate_domain_node_records,
)


def _sample_record(domain_candidate="example.com", org_idx=1):
    return {
        "unified_organization_id": f"org-unified-{org_idx}",
        "canonical_organization_ids": [f"id-{org_idx}-a"],
        "canonical_organization_name": f"Org Name {org_idx}",
        "canonical_organization_key": f"key-{org_idx}",
        "domain_candidate": domain_candidate,
        "source_ids": [f"src-{org_idx}"],
        "domain_classification": "corporate",
        "node_id": f"node-{org_idx}",
        "node_type": "organization",
        "node_label": f"Org Label {org_idx}",
        "edge_id": f"edge-{org_idx}",
        "edge_type": "has_domain",
        "edge_from": f"from-{org_idx}",
        "edge_to": f"domainnode:{domain_candidate}",
        "domain_node_id": f"domainnode:{domain_candidate}",
        "domain_node_type": "domain",
        "domain_node_label": domain_candidate,
        "candidate_state": "domain_node_built",
    }


class TestValidateDomainNodeRecords:
    def test_valid_input(self):
        records = [_sample_record()]
        validate_domain_node_records(records)

    def test_missing_field_raises(self):
        rec = _sample_record()
        rec.pop("unified_organization_id")
        with pytest.raises(ValueError, match="missing or extra fields"):
            validate_domain_node_records([rec])

    def test_extra_field_raises(self):
        rec = _sample_record()
        rec["extra"] = "x"
        with pytest.raises(ValueError, match="missing or extra fields"):
            validate_domain_node_records([rec])

    def test_wrong_contract_raises(self):
        rec = _sample_record()
        rec["candidate_state"] = "bad_state"
        with pytest.raises(ValueError, match="candidate_state must be exactly 'domain_node_built'"):
            validate_domain_node_records([rec])


class TestBuildDomainGroupKey:
    def test_group_key_is_domain_node_id(self):
        rec = _sample_record()
        assert build_domain_group_key(rec) == "domainnode:example.com"


class TestMergeDomainNodeGroup:
    def test_merge_group(self):
        r1 = _sample_record(domain_candidate="example.com", org_idx=1)
        r2 = _sample_record(domain_candidate="example.com", org_idx=2)
        r2["source_ids"] = ["src-2", "src-1"]
        r2["edge_to"] = "domainnode:example.com"

        merged = merge_domain_node_group([r1, r2])

        assert merged["unified_domain_id"] == "unifieddomain:example.com"
        assert merged["domain_node_ids"] == ["domainnode:example.com", "domainnode:example.com"]
        assert merged["domain_node_id"] == "domainnode:example.com"
        assert merged["domain_node_type"] == "domain"
        assert merged["domain_node_label"] == "example.com"
        assert merged["domain_candidate"] == "example.com"
        assert merged["domain_classification"] == "corporate"
        assert merged["edge_tos"] == ["domainnode:example.com"]
        assert merged["source_ids"] == ["src-1", "src-2"]
        assert merged["organization_node_ids"] == ["node-1", "node-2"]
        assert merged["unified_organization_ids"] == ["org-unified-1", "org-unified-2"]
        assert merged["candidate_state"] == "domain_node_unified"


class TestBuildUnifiedDomainIdentity:
    def test_group_order_and_deduplication(self):
        r1 = _sample_record(domain_candidate="example.com", org_idx=1)
        r2 = _sample_record(domain_candidate="test.org", org_idx=2)
        r3 = _sample_record(domain_candidate="example.com", org_idx=3)

        unified = build_unified_domain_identity([r1, r2, r3])

        assert len(unified) == 2
        assert unified[0]["unified_domain_id"] == "unifieddomain:example.com"
        assert unified[1]["unified_domain_id"] == "unifieddomain:test.org"

        assert unified[0]["organization_node_ids"] == ["node-1", "node-3"]


class TestAssembleUnifiedDomainNodes:
    def test_assemble_all(self):
        r1 = _sample_record(domain_candidate="example.com", org_idx=1)
        r2 = _sample_record(domain_candidate="example.com", org_idx=2)

        original = [r1.copy(), r2.copy()]
        result = assemble_unified_domain_nodes([r1, r2])

        assert r1 == original[0]
        assert r2 == original[1]

        assert len(result) == 1
        assert result[0]["candidate_state"] == "domain_node_unified"
