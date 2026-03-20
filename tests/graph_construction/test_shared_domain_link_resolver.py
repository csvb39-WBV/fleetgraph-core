import pytest

from src.fleetgraph_core.graph_construction.shared_domain_link_resolver import (
    assemble_shared_domain_links,
    build_shared_domain_link_id,
    build_shared_domain_link_record,
    build_shared_domain_pairs,
    validate_unified_domain_records,
)


def _sample_record(
    domain_candidate="example.com",
    organization_node_ids=None,
    unified_organization_ids=None,
    source_ids=None,
):
    if organization_node_ids is None:
        organization_node_ids = ["node-1", "node-2"]
    if unified_organization_ids is None:
        unified_organization_ids = ["org-unified-1", "org-unified-2"]
    if source_ids is None:
        source_ids = ["src-1", "src-2"]
    return {
        "unified_domain_id": f"unifieddomain:{domain_candidate}",
        "domain_node_ids": [f"domainnode:{domain_candidate}"],
        "domain_node_id": f"domainnode:{domain_candidate}",
        "domain_node_type": "domain",
        "domain_node_label": domain_candidate,
        "domain_candidate": domain_candidate,
        "domain_classification": "corporate",
        "edge_tos": [f"domainnode:{domain_candidate}"],
        "source_ids": source_ids,
        "organization_node_ids": organization_node_ids,
        "unified_organization_ids": unified_organization_ids,
        "candidate_state": "domain_node_unified",
    }


class TestValidateUnifiedDomainRecords:
    def test_valid_input(self):
        validate_unified_domain_records([_sample_record()])

    def test_empty_list_raises(self):
        with pytest.raises(ValueError):
            validate_unified_domain_records([])

    def test_missing_field_raises(self):
        rec = _sample_record()
        rec.pop("unified_domain_id")
        with pytest.raises(ValueError, match="missing or extra fields"):
            validate_unified_domain_records([rec])

    def test_extra_field_raises(self):
        rec = _sample_record()
        rec["unexpected"] = "value"
        with pytest.raises(ValueError, match="missing or extra fields"):
            validate_unified_domain_records([rec])

    def test_wrong_domain_node_type_raises(self):
        rec = _sample_record()
        rec["domain_node_type"] = "organization"
        with pytest.raises(ValueError, match="domain_node_type must be exactly 'domain'"):
            validate_unified_domain_records([rec])

    def test_invalid_domain_classification_raises(self):
        rec = _sample_record()
        rec["domain_classification"] = "unknown"
        with pytest.raises(ValueError, match="domain_classification must be exactly 'corporate' or 'generic'"):
            validate_unified_domain_records([rec])

    def test_wrong_candidate_state_raises(self):
        rec = _sample_record()
        rec["candidate_state"] = "domain_node_built"
        with pytest.raises(ValueError, match="candidate_state must be exactly 'domain_node_unified'"):
            validate_unified_domain_records([rec])

    def test_empty_string_field_raises(self):
        rec = _sample_record()
        rec["domain_candidate"] = ""
        with pytest.raises(ValueError, match="domain_candidate must be non-empty string"):
            validate_unified_domain_records([rec])

    def test_empty_list_field_raises(self):
        rec = _sample_record()
        rec["source_ids"] = []
        with pytest.raises(ValueError, match="source_ids must be non-empty list"):
            validate_unified_domain_records([rec])

    def test_empty_string_in_list_raises(self):
        rec = _sample_record()
        rec["organization_node_ids"] = ["node-1", ""]
        with pytest.raises(ValueError, match="all elements in organization_node_ids must be non-empty strings"):
            validate_unified_domain_records([rec])


class TestBuildSharedDomainPairs:
    def test_two_orgs_yields_one_pair(self):
        pairs = build_shared_domain_pairs(["node-1", "node-2"])
        assert pairs == [("node-1", "node-2")]

    def test_three_orgs_yields_three_pairs(self):
        pairs = build_shared_domain_pairs(["node-a", "node-b", "node-c"])
        assert pairs == [
            ("node-a", "node-b"),
            ("node-a", "node-c"),
            ("node-b", "node-c"),
        ]

    def test_single_org_yields_no_pairs(self):
        pairs = build_shared_domain_pairs(["node-1"])
        assert pairs == []

    def test_lexicographic_order_within_pair(self):
        pairs = build_shared_domain_pairs(["node-z", "node-a"])
        assert pairs == [("node-a", "node-z")]

    def test_duplicate_ids_collapsed(self):
        pairs = build_shared_domain_pairs(["node-1", "node-1"])
        assert pairs == []

    def test_first_seen_pair_order_preserved(self):
        pairs = build_shared_domain_pairs(["node-b", "node-c", "node-a"])
        assert pairs == [
            ("node-b", "node-c"),
            ("node-a", "node-b"),
            ("node-a", "node-c"),
        ]


class TestBuildSharedDomainLinkId:
    def test_deterministic_construction(self):
        result = build_shared_domain_link_id("example.com", "node-1", "node-2")
        assert result == "shareddomainlink:example.com:node-1:node-2"

    def test_different_domain_gives_different_id(self):
        a = build_shared_domain_link_id("example.com", "node-1", "node-2")
        b = build_shared_domain_link_id("other.com", "node-1", "node-2")
        assert a != b


class TestBuildSharedDomainLinkRecord:
    def test_output_fields_exactly(self):
        rec = _sample_record()
        result = build_shared_domain_link_record(rec, "node-1", "node-2")
        expected_keys = {
            "shared_domain_link_id",
            "left_organization_node_id",
            "right_organization_node_id",
            "shared_domain_id",
            "shared_domain_node_id",
            "shared_domain_candidate",
            "shared_domain_classification",
            "supporting_unified_organization_ids",
            "supporting_source_ids",
            "candidate_state",
        }
        assert set(result.keys()) == expected_keys

    def test_field_values(self):
        rec = _sample_record()
        result = build_shared_domain_link_record(rec, "node-1", "node-2")
        assert result["shared_domain_link_id"] == "shareddomainlink:example.com:node-1:node-2"
        assert result["left_organization_node_id"] == "node-1"
        assert result["right_organization_node_id"] == "node-2"
        assert result["shared_domain_id"] == "unifieddomain:example.com"
        assert result["shared_domain_node_id"] == "domainnode:example.com"
        assert result["shared_domain_candidate"] == "example.com"
        assert result["shared_domain_classification"] == "corporate"
        assert result["supporting_unified_organization_ids"] == ["org-unified-1", "org-unified-2"]
        assert result["supporting_source_ids"] == ["src-1", "src-2"]
        assert result["candidate_state"] == "shared_domain_link_built"

    def test_no_input_mutation(self):
        rec = _sample_record()
        original_org_ids = list(rec["organization_node_ids"])
        original_source_ids = list(rec["source_ids"])
        build_shared_domain_link_record(rec, "node-1", "node-2")
        assert rec["organization_node_ids"] == original_org_ids
        assert rec["source_ids"] == original_source_ids


class TestAssembleSharedDomainLinks:
    def test_single_org_yields_no_links(self):
        rec = _sample_record(organization_node_ids=["node-1"])
        result = assemble_shared_domain_links([rec])
        assert result == []

    def test_two_orgs_yields_one_link(self):
        rec = _sample_record(organization_node_ids=["node-1", "node-2"])
        result = assemble_shared_domain_links([rec])
        assert len(result) == 1
        assert result[0]["left_organization_node_id"] == "node-1"
        assert result[0]["right_organization_node_id"] == "node-2"
        assert result[0]["candidate_state"] == "shared_domain_link_built"

    def test_three_orgs_yields_three_links(self):
        rec = _sample_record(organization_node_ids=["node-a", "node-b", "node-c"])
        result = assemble_shared_domain_links([rec])
        assert len(result) == 3

    def test_multiple_input_records_in_order(self):
        rec1 = _sample_record(domain_candidate="alpha.com", organization_node_ids=["node-1", "node-2"])
        rec2 = _sample_record(domain_candidate="beta.com", organization_node_ids=["node-3", "node-4"])
        result = assemble_shared_domain_links([rec1, rec2])
        assert len(result) == 2
        assert result[0]["shared_domain_candidate"] == "alpha.com"
        assert result[1]["shared_domain_candidate"] == "beta.com"

    def test_state_transition(self):
        rec = _sample_record()
        result = assemble_shared_domain_links([rec])
        for link in result:
            assert link["candidate_state"] == "shared_domain_link_built"

    def test_no_input_mutation(self):
        rec = _sample_record()
        original = {k: list(v) if isinstance(v, list) else v for k, v in rec.items()}
        assemble_shared_domain_links([rec])
        for k, v in original.items():
            assert rec[k] == v

    def test_invalid_input_raises(self):
        rec = _sample_record()
        rec["candidate_state"] = "wrong_state"
        with pytest.raises(ValueError):
            assemble_shared_domain_links([rec])

    def test_supporting_lists_are_copies(self):
        rec = _sample_record()
        result = assemble_shared_domain_links([rec])
        result[0]["supporting_source_ids"].append("injected")
        assert "injected" not in rec["source_ids"]
