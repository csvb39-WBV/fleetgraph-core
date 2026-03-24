"""Tests for deterministic fleet signal source registry."""

import pytest
import pathlib
import sys


PROJECT_ROOT = pathlib.Path(__file__).resolve().parents[2]
SRC_ROOT = PROJECT_ROOT / "src"

if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from fleetgraph_core.intelligence.fleet_signal_source_registry import (
    get_fleet_signal_source_registry,
    get_fleet_signal_source,
    has_fleet_signal_source,
    get_fleet_signal_source_names,
)


class TestGetFleetSignalSourceNames:
    """Tests for get_fleet_signal_source_names function."""

    def test_returns_tuple(self):
        """get_fleet_signal_source_names returns a tuple."""
        result = get_fleet_signal_source_names()
        assert isinstance(result, tuple)

    def test_returns_all_five_sources(self):
        """get_fleet_signal_source_names includes all five required source names."""
        result = get_fleet_signal_source_names()
        assert "permit" in result
        assert "rfp" in result
        assert "company" in result
        assert "partner" in result
        assert "telematics" in result

    def test_returns_exactly_five_sources(self):
        """get_fleet_signal_source_names returns exactly five sources."""
        result = get_fleet_signal_source_names()
        assert len(result) == 5

    def test_tuple_is_immutable(self):
        """get_fleet_signal_source_names returns an immutable tuple."""
        result = get_fleet_signal_source_names()
        with pytest.raises(AttributeError):
            result.append("other")

    def test_consistent_ordering(self):
        """get_fleet_signal_source_names returns deterministic ordering."""
        result1 = get_fleet_signal_source_names()
        result2 = get_fleet_signal_source_names()
        assert result1 == result2

    def test_deterministic_source_order(self):
        """get_fleet_signal_source_names returns sources in expected order."""
        result = get_fleet_signal_source_names()
        # Order should match registry definition
        assert result == ("permit", "rfp", "company", "partner", "telematics")


class TestGetFleetSignalSourceRegistry:
    """Tests for get_fleet_signal_source_registry function."""

    def test_returns_dict(self):
        """get_fleet_signal_source_registry returns a dict."""
        result = get_fleet_signal_source_registry()
        assert isinstance(result, dict)

    def test_contains_all_five_sources(self):
        """Registry contains all five required source names."""
        result = get_fleet_signal_source_registry()
        assert "permit" in result
        assert "rfp" in result
        assert "company" in result
        assert "partner" in result
        assert "telematics" in result

    def test_contains_exactly_five_sources(self):
        """Registry contains exactly five sources."""
        result = get_fleet_signal_source_registry()
        assert len(result) == 5

    def test_permit_source_has_correct_fields(self):
        """Permit source has exact required fields with correct values."""
        result = get_fleet_signal_source_registry()
        permit = result["permit"]
        assert set(permit.keys()) == {"source_name", "signal_category", "signal_tier", "entity_type"}
        assert permit["source_name"] == "permit"
        assert permit["signal_category"] == "demand"
        assert permit["signal_tier"] == "tier_1"
        assert permit["entity_type"] == "company"

    def test_rfp_source_has_correct_fields(self):
        """RFP source has exact required fields with correct values."""
        result = get_fleet_signal_source_registry()
        rfp = result["rfp"]
        assert set(rfp.keys()) == {"source_name", "signal_category", "signal_tier", "entity_type"}
        assert rfp["source_name"] == "rfp"
        assert rfp["signal_category"] == "procurement"
        assert rfp["signal_tier"] == "tier_1"
        assert rfp["entity_type"] == "company"

    def test_company_source_has_correct_fields(self):
        """Company source has exact required fields with correct values."""
        result = get_fleet_signal_source_registry()
        company = result["company"]
        assert set(company.keys()) == {"source_name", "signal_category", "signal_tier", "entity_type"}
        assert company["source_name"] == "company"
        assert company["signal_category"] == "entity"
        assert company["signal_tier"] == "tier_2"
        assert company["entity_type"] == "company"

    def test_partner_source_has_correct_fields(self):
        """Partner source has exact required fields with correct values."""
        result = get_fleet_signal_source_registry()
        partner = result["partner"]
        assert set(partner.keys()) == {"source_name", "signal_category", "signal_tier", "entity_type"}
        assert partner["source_name"] == "partner"
        assert partner["signal_category"] == "relationship"
        assert partner["signal_tier"] == "tier_2"
        assert partner["entity_type"] == "company"

    def test_telematics_source_has_correct_fields(self):
        """Telematics source has exact required fields with correct values."""
        result = get_fleet_signal_source_registry()
        telematics = result["telematics"]
        assert set(telematics.keys()) == {"source_name", "signal_category", "signal_tier", "entity_type"}
        assert telematics["source_name"] == "telematics"
        assert telematics["signal_category"] == "usage"
        assert telematics["signal_tier"] == "tier_3"
        assert telematics["entity_type"] == "asset"

    def test_registry_copy_does_not_expose_internal_state(self):
        """Registry copy is independent from internal state."""
        registry1 = get_fleet_signal_source_registry()
        registry2 = get_fleet_signal_source_registry()
        assert registry1 == registry2
        assert registry1 is not registry2


class TestGetFleetSignalSourceSuccess:
    """Tests for get_fleet_signal_source successful lookups."""

    def test_get_permit_source(self):
        """Get permit source returns correct dictionary."""
        result = get_fleet_signal_source("permit")
        assert result["source_name"] == "permit"
        assert result["signal_category"] == "demand"
        assert result["signal_tier"] == "tier_1"
        assert result["entity_type"] == "company"

    def test_get_rfp_source(self):
        """Get rfp source returns correct dictionary."""
        result = get_fleet_signal_source("rfp")
        assert result["source_name"] == "rfp"
        assert result["signal_category"] == "procurement"
        assert result["signal_tier"] == "tier_1"
        assert result["entity_type"] == "company"

    def test_get_company_source(self):
        """Get company source returns correct dictionary."""
        result = get_fleet_signal_source("company")
        assert result["source_name"] == "company"
        assert result["signal_category"] == "entity"
        assert result["signal_tier"] == "tier_2"
        assert result["entity_type"] == "company"

    def test_get_partner_source(self):
        """Get partner source returns correct dictionary."""
        result = get_fleet_signal_source("partner")
        assert result["source_name"] == "partner"
        assert result["signal_category"] == "relationship"
        assert result["signal_tier"] == "tier_2"
        assert result["entity_type"] == "company"

    def test_get_telematics_source(self):
        """Get telematics source returns correct dictionary."""
        result = get_fleet_signal_source("telematics")
        assert result["source_name"] == "telematics"
        assert result["signal_category"] == "usage"
        assert result["signal_tier"] == "tier_3"
        assert result["entity_type"] == "asset"

    def test_get_source_with_leading_whitespace(self):
        """Get source normalizes leading whitespace."""
        result = get_fleet_signal_source("  permit")
        assert result["source_name"] == "permit"

    def test_get_source_with_trailing_whitespace(self):
        """Get source normalizes trailing whitespace."""
        result = get_fleet_signal_source("permit  ")
        assert result["source_name"] == "permit"

    def test_get_source_with_surrounding_whitespace(self):
        """Get source normalizes surrounding whitespace."""
        result = get_fleet_signal_source("  rfp  ")
        assert result["source_name"] == "rfp"

    def test_get_source_returns_copy_safe_dict(self):
        """Get source returns a copy-safe dictionary."""
        result1 = get_fleet_signal_source("permit")
        result2 = get_fleet_signal_source("permit")
        assert result1 == result2
        assert result1 is not result2


class TestGetFleetSignalSourceFailure:
    """Tests for get_fleet_signal_source error conditions."""

    def test_get_source_rejects_unsupported_name(self):
        """Get source raises ValueError for unsupported source name."""
        with pytest.raises(ValueError, match="is not a supported fleet signal source"):
            get_fleet_signal_source("construction")

    def test_get_source_rejects_empty_string(self):
        """Get source raises ValueError for empty string."""
        with pytest.raises(ValueError, match="cannot be empty or whitespace-only"):
            get_fleet_signal_source("")

    def test_get_source_rejects_whitespace_only_string(self):
        """Get source raises ValueError for whitespace-only string."""
        with pytest.raises(ValueError, match="cannot be empty or whitespace-only"):
            get_fleet_signal_source("   ")

    def test_get_source_rejects_non_string_int(self):
        """Get source raises ValueError for integer."""
        with pytest.raises(ValueError, match="must be a string"):
            get_fleet_signal_source(123)

    def test_get_source_rejects_non_string_none(self):
        """Get source raises ValueError for None."""
        with pytest.raises(ValueError, match="must be a string"):
            get_fleet_signal_source(None)

    def test_get_source_rejects_non_string_list(self):
        """Get source raises ValueError for list."""
        with pytest.raises(ValueError, match="must be a string"):
            get_fleet_signal_source(["permit"])

    def test_get_source_rejects_non_string_dict(self):
        """Get source raises ValueError for dict."""
        with pytest.raises(ValueError, match="must be a string"):
            get_fleet_signal_source({"source_name": "permit"})


class TestHasFleetSignalSource:
    """Tests for has_fleet_signal_source function."""

    def test_has_permit_source(self):
        """has_fleet_signal_source returns True for permit."""
        assert has_fleet_signal_source("permit") is True

    def test_has_rfp_source(self):
        """has_fleet_signal_source returns True for rfp."""
        assert has_fleet_signal_source("rfp") is True

    def test_has_company_source(self):
        """has_fleet_signal_source returns True for company."""
        assert has_fleet_signal_source("company") is True

    def test_has_partner_source(self):
        """has_fleet_signal_source returns True for partner."""
        assert has_fleet_signal_source("partner") is True

    def test_has_telematics_source(self):
        """has_fleet_signal_source returns True for telematics."""
        assert has_fleet_signal_source("telematics") is True

    def test_has_source_with_leading_whitespace(self):
        """has_fleet_signal_source normalizes leading whitespace."""
        assert has_fleet_signal_source("  permit") is True

    def test_has_source_with_trailing_whitespace(self):
        """has_fleet_signal_source normalizes trailing whitespace."""
        assert has_fleet_signal_source("permit  ") is True

    def test_has_source_with_surrounding_whitespace(self):
        """has_fleet_signal_source normalizes surrounding whitespace."""
        assert has_fleet_signal_source("  rfp  ") is True

    def test_has_source_returns_false_for_unsupported(self):
        """has_fleet_signal_source returns False for unsupported source."""
        assert has_fleet_signal_source("construction") is False

    def test_has_source_returns_false_for_empty_string(self):
        """has_fleet_signal_source returns False for empty string."""
        assert has_fleet_signal_source("") is False

    def test_has_source_returns_false_for_whitespace_only_string(self):
        """has_fleet_signal_source returns False for whitespace-only string."""
        assert has_fleet_signal_source("   ") is False

    def test_has_source_returns_false_for_non_string_int(self):
        """has_fleet_signal_source returns False for integer (no exception)."""
        assert has_fleet_signal_source(123) is False

    def test_has_source_returns_false_for_non_string_none(self):
        """has_fleet_signal_source returns False for None (no exception)."""
        assert has_fleet_signal_source(None) is False

    def test_has_source_returns_false_for_non_string_list(self):
        """has_fleet_signal_source returns False for list (no exception)."""
        assert has_fleet_signal_source(["permit"]) is False

    def test_has_source_returns_false_for_non_string_dict(self):
        """has_fleet_signal_source returns False for dict (no exception)."""
        assert has_fleet_signal_source({"source_name": "permit"}) is False


class TestCopySafety:
    """Tests for copy safety of returned dictionaries."""

    def test_modifying_registry_copy_does_not_mutate_internal_state(self):
        """Modifying returned registry does not affect internal state."""
        registry1 = get_fleet_signal_source_registry()
        registry1["fake_source"] = {"source_name": "fake"}
        
        registry2 = get_fleet_signal_source_registry()
        assert "fake_source" not in registry2
        assert len(registry2) == 5

    def test_modifying_source_dict_does_not_mutate_internal_state(self):
        """Modifying returned source dictionary does not affect internal state."""
        source1 = get_fleet_signal_source("permit")
        source1["signal_category"] = "modified"
        
        source2 = get_fleet_signal_source("permit")
        assert source2["signal_category"] == "demand"

    def test_modifying_source_field_in_registry_copy_does_not_mutate_internal_state(self):
        """Modifying nested source in registry copy doesn't affect internal state."""
        registry1 = get_fleet_signal_source_registry()
        registry1["permit"]["signal_category"] = "modified"
        
        registry2 = get_fleet_signal_source_registry()
        assert registry2["permit"]["signal_category"] == "demand"

    def test_repeated_calls_remain_stable(self):
        """Repeated calls to get functions return stable values."""
        for _ in range(10):
            registry = get_fleet_signal_source_registry()
            assert len(registry) == 5
            assert registry["permit"]["signal_category"] == "demand"
            
            permit = get_fleet_signal_source("permit")
            assert permit["source_name"] == "permit"
            
            assert has_fleet_signal_source("permit") is True
            
            names = get_fleet_signal_source_names()
            assert len(names) == 5
