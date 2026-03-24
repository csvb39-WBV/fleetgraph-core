"""Tests for deterministic vertical configuration."""

import pytest
import pathlib
import sys


PROJECT_ROOT = pathlib.Path(__file__).resolve().parents[2]
SRC_ROOT = PROJECT_ROOT / "src"

if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from fleetgraph_core.config.vertical_config import (
    get_supported_verticals,
    is_supported_vertical,
    validate_vertical,
    get_active_vertical,
)


class TestGetSupportedVerticals:
    """Tests for get_supported_verticals function."""

    def test_returns_tuple(self):
        """get_supported_verticals returns a tuple."""
        result = get_supported_verticals()
        assert isinstance(result, tuple)

    def test_returns_both_supported_values(self):
        """get_supported_verticals includes both supported vertical names."""
        result = get_supported_verticals()
        assert "fleet" in result
        assert "construction_audit_litigation" in result

    def test_tuple_is_immutable(self):
        """get_supported_verticals returns an immutable tuple."""
        result = get_supported_verticals()
        with pytest.raises(AttributeError):
            result.append("other")

    def test_consistent_ordering(self):
        """get_supported_verticals returns values in consistent order."""
        result1 = get_supported_verticals()
        result2 = get_supported_verticals()
        assert result1 == result2


class TestValidateVertical:
    """Tests for validate_vertical function."""

    def test_validate_fleet_success(self):
        """validate_vertical accepts 'fleet'."""
        result = validate_vertical("fleet")
        assert result == "fleet"

    def test_validate_construction_audit_litigation_success(self):
        """validate_vertical accepts 'construction_audit_litigation'."""
        result = validate_vertical("construction_audit_litigation")
        assert result == "construction_audit_litigation"

    def test_validate_with_leading_whitespace(self):
        """validate_vertical normalizes leading whitespace."""
        result = validate_vertical("  fleet")
        assert result == "fleet"

    def test_validate_with_trailing_whitespace(self):
        """validate_vertical normalizes trailing whitespace."""
        result = validate_vertical("fleet  ")
        assert result == "fleet"

    def test_validate_with_surrounding_whitespace(self):
        """validate_vertical normalizes both leading and trailing whitespace."""
        result = validate_vertical("  construction_audit_litigation  ")
        assert result == "construction_audit_litigation"

    def test_validate_rejects_unsupported_string(self):
        """validate_vertical rejects unsupported vertical name."""
        with pytest.raises(ValueError, match="is not supported"):
            validate_vertical("construction")

    def test_validate_rejects_empty_string(self):
        """validate_vertical rejects empty string."""
        with pytest.raises(ValueError, match="cannot be empty or whitespace-only"):
            validate_vertical("")

    def test_validate_rejects_whitespace_only_string(self):
        """validate_vertical rejects whitespace-only string."""
        with pytest.raises(ValueError, match="cannot be empty or whitespace-only"):
            validate_vertical("   ")

    def test_validate_rejects_non_string_int(self):
        """validate_vertical rejects integer values."""
        with pytest.raises(ValueError, match="must be a string"):
            validate_vertical(123)

    def test_validate_rejects_non_string_none(self):
        """validate_vertical rejects None."""
        with pytest.raises(ValueError, match="must be a string"):
            validate_vertical(None)

    def test_validate_rejects_non_string_list(self):
        """validate_vertical rejects list values."""
        with pytest.raises(ValueError, match="must be a string"):
            validate_vertical(["fleet"])

    def test_validate_rejects_non_string_dict(self):
        """validate_vertical rejects dict values."""
        with pytest.raises(ValueError, match="must be a string"):
            validate_vertical({"vertical": "fleet"})


class TestIsSupportedVertical:
    """Tests for is_supported_vertical function."""

    def test_is_supported_returns_true_for_fleet(self):
        """is_supported_vertical returns True for 'fleet'."""
        assert is_supported_vertical("fleet") is True

    def test_is_supported_returns_true_for_construction_audit_litigation(self):
        """is_supported_vertical returns True for 'construction_audit_litigation'."""
        assert is_supported_vertical("construction_audit_litigation") is True

    def test_is_supported_returns_false_for_unsupported_string(self):
        """is_supported_vertical returns False for unsupported values."""
        assert is_supported_vertical("construction") is False
        assert is_supported_vertical("audit") is False
        assert is_supported_vertical("litigation") is False

    def test_is_supported_returns_false_for_empty_string(self):
        """is_supported_vertical returns False for empty string."""
        assert is_supported_vertical("") is False

    def test_is_supported_returns_false_for_whitespace_only_string(self):
        """is_supported_vertical returns False for whitespace-only string."""
        assert is_supported_vertical("   ") is False

    def test_is_supported_returns_false_for_non_string_int(self):
        """is_supported_vertical returns False for integer."""
        assert is_supported_vertical(123) is False

    def test_is_supported_returns_false_for_non_string_none(self):
        """is_supported_vertical returns False for None."""
        assert is_supported_vertical(None) is False

    def test_is_supported_returns_false_for_non_string_list(self):
        """is_supported_vertical returns False for list."""
        assert is_supported_vertical(["fleet"]) is False

    def test_is_supported_returns_false_for_non_string_dict(self):
        """is_supported_vertical returns False for dict."""
        assert is_supported_vertical({"vertical": "fleet"}) is False

    def test_is_supported_handles_whitespace_normalization(self):
        """is_supported_vertical normalizes whitespace before checking."""
        assert is_supported_vertical("  fleet  ") is True
        assert is_supported_vertical("  construction_audit_litigation  ") is True


class TestGetActiveVerticalDefaults:
    """Tests for get_active_vertical with default behavior."""

    def test_get_active_vertical_none_returns_fleet(self):
        """get_active_vertical returns 'fleet' when runtime_config is None."""
        result = get_active_vertical(None)
        assert result == "fleet"

    def test_get_active_vertical_empty_mapping_returns_fleet(self):
        """get_active_vertical returns 'fleet' for empty mapping."""
        result = get_active_vertical({})
        assert result == "fleet"

    def test_get_active_vertical_mapping_without_vertical_returns_fleet(self):
        """get_active_vertical returns 'fleet' when vertical key is missing."""
        result = get_active_vertical({"other_key": "some_value"})
        assert result == "fleet"


class TestGetActiveVerticalExplicitValue:
    """Tests for get_active_vertical with explicit vertical values."""

    def test_get_active_vertical_explicit_fleet(self):
        """get_active_vertical returns 'fleet' when explicitly specified."""
        result = get_active_vertical({"vertical": "fleet"})
        assert result == "fleet"

    def test_get_active_vertical_explicit_construction_audit_litigation(self):
        """get_active_vertical returns 'construction_audit_litigation' when specified."""
        result = get_active_vertical({"vertical": "construction_audit_litigation"})
        assert result == "construction_audit_litigation"

    def test_get_active_vertical_with_whitespace_padded_value(self):
        """get_active_vertical normalizes whitespace in explicit value."""
        result = get_active_vertical({"vertical": "  fleet  "})
        assert result == "fleet"

    def test_get_active_vertical_with_whitespace_padded_construction(self):
        """get_active_vertical normalizes whitespace for construction_audit_litigation."""
        result = get_active_vertical({"vertical": "  construction_audit_litigation  "})
        assert result == "construction_audit_litigation"


class TestGetActiveVerticalFailure:
    """Tests for get_active_vertical error conditions."""

    def test_get_active_vertical_rejects_invalid_string(self):
        """get_active_vertical raises ValueError for unsupported vertical."""
        with pytest.raises(ValueError, match="is not supported"):
            get_active_vertical({"vertical": "construction"})

    def test_get_active_vertical_rejects_blank_string(self):
        """get_active_vertical raises ValueError for empty string."""
        with pytest.raises(ValueError, match="cannot be empty or whitespace-only"):
            get_active_vertical({"vertical": ""})

    def test_get_active_vertical_rejects_whitespace_only_string(self):
        """get_active_vertical raises ValueError for whitespace-only string."""
        with pytest.raises(ValueError, match="cannot be empty or whitespace-only"):
            get_active_vertical({"vertical": "   "})

    def test_get_active_vertical_rejects_non_string_vertical_int(self):
        """get_active_vertical raises ValueError for non-string vertical field."""
        with pytest.raises(ValueError, match="must be a string"):
            get_active_vertical({"vertical": 123})

    def test_get_active_vertical_rejects_non_string_vertical_none(self):
        """get_active_vertical raises ValueError for None vertical field."""
        with pytest.raises(ValueError, match="must be a string"):
            get_active_vertical({"vertical": None})

    def test_get_active_vertical_rejects_non_string_vertical_list(self):
        """get_active_vertical raises ValueError for list vertical field."""
        with pytest.raises(ValueError, match="must be a string"):
            get_active_vertical({"vertical": ["fleet"]})

    def test_get_active_vertical_rejects_non_mapping_runtime_config(self):
        """get_active_vertical raises ValueError for non-mapping runtime_config."""
        with pytest.raises(ValueError, match="must be a mapping"):
            get_active_vertical("fleet")

    def test_get_active_vertical_rejects_non_mapping_list_runtime_config(self):
        """get_active_vertical raises ValueError for list runtime_config."""
        with pytest.raises(ValueError, match="must be a mapping"):
            get_active_vertical(["fleet"])

    def test_get_active_vertical_rejects_non_mapping_int_runtime_config(self):
        """get_active_vertical raises ValueError for int runtime_config."""
        with pytest.raises(ValueError, match="must be a mapping"):
            get_active_vertical(123)


class TestGetActiveVerticalIntegration:
    """Integration tests for get_active_vertical with various config structures."""

    def test_get_active_vertical_with_multiple_config_keys(self):
        """get_active_vertical works with config containing multiple keys."""
        config = {
            "vertical": "construction_audit_litigation",
            "environment": "production",
            "debug": False,
        }
        result = get_active_vertical(config)
        assert result == "construction_audit_litigation"

    def test_get_active_vertical_default_when_vertical_not_in_large_config(self):
        """get_active_vertical defaults to fleet when vertical key missing in large config."""
        config = {
            "environment": "production",
            "debug": False,
            "timeout": 30,
        }
        result = get_active_vertical(config)
        assert result == "fleet"
