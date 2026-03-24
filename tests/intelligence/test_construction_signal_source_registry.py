from __future__ import annotations

import pathlib
import sys

import pytest


PROJECT_ROOT = pathlib.Path(__file__).resolve().parents[2]
SRC_ROOT = PROJECT_ROOT / "src"

if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))


from fleetgraph_core.intelligence.construction_signal_source_registry import (
    get_construction_signal_source,
    get_construction_signal_source_names,
    get_construction_signal_source_registry,
    has_construction_signal_source,
)


EXPECTED_SOURCE_NAMES = (
    "court_dockets",
    "regulatory_enforcement",
    "osha_citations",
    "mechanics_liens",
    "bond_claims",
)

EXPECTED_REGISTRY = {
    "court_dockets": {
        "source_name": "court_dockets",
        "signal_category": "litigation",
        "signal_tier": "tier_1",
        "entity_type": "company",
    },
    "regulatory_enforcement": {
        "source_name": "regulatory_enforcement",
        "signal_category": "enforcement",
        "signal_tier": "tier_1",
        "entity_type": "company",
    },
    "osha_citations": {
        "source_name": "osha_citations",
        "signal_category": "safety",
        "signal_tier": "tier_1",
        "entity_type": "company",
    },
    "mechanics_liens": {
        "source_name": "mechanics_liens",
        "signal_category": "payment_risk",
        "signal_tier": "tier_2",
        "entity_type": "project",
    },
    "bond_claims": {
        "source_name": "bond_claims",
        "signal_category": "financial_risk",
        "signal_tier": "tier_2",
        "entity_type": "project",
    },
}


class TestSourceNames:
    def test_source_names_match_exact_ordered_tuple(self) -> None:
        assert get_construction_signal_source_names() == EXPECTED_SOURCE_NAMES

    def test_source_names_return_tuple_shape(self) -> None:
        source_names = get_construction_signal_source_names()

        assert isinstance(source_names, tuple)

        with pytest.raises(TypeError):
            source_names[0] = "other"


class TestRegistryContents:
    def test_registry_contains_exact_required_sources(self) -> None:
        registry = get_construction_signal_source_registry()

        assert tuple(registry.keys()) == EXPECTED_SOURCE_NAMES

    def test_each_source_has_exact_required_keys(self) -> None:
        registry = get_construction_signal_source_registry()

        for source_record in registry.values():
            assert set(source_record.keys()) == {
                "source_name",
                "signal_category",
                "signal_tier",
                "entity_type",
            }

    def test_registry_contains_exact_expected_values(self) -> None:
        assert get_construction_signal_source_registry() == EXPECTED_REGISTRY


class TestSingleSourceLookupSuccess:
    @pytest.mark.parametrize("source_name", EXPECTED_SOURCE_NAMES)
    def test_lookup_returns_expected_source_record(self, source_name: str) -> None:
        assert get_construction_signal_source(source_name) == EXPECTED_REGISTRY[source_name]

    def test_whitespace_padded_source_name_normalizes(self) -> None:
        assert get_construction_signal_source("  court_dockets  ") == EXPECTED_REGISTRY[
            "court_dockets"
        ]


class TestSingleSourceLookupFailure:
    def test_unsupported_source_name_rejected(self) -> None:
        with pytest.raises(
            ValueError,
            match="construction signal source 'unknown_source' is not supported",
        ):
            get_construction_signal_source("unknown_source")

    def test_empty_string_source_name_rejected(self) -> None:
        with pytest.raises(ValueError, match="source_name cannot be empty or whitespace-only"):
            get_construction_signal_source("")

    def test_whitespace_only_source_name_rejected(self) -> None:
        with pytest.raises(ValueError, match="source_name cannot be empty or whitespace-only"):
            get_construction_signal_source("   ")

    @pytest.mark.parametrize("source_name", [None, 1, {}, []])
    def test_non_string_source_name_rejected(self, source_name: object) -> None:
        with pytest.raises(ValueError, match="source_name must be a string"):
            get_construction_signal_source(source_name)  # type: ignore[arg-type]


class TestPresenceCheck:
    @pytest.mark.parametrize("source_name", EXPECTED_SOURCE_NAMES)
    def test_valid_source_names_return_true(self, source_name: str) -> None:
        assert has_construction_signal_source(source_name) is True

    @pytest.mark.parametrize("source_name", ["unknown_source", "", "   "])
    def test_invalid_source_names_return_false(self, source_name: str) -> None:
        assert has_construction_signal_source(source_name) is False

    @pytest.mark.parametrize("source_name", [None, 1, {}, []])
    def test_non_string_source_names_return_false(self, source_name: object) -> None:
        assert has_construction_signal_source(source_name) is False  # type: ignore[arg-type]

    def test_presence_check_whitespace_normalization(self) -> None:
        assert has_construction_signal_source("  bond_claims  ") is True


class TestCopySafety:
    def test_mutating_returned_registry_does_not_mutate_internal_state(self) -> None:
        returned_registry = get_construction_signal_source_registry()
        returned_registry["court_dockets"]["signal_category"] = "changed"

        assert get_construction_signal_source_registry() == EXPECTED_REGISTRY

    def test_mutating_returned_source_does_not_mutate_internal_state(self) -> None:
        returned_source = get_construction_signal_source("court_dockets")
        returned_source["signal_category"] = "changed"

        assert get_construction_signal_source("court_dockets") == EXPECTED_REGISTRY[
            "court_dockets"
        ]

    def test_repeated_calls_remain_stable(self) -> None:
        first_registry = get_construction_signal_source_registry()
        second_registry = get_construction_signal_source_registry()

        assert first_registry == EXPECTED_REGISTRY
        assert second_registry == EXPECTED_REGISTRY
        assert first_registry is not second_registry

        first_source = get_construction_signal_source("bond_claims")
        second_source = get_construction_signal_source("bond_claims")

        assert first_source == EXPECTED_REGISTRY["bond_claims"]
        assert second_source == EXPECTED_REGISTRY["bond_claims"]
        assert first_source is not second_source
