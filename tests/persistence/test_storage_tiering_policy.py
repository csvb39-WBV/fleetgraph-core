"""
Test suite for D13-MB6 storage tiering policy evaluator.
"""

from __future__ import annotations

from copy import deepcopy

import pytest

from fleetgraph_core.persistence.storage_tiering_policy import (
    evaluate_storage_tiering_policy,
)


def make_input(
    artifact_category: str = "manifest",
    access_frequency: str = "medium",
    retention_class: str = "standard",
) -> dict:
    return {
        "artifact_category": artifact_category,
        "access_frequency": access_frequency,
        "retention_class": retention_class,
    }


class TestHotTierPath:
    def test_retrieval_projection_high_access_is_hot(self) -> None:
        result = evaluate_storage_tiering_policy(
            make_input("retrieval_projection", "high", "active")
        )

        assert result == {
            "storage_tier": "hot",
            "reasons": ["retrieval_projection_high_access"],
        }


class TestWarmTierPaths:
    def test_manifest_defaults_to_warm(self) -> None:
        result = evaluate_storage_tiering_policy(
            make_input("manifest", "medium", "standard")
        )

        assert result == {
            "storage_tier": "warm",
            "reasons": ["manifest_default_warm"],
        }

    def test_canonical_store_defaults_to_warm(self) -> None:
        result = evaluate_storage_tiering_policy(
            make_input("canonical_store", "medium", "standard")
        )

        assert result == {
            "storage_tier": "warm",
            "reasons": ["canonical_store_default_warm"],
        }

    def test_default_warm_path(self) -> None:
        result = evaluate_storage_tiering_policy(
            make_input("source_reference", "medium", "active")
        )

        assert result == {
            "storage_tier": "warm",
            "reasons": ["default_warm_tier"],
        }


class TestColdTierPaths:
    def test_source_reference_archive_is_cold(self) -> None:
        result = evaluate_storage_tiering_policy(
            make_input("source_reference", "high", "archive")
        )

        assert result == {
            "storage_tier": "cold",
            "reasons": ["source_reference_archive_cold"],
        }

    def test_low_access_archive_is_cold_for_any_category(self) -> None:
        result = evaluate_storage_tiering_policy(
            make_input("manifest", "low", "archive")
        )

        assert result == {
            "storage_tier": "cold",
            "reasons": ["low_access_archive_cold"],
        }


class TestExactReasonValues:
    def test_manifest_low_archive_prefers_low_access_archive_reason(self) -> None:
        result = evaluate_storage_tiering_policy(
            make_input("manifest", "low", "archive")
        )

        assert result["reasons"] == ["low_access_archive_cold"]

    def test_source_reference_low_archive_uses_source_reference_reason(self) -> None:
        result = evaluate_storage_tiering_policy(
            make_input("source_reference", "low", "archive")
        )

        assert result["reasons"] == ["source_reference_archive_cold"]


class TestOutputContract:
    def test_exact_output_key_order(self) -> None:
        result = evaluate_storage_tiering_policy(make_input())

        assert tuple(result.keys()) == ("storage_tier", "reasons")

    def test_reasons_is_list(self) -> None:
        result = evaluate_storage_tiering_policy(make_input())

        assert isinstance(result["reasons"], list)


class TestDeterminism:
    def test_deterministic_repeated_calls(self) -> None:
        payload = make_input("canonical_store", "medium", "active")

        first = evaluate_storage_tiering_policy(payload)
        second = evaluate_storage_tiering_policy(payload)

        assert first == second


class TestInputImmutability:
    def test_input_not_mutated(self) -> None:
        payload = make_input("retrieval_projection", "high", "active")
        before = deepcopy(payload)

        evaluate_storage_tiering_policy(payload)

        assert payload == before


class TestMalformedInputRejection:
    def test_missing_keys_rejected(self) -> None:
        payload = make_input()
        del payload["retention_class"]

        with pytest.raises(ValueError, match="missing required fields"):
            evaluate_storage_tiering_policy(payload)

    def test_extra_keys_rejected(self) -> None:
        payload = make_input()
        payload["unexpected"] = "x"

        with pytest.raises(ValueError, match="unexpected fields"):
            evaluate_storage_tiering_policy(payload)

    @pytest.mark.parametrize(
        "field,bad_value",
        [
            ("artifact_category", 1),
            ("access_frequency", 2),
            ("retention_class", 3),
        ],
    )
    def test_non_string_fields_rejected(self, field: str, bad_value: object) -> None:
        payload = make_input()
        payload[field] = bad_value

        with pytest.raises(TypeError, match=f"field '{field}'.*str"):
            evaluate_storage_tiering_policy(payload)

    @pytest.mark.parametrize(
        "field",
        [
            "artifact_category",
            "access_frequency",
            "retention_class",
        ],
    )
    def test_empty_string_fields_rejected(self, field: str) -> None:
        payload = make_input()
        payload[field] = ""

        with pytest.raises(ValueError, match=f"field '{field}'.*non-empty string"):
            evaluate_storage_tiering_policy(payload)

    def test_invalid_artifact_category_rejected(self) -> None:
        payload = make_input(artifact_category="unknown")

        with pytest.raises(ValueError, match="artifact_category.*must be one of"):
            evaluate_storage_tiering_policy(payload)

    def test_invalid_access_frequency_rejected(self) -> None:
        payload = make_input(access_frequency="burst")

        with pytest.raises(ValueError, match="access_frequency.*must be one of"):
            evaluate_storage_tiering_policy(payload)

    def test_invalid_retention_class_rejected(self) -> None:
        payload = make_input(retention_class="forever")

        with pytest.raises(ValueError, match="retention_class.*must be one of"):
            evaluate_storage_tiering_policy(payload)
