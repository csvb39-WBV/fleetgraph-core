"""
Test suite for P13A-MB4 recompute gate / policy evaluator.
"""

from __future__ import annotations

from copy import deepcopy

import pytest

from fleetgraph_core.persistence.recompute_gate import evaluate_recompute_gate


def make_valid_gate_input() -> dict:
    return {
        "stored_manifest": {
            "matter_id": "matter_001",
            "document_set_version": "v2026.03.23",
            "ingestion_run_id": "ingest_run_abc123",
            "pipeline_version": "pipeline_v1",
            "schema_version": "schema_v1",
            "source_hash": "sha256:abc123",
            "artifact_keys": [
                "artifacts/events/events.json",
                "artifacts/entities/entities.json",
            ],
        },
        "requested_state": {
            "document_set_version": "v2026.03.23",
            "pipeline_version": "pipeline_v1",
            "schema_version": "schema_v1",
            "source_hash": "sha256:abc123",
            "force_reprocess": False,
        },
    }


class TestReuseDecision:
    def test_exact_reuse_decision(self) -> None:
        result = evaluate_recompute_gate(make_valid_gate_input())

        assert result == {
            "decision": "reuse_stored_artifacts",
            "reasons": ["stored_artifacts_valid"],
        }


class TestRecomputeIndividualMismatches:
    def test_recompute_when_document_set_version_mismatch(self) -> None:
        payload = make_valid_gate_input()
        payload["requested_state"]["document_set_version"] = "v2026.03.24"

        result = evaluate_recompute_gate(payload)

        assert result == {
            "decision": "recompute_required",
            "reasons": ["document_set_version_changed"],
        }

    def test_recompute_when_pipeline_version_mismatch(self) -> None:
        payload = make_valid_gate_input()
        payload["requested_state"]["pipeline_version"] = "pipeline_v2"

        result = evaluate_recompute_gate(payload)

        assert result == {
            "decision": "recompute_required",
            "reasons": ["pipeline_version_changed"],
        }

    def test_recompute_when_schema_version_mismatch(self) -> None:
        payload = make_valid_gate_input()
        payload["requested_state"]["schema_version"] = "schema_v2"

        result = evaluate_recompute_gate(payload)

        assert result == {
            "decision": "recompute_required",
            "reasons": ["schema_version_changed"],
        }

    def test_recompute_when_source_hash_mismatch(self) -> None:
        payload = make_valid_gate_input()
        payload["requested_state"]["source_hash"] = "sha256:different"

        result = evaluate_recompute_gate(payload)

        assert result == {
            "decision": "recompute_required",
            "reasons": ["source_hash_changed"],
        }


class TestForceReprocessPath:
    def test_force_reprocess_triggers_recompute(self) -> None:
        payload = make_valid_gate_input()
        payload["requested_state"]["force_reprocess"] = True

        result = evaluate_recompute_gate(payload)

        assert result == {
            "decision": "recompute_required",
            "reasons": ["force_reprocess_requested"],
        }

    def test_force_reprocess_included_after_other_mismatch_reasons(self) -> None:
        payload = make_valid_gate_input()
        payload["requested_state"]["document_set_version"] = "v2026.03.24"
        payload["requested_state"]["force_reprocess"] = True

        result = evaluate_recompute_gate(payload)

        assert result == {
            "decision": "recompute_required",
            "reasons": [
                "document_set_version_changed",
                "force_reprocess_requested",
            ],
        }


class TestMultipleMismatchReasonOrder:
    def test_multiple_mismatches_have_deterministic_reason_order(self) -> None:
        payload = make_valid_gate_input()
        payload["requested_state"]["document_set_version"] = "vX"
        payload["requested_state"]["pipeline_version"] = "pX"
        payload["requested_state"]["schema_version"] = "sX"
        payload["requested_state"]["source_hash"] = "hX"
        payload["requested_state"]["force_reprocess"] = True

        result = evaluate_recompute_gate(payload)

        assert result == {
            "decision": "recompute_required",
            "reasons": [
                "document_set_version_changed",
                "pipeline_version_changed",
                "schema_version_changed",
                "source_hash_changed",
                "force_reprocess_requested",
            ],
        }


class TestOutputContract:
    def test_exact_output_key_order(self) -> None:
        result = evaluate_recompute_gate(make_valid_gate_input())

        assert tuple(result.keys()) == ("decision", "reasons")

    def test_reasons_is_list(self) -> None:
        result = evaluate_recompute_gate(make_valid_gate_input())

        assert isinstance(result["reasons"], list)


class TestDeterminism:
    def test_deterministic_repeated_calls(self) -> None:
        payload = make_valid_gate_input()

        first = evaluate_recompute_gate(payload)
        second = evaluate_recompute_gate(payload)

        assert first == second


class TestInputImmutability:
    def test_input_not_mutated(self) -> None:
        payload = make_valid_gate_input()
        before = deepcopy(payload)

        evaluate_recompute_gate(payload)

        assert payload == before


class TestMalformedInputRejection:
    def test_non_dict_input_rejected(self) -> None:
        with pytest.raises(TypeError, match="gate_input must be a dict"):
            evaluate_recompute_gate("not a dict")  # type: ignore[arg-type]

    def test_missing_top_level_keys_rejected(self) -> None:
        payload = make_valid_gate_input()
        del payload["requested_state"]

        with pytest.raises(ValueError, match="missing required fields"):
            evaluate_recompute_gate(payload)

    def test_extra_top_level_keys_rejected(self) -> None:
        payload = make_valid_gate_input()
        payload["extra"] = "x"

        with pytest.raises(ValueError, match="unexpected fields"):
            evaluate_recompute_gate(payload)

    def test_stored_manifest_not_dict_rejected(self) -> None:
        payload = make_valid_gate_input()
        payload["stored_manifest"] = "not a dict"

        with pytest.raises(TypeError, match="stored_manifest.*type dict"):
            evaluate_recompute_gate(payload)

    def test_requested_state_not_dict_rejected(self) -> None:
        payload = make_valid_gate_input()
        payload["requested_state"] = "not a dict"

        with pytest.raises(TypeError, match="requested_state.*type dict"):
            evaluate_recompute_gate(payload)

    def test_malformed_stored_manifest_rejected(self) -> None:
        payload = make_valid_gate_input()
        del payload["stored_manifest"]["source_hash"]

        with pytest.raises(ValueError, match="manifest_input is missing required fields"):
            evaluate_recompute_gate(payload)

    def test_missing_requested_state_keys_rejected(self) -> None:
        payload = make_valid_gate_input()
        del payload["requested_state"]["schema_version"]

        with pytest.raises(ValueError, match="requested_state'.*missing required fields"):
            evaluate_recompute_gate(payload)

    def test_extra_requested_state_keys_rejected(self) -> None:
        payload = make_valid_gate_input()
        payload["requested_state"]["extra"] = "x"

        with pytest.raises(ValueError, match="requested_state'.*unexpected fields"):
            evaluate_recompute_gate(payload)

    @pytest.mark.parametrize(
        "field,bad_value",
        [
            ("document_set_version", 1),
            ("pipeline_version", 1),
            ("schema_version", 1),
            ("source_hash", 1),
        ],
    )
    def test_non_string_requested_state_fields_rejected(
        self,
        field: str,
        bad_value: object,
    ) -> None:
        payload = make_valid_gate_input()
        payload["requested_state"][field] = bad_value

        with pytest.raises(TypeError, match=f"requested_state\\.{field}'.*type str"):
            evaluate_recompute_gate(payload)

    @pytest.mark.parametrize(
        "field",
        [
            "document_set_version",
            "pipeline_version",
            "schema_version",
            "source_hash",
        ],
    )
    def test_empty_requested_state_string_fields_rejected(self, field: str) -> None:
        payload = make_valid_gate_input()
        payload["requested_state"][field] = ""

        with pytest.raises(ValueError, match=f"requested_state\\.{field}'.*non-empty string"):
            evaluate_recompute_gate(payload)

    def test_force_reprocess_not_bool_rejected(self) -> None:
        payload = make_valid_gate_input()
        payload["requested_state"]["force_reprocess"] = "true"

        with pytest.raises(TypeError, match="force_reprocess'.*type bool"):
            evaluate_recompute_gate(payload)
