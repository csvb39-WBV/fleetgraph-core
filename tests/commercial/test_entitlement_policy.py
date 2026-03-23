from __future__ import annotations

from copy import deepcopy

import pytest

from fleetgraph_core.commercial.entitlement_policy import evaluate_entitlement_policy


def _valid_input(
    operation_type: str = "ingest",
    ingest_allowed: bool = True,
    retrieve_allowed: bool = True,
    reprocess_allowed: bool = True,
) -> dict[str, object]:
    return {
        "client_id": "client-001",
        "operation_type": operation_type,
        "subscription_tier": "pro",
        "limits": {
            "ingest_allowed": ingest_allowed,
            "retrieve_allowed": retrieve_allowed,
            "reprocess_allowed": reprocess_allowed,
        },
    }


class TestAllowedPaths:
    def test_allowed_ingest(self) -> None:
        result = evaluate_entitlement_policy(
            _valid_input(operation_type="ingest", ingest_allowed=True)
        )

        assert result == {
            "status": "allowed",
            "reasons": ["operation_allowed"],
        }

    def test_allowed_retrieve(self) -> None:
        result = evaluate_entitlement_policy(
            _valid_input(operation_type="retrieve", retrieve_allowed=True)
        )

        assert result == {
            "status": "allowed",
            "reasons": ["operation_allowed"],
        }

    def test_allowed_reprocess(self) -> None:
        result = evaluate_entitlement_policy(
            _valid_input(operation_type="reprocess", reprocess_allowed=True)
        )

        assert result == {
            "status": "allowed",
            "reasons": ["operation_allowed"],
        }


class TestDeniedPathsAndReasonMapping:
    def test_denied_ingest_reason(self) -> None:
        result = evaluate_entitlement_policy(
            _valid_input(operation_type="ingest", ingest_allowed=False)
        )

        assert result == {
            "status": "denied",
            "reasons": ["ingest_not_allowed"],
        }

    def test_denied_retrieve_reason(self) -> None:
        result = evaluate_entitlement_policy(
            _valid_input(operation_type="retrieve", retrieve_allowed=False)
        )

        assert result == {
            "status": "denied",
            "reasons": ["retrieve_not_allowed"],
        }

    def test_denied_reprocess_reason(self) -> None:
        result = evaluate_entitlement_policy(
            _valid_input(operation_type="reprocess", reprocess_allowed=False)
        )

        assert result == {
            "status": "denied",
            "reasons": ["reprocess_not_allowed"],
        }


class TestDeterminismAndOrdering:
    def test_deterministic_repeated_calls(self) -> None:
        payload = _valid_input(operation_type="retrieve", retrieve_allowed=False)

        first = evaluate_entitlement_policy(payload)
        second = evaluate_entitlement_policy(payload)

        assert first == second

    def test_exact_output_key_order(self) -> None:
        result = evaluate_entitlement_policy(_valid_input())

        assert tuple(result.keys()) == ("status", "reasons")


class TestInputImmutability:
    def test_input_not_mutated(self) -> None:
        payload = _valid_input(operation_type="reprocess", reprocess_allowed=False)
        before = deepcopy(payload)

        evaluate_entitlement_policy(payload)

        assert payload == before


class TestMalformedInputRejection:
    def test_non_dict_input_rejected(self) -> None:
        with pytest.raises(TypeError, match="policy_input must be a dict"):
            evaluate_entitlement_policy("bad")

    def test_missing_top_level_key_rejected(self) -> None:
        payload = _valid_input()
        del payload["limits"]

        with pytest.raises(ValueError, match="missing keys"):
            evaluate_entitlement_policy(payload)

    def test_extra_top_level_key_rejected(self) -> None:
        payload = _valid_input()
        payload["unexpected"] = "x"

        with pytest.raises(ValueError, match="extra keys"):
            evaluate_entitlement_policy(payload)

    def test_client_id_not_string_rejected(self) -> None:
        payload = _valid_input()
        payload["client_id"] = 1

        with pytest.raises(ValueError, match="client_id must be a non-empty string"):
            evaluate_entitlement_policy(payload)

    def test_client_id_empty_rejected(self) -> None:
        payload = _valid_input()
        payload["client_id"] = ""

        with pytest.raises(ValueError, match="client_id must be a non-empty string"):
            evaluate_entitlement_policy(payload)

    def test_invalid_operation_type_rejected(self) -> None:
        payload = _valid_input()
        payload["operation_type"] = "status"

        with pytest.raises(ValueError, match="operation_type must be one of"):
            evaluate_entitlement_policy(payload)

    def test_invalid_subscription_tier_rejected(self) -> None:
        payload = _valid_input()
        payload["subscription_tier"] = "team"

        with pytest.raises(ValueError, match="subscription_tier must be one of"):
            evaluate_entitlement_policy(payload)

    def test_limits_not_dict_rejected(self) -> None:
        payload = _valid_input()
        payload["limits"] = []

        with pytest.raises(TypeError, match="limits must be a dict"):
            evaluate_entitlement_policy(payload)

    def test_limits_missing_key_rejected(self) -> None:
        payload = _valid_input()
        del payload["limits"]["reprocess_allowed"]

        with pytest.raises(ValueError, match="limits missing keys"):
            evaluate_entitlement_policy(payload)

    def test_limits_extra_key_rejected(self) -> None:
        payload = _valid_input()
        payload["limits"]["status_allowed"] = True

        with pytest.raises(ValueError, match="limits has extra keys"):
            evaluate_entitlement_policy(payload)

    @pytest.mark.parametrize(
        "field,bad_value",
        [
            ("ingest_allowed", "true"),
            ("retrieve_allowed", 1),
            ("reprocess_allowed", None),
        ],
    )
    def test_limits_values_must_be_bool(self, field: str, bad_value: object) -> None:
        payload = _valid_input()
        payload["limits"][field] = bad_value

        with pytest.raises(TypeError, match=f"limits field '{field}' must be a bool"):
            evaluate_entitlement_policy(payload)
