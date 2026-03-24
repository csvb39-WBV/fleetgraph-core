from __future__ import annotations

import pathlib
import sys
from typing import Any

import pytest


PROJECT_ROOT = pathlib.Path(__file__).resolve().parents[2]
SRC_ROOT = PROJECT_ROOT / "src"

if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))


from fleetgraph_core.intelligence import signal_aggregator
from fleetgraph_core.intelligence.construction_signal_source_registry import (
    get_construction_signal_source,
    get_construction_signal_source_names,
    get_construction_signal_source_registry,
    has_construction_signal_source,
)
from fleetgraph_core.intelligence.fleet_signal_source_registry import (
    get_fleet_signal_source,
    get_fleet_signal_source_names,
    get_fleet_signal_source_registry,
    has_fleet_signal_source,
)


@pytest.fixture(autouse=True)
def install_vertical_aware_registry_adapter(monkeypatch: pytest.MonkeyPatch) -> None:
    def resolve_active_vertical(runtime_config: dict[str, Any] | None = None) -> str:
        resolved_vertical = "fleet"

        if runtime_config is not None:
            if not isinstance(runtime_config, dict):
                raise ValueError("runtime_config must be a mapping")

            if "vertical" in runtime_config:
                resolved_vertical = runtime_config["vertical"]

        if not isinstance(resolved_vertical, str):
            raise ValueError("vertical must be a string")

        normalized_vertical = resolved_vertical.strip()
        if normalized_vertical == "":
            raise ValueError("vertical cannot be empty or whitespace-only")

        if normalized_vertical not in {"fleet", "construction_audit_litigation"}:
            raise ValueError(f"vertical '{normalized_vertical}' is not supported")

        return normalized_vertical

    def get_signal_source_registry(
        runtime_config: dict[str, Any] | None = None,
    ) -> dict[str, dict[str, str]]:
        active_vertical = resolve_active_vertical(runtime_config)
        if active_vertical == "fleet":
            return get_fleet_signal_source_registry()
        return get_construction_signal_source_registry()

    def get_signal_source(
        source_name: Any,
        runtime_config: dict[str, Any] | None = None,
    ) -> dict[str, str]:
        active_vertical = resolve_active_vertical(runtime_config)
        if active_vertical == "fleet":
            return get_fleet_signal_source(source_name)
        return get_construction_signal_source(source_name)

    def has_signal_source(source_name: Any, runtime_config: dict[str, Any] | None = None) -> bool:
        active_vertical = resolve_active_vertical(runtime_config)
        if active_vertical == "fleet":
            return has_fleet_signal_source(source_name)
        return has_construction_signal_source(source_name)

    def get_signal_source_names(runtime_config: dict[str, Any] | None = None) -> tuple[str, ...]:
        active_vertical = resolve_active_vertical(runtime_config)
        if active_vertical == "fleet":
            return get_fleet_signal_source_names()
        return get_construction_signal_source_names()

    monkeypatch.setattr(
        signal_aggregator.signal_source_registry,
        "get_signal_source_registry",
        get_signal_source_registry,
        raising=False,
    )
    monkeypatch.setattr(
        signal_aggregator.signal_source_registry,
        "get_signal_source",
        get_signal_source,
        raising=False,
    )
    monkeypatch.setattr(
        signal_aggregator.signal_source_registry,
        "has_signal_source",
        has_signal_source,
        raising=False,
    )
    monkeypatch.setattr(
        signal_aggregator.signal_source_registry,
        "get_signal_source_names",
        get_signal_source_names,
        raising=False,
    )


class TestFleetDefaultBehavior:
    def test_fleet_valid_records_are_accepted_by_default(self) -> None:
        result = signal_aggregator.aggregate_signals(
            [
                {"source_name": "permit", "payload": {"id": 1}},
                {"source_name": "rfp", "payload": {"id": 2}},
            ]
        )

        assert result == {
            "ok": True,
            "vertical": "fleet",
            "accepted_sources": ["permit", "rfp"],
            "rejected_sources": [],
            "accepted_records": [
                {"source_name": "permit", "payload": {"id": 1}},
                {"source_name": "rfp", "payload": {"id": 2}},
            ],
            "rejected_records": [],
            "accepted_count": 2,
            "rejected_count": 0,
        }

    def test_construction_only_records_are_rejected_in_default_fleet_mode(self) -> None:
        result = signal_aggregator.aggregate_signals(
            [{"source_name": "court_dockets", "payload": {"id": 1}}]
        )

        assert result["vertical"] == "fleet"
        assert result["accepted_records"] == []
        assert result["rejected_sources"] == ["court_dockets"]
        assert result["rejected_records"] == [
            {
                "record": {"source_name": "court_dockets", "payload": {"id": 1}},
                "reason": "source_name is not supported for active vertical",
            }
        ]


class TestConstructionBehavior:
    def test_construction_valid_records_are_accepted_in_construction_mode(self) -> None:
        result = signal_aggregator.aggregate_signals(
            [
                {"source_name": "court_dockets", "payload": {"id": 1}},
                {"source_name": "bond_claims", "payload": {"id": 2}},
            ],
            runtime_config={"vertical": "construction_audit_litigation"},
        )

        assert result["vertical"] == "construction_audit_litigation"
        assert result["accepted_sources"] == ["court_dockets", "bond_claims"]
        assert result["rejected_sources"] == []
        assert result["accepted_count"] == 2
        assert result["rejected_count"] == 0

    def test_fleet_only_records_are_rejected_in_construction_mode(self) -> None:
        result = signal_aggregator.aggregate_signals(
            [{"source_name": "permit", "payload": {"id": 1}}],
            runtime_config={"vertical": "construction_audit_litigation"},
        )

        assert result["vertical"] == "construction_audit_litigation"
        assert result["accepted_records"] == []
        assert result["rejected_sources"] == ["permit"]
        assert result["rejected_count"] == 1


class TestMixedBatchBehavior:
    def test_accepted_and_rejected_records_preserve_order(self) -> None:
        payload = [
            {"source_name": "permit", "payload": {"id": 1}},
            {"source_name": "court_dockets", "payload": {"id": 2}},
            {"payload": {"id": 3}},
            {"source_name": "partner", "payload": {"id": 4}},
            "bad-record",
            {"source_name": "court_dockets", "payload": {"id": 5}},
            {"source_name": "permit", "payload": {"id": 6}},
        ]

        result = signal_aggregator.aggregate_signals(payload)

        assert result["accepted_records"] == [
            {"source_name": "permit", "payload": {"id": 1}},
            {"source_name": "partner", "payload": {"id": 4}},
            {"source_name": "permit", "payload": {"id": 6}},
        ]
        assert result["rejected_records"] == [
            {
                "record": {"source_name": "court_dockets", "payload": {"id": 2}},
                "reason": "source_name is not supported for active vertical",
            },
            {
                "record": {"payload": {"id": 3}},
                "reason": "source_name is required",
            },
            {
                "record": "bad-record",
                "reason": "record must be a dictionary",
            },
            {
                "record": {"source_name": "court_dockets", "payload": {"id": 5}},
                "reason": "source_name is not supported for active vertical",
            },
        ]

    def test_unique_source_tracking_preserves_first_seen_order(self) -> None:
        payload = [
            {"source_name": " permit ", "payload": 1},
            {"source_name": "partner", "payload": 2},
            {"source_name": "permit", "payload": 3},
            {"source_name": "court_dockets", "payload": 4},
            {"source_name": "court_dockets", "payload": 5},
            {"source_name": " bond_claims ", "payload": 6},
        ]

        result = signal_aggregator.aggregate_signals(payload)

        assert result["accepted_sources"] == ["permit", "partner"]
        assert result["rejected_sources"] == ["court_dockets", "bond_claims"]
        assert result["accepted_count"] == len(result["accepted_records"])
        assert result["rejected_count"] == len(result["rejected_records"])


class TestPerRecordRejectionBehavior:
    def test_non_dict_item_is_rejected_with_required_reason(self) -> None:
        result = signal_aggregator.aggregate_signals([123])

        assert result["rejected_records"] == [
            {
                "record": 123,
                "reason": "record must be a dictionary",
            }
        ]

    def test_missing_source_name_is_rejected_with_required_reason(self) -> None:
        result = signal_aggregator.aggregate_signals([{"payload": {"id": 1}}])

        assert result["rejected_records"] == [
            {
                "record": {"payload": {"id": 1}},
                "reason": "source_name is required",
            }
        ]

    def test_unsupported_source_is_rejected_with_required_reason(self) -> None:
        result = signal_aggregator.aggregate_signals([{"source_name": "unknown_source"}])

        assert result["rejected_records"] == [
            {
                "record": {"source_name": "unknown_source"},
                "reason": "source_name is not supported for active vertical",
            }
        ]


class TestRuntimeConfigFailurePropagation:
    def test_invalid_runtime_config_raises(self) -> None:
        with pytest.raises(ValueError, match="runtime_config must be a mapping"):
            signal_aggregator.aggregate_signals([], runtime_config=["fleet"])  # type: ignore[arg-type]

    def test_invalid_vertical_raises(self) -> None:
        with pytest.raises(ValueError, match="vertical 'invalid_vertical' is not supported"):
            signal_aggregator.aggregate_signals([], runtime_config={"vertical": "invalid_vertical"})


class TestInputValidation:
    def test_non_list_signal_records_raises_value_error(self) -> None:
        with pytest.raises(ValueError, match="signal_records must be a list"):
            signal_aggregator.aggregate_signals({"records": []})  # type: ignore[arg-type]


class TestCopySafety:
    def test_mutating_returned_accepted_records_does_not_affect_subsequent_calls(self) -> None:
        payload = [{"source_name": "permit", "payload": {"id": 1}}]

        first_result = signal_aggregator.aggregate_signals(payload)
        first_result["accepted_records"][0]["payload"]["id"] = 999

        second_result = signal_aggregator.aggregate_signals(payload)

        assert second_result["accepted_records"] == [
            {"source_name": "permit", "payload": {"id": 1}}
        ]

    def test_mutating_rejected_wrappers_does_not_affect_subsequent_calls(self) -> None:
        payload = [{"source_name": "court_dockets", "payload": {"id": 1}}]

        first_result = signal_aggregator.aggregate_signals(payload)
        first_result["rejected_records"][0]["record"]["payload"]["id"] = 999
        first_result["rejected_records"][0]["reason"] = "changed"

        second_result = signal_aggregator.aggregate_signals(payload)

        assert second_result["rejected_records"] == [
            {
                "record": {"source_name": "court_dockets", "payload": {"id": 1}},
                "reason": "source_name is not supported for active vertical",
            }
        ]


class TestExactOutputShape:
    def test_successful_output_contains_exact_required_top_level_keys(self) -> None:
        result = signal_aggregator.aggregate_signals([])

        assert set(result.keys()) == {
            "ok",
            "vertical",
            "accepted_sources",
            "rejected_sources",
            "accepted_records",
            "rejected_records",
            "accepted_count",
            "rejected_count",
        }

    def test_rejected_wrappers_contain_exact_record_and_reason_keys(self) -> None:
        result = signal_aggregator.aggregate_signals([{"payload": 1}])

        assert set(result["rejected_records"][0].keys()) == {"record", "reason"}
