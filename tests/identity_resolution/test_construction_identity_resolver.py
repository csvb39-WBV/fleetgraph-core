from __future__ import annotations

import copy
import pathlib
import sys

import pytest


PROJECT_ROOT = pathlib.Path(__file__).resolve().parents[2]
SRC_ROOT = PROJECT_ROOT / "src"

if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))


import fleetgraph_core.identity_resolution.construction_identity_resolver as resolver_module
from fleetgraph_core.identity_resolution.construction_identity_resolver import (
    get_supported_construction_identity_types,
    resolve_construction_graph_identities,
    resolve_construction_graph_identity_batch,
)
from fleetgraph_core.intelligence.unified_event_schema import build_unified_event_record


def _event_details_for(event_type: str) -> dict[str, str]:
    if event_type == "litigation":
        return {
            "case_id": "CASE-1",
            "case_type": "civil",
            "filing_date": "2026-03-01",
            "plaintiff_role": "plaintiff",
            "defendant_role": "defendant",
        }
    if event_type == "audit":
        return {
            "audit_id": "AUD-1",
            "issue_type": "compliance",
            "opened_date": "2026-03-01",
            "agency": "Inspector",
        }
    if event_type == "enforcement":
        return {
            "action_id": "ENF-1",
            "issue_type": "safety",
            "opened_date": "2026-03-01",
            "agency": "Regulator",
        }
    if event_type == "lien":
        return {
            "lien_id": "LIEN-1",
            "filing_date": "2026-03-01",
            "claimant_role": "claimant",
        }
    return {
        "bond_claim_id": "BOND-1",
        "filing_date": "2026-03-01",
        "claimant_role": "claimant",
    }


def _make_record(
    event_type: str = "litigation",
    event_id: str = "evt-1",
    company_name: str = "Acme Builders",
    project_name: str | None = "North Tower",
    agency_or_court: str | None = "Superior Court",
) -> dict[str, object]:
    return build_unified_event_record(
        {
            "event_id": event_id,
            "event_type": event_type,
            "company_name": company_name,
            "source_name": "Daily Ledger",
            "status": "open",
            "event_date": "2026-03-01",
            "jurisdiction": "CA",
            "project_name": project_name,
            "agency_or_court": agency_or_court,
            "severity": "medium",
            "amount": 1000,
            "currency": "USD",
            "service_fit": ["legal_monitoring"],
            "trigger_tags": ["construction"],
            "evidence": {
                "summary": "summary",
                "source_record_id": "src-1",
            },
            "event_details": _event_details_for(event_type),
        }
    )


def test_supported_identity_type_tuple_returned_correctly() -> None:
    assert get_supported_construction_identity_types() == (
        "event",
        "company",
        "project",
        "agency",
        "court",
    )


def test_litigation_record_resolves_event_company_project_court() -> None:
    result = resolve_construction_graph_identities(
        _make_record(event_type="litigation", agency_or_court="Superior Court")
    )

    assert result["source_event_id"] == "evt-1"
    assert result["source_event_type"] == "litigation"
    assert [identity["identity_type"] for identity in result["identities"]] == [
        "event",
        "company",
        "project",
        "court",
    ]


def test_audit_record_resolves_event_company_project_agency() -> None:
    result = resolve_construction_graph_identities(
        _make_record(event_type="audit", agency_or_court="Inspector General")
    )

    assert [identity["identity_type"] for identity in result["identities"]] == [
        "event",
        "company",
        "project",
        "agency",
    ]


def test_enforcement_record_resolves_event_company_project_agency() -> None:
    result = resolve_construction_graph_identities(
        _make_record(event_type="enforcement", agency_or_court="State Regulator")
    )

    assert [identity["identity_type"] for identity in result["identities"]] == [
        "event",
        "company",
        "project",
        "agency",
    ]


def test_lien_record_resolves_event_company_project() -> None:
    result = resolve_construction_graph_identities(
        _make_record(event_type="lien", agency_or_court="County Clerk")
    )

    assert [identity["identity_type"] for identity in result["identities"]] == [
        "event",
        "company",
        "project",
    ]


def test_bond_claim_record_resolves_event_company_project() -> None:
    result = resolve_construction_graph_identities(
        _make_record(event_type="bond_claim", agency_or_court="Bond Board")
    )

    assert [identity["identity_type"] for identity in result["identities"]] == [
        "event",
        "company",
        "project",
    ]


def test_top_level_keys_are_exact() -> None:
    result = resolve_construction_graph_identities(_make_record())

    assert set(result.keys()) == {
        "source_event_id",
        "source_event_type",
        "identities",
        "identity_count",
    }


def test_identity_keys_are_exact() -> None:
    result = resolve_construction_graph_identities(_make_record())
    first_identity = result["identities"][0]

    assert set(first_identity.keys()) == {
        "identity_type",
        "node_id",
        "canonical_name",
        "source_event_id",
        "source_event_type",
    }


def test_identity_order_preserved_from_graph_node_order() -> None:
    result = resolve_construction_graph_identities(
        _make_record(event_type="audit", agency_or_court="Agency A")
    )

    assert [identity["node_id"] for identity in result["identities"]] == [
        "audit:evt-1",
        "company:Acme Builders",
        "project:North Tower",
        "agency:Agency A",
    ]


def test_duplicate_node_ids_collapse_to_one_identity_preserving_first_occurrence(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        resolver_module,
        "build_construction_graph",
        lambda record: {
            "nodes": [
                {
                    "node_id": "case:evt-1",
                    "node_type": "case",
                    "label": "Acme Builders case",
                    "properties": {},
                },
                {
                    "node_id": "company:Acme Builders",
                    "node_type": "company",
                    "label": "Acme Builders",
                    "properties": {},
                },
                {
                    "node_id": "company:Acme Builders",
                    "node_type": "company",
                    "label": "Duplicate Company",
                    "properties": {},
                },
            ],
            "edges": [],
            "metadata": {
                "source_event_id": "evt-1",
                "source_event_type": "litigation",
                "node_count": 3,
                "edge_count": 0,
            },
        },
    )

    result = resolve_construction_graph_identities(_make_record())

    assert [identity["node_id"] for identity in result["identities"]] == [
        "case:evt-1",
        "company:Acme Builders",
    ]
    assert result["identities"][1]["canonical_name"] == "Acme Builders"


def test_malformed_graph_metadata_raises_exact_message(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        resolver_module,
        "build_construction_graph",
        lambda record: {
            "nodes": [],
            "edges": [],
            "metadata": {
                "source_event_id": "evt-1",
            },
        },
    )

    with pytest.raises(
        ValueError,
        match="construction identities could not be resolved from the provided record.",
    ):
        resolve_construction_graph_identities(_make_record())


def test_malformed_node_raises_exact_message(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        resolver_module,
        "build_construction_graph",
        lambda record: {
            "nodes": [
                {
                    "node_type": "company",
                    "label": "Acme Builders",
                    "properties": {},
                }
            ],
            "edges": [],
            "metadata": {
                "source_event_id": "evt-1",
                "source_event_type": "audit",
                "node_count": 1,
                "edge_count": 0,
            },
        },
    )

    with pytest.raises(
        ValueError,
        match="construction identities could not be resolved from the provided record.",
    ):
        resolve_construction_graph_identities(_make_record())


def test_unsupported_node_type_raises_exact_message(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        resolver_module,
        "build_construction_graph",
        lambda record: {
            "nodes": [
                {
                    "node_id": "mystery:1",
                    "node_type": "mystery",
                    "label": "Mystery",
                    "properties": {},
                }
            ],
            "edges": [],
            "metadata": {
                "source_event_id": "evt-1",
                "source_event_type": "audit",
                "node_count": 1,
                "edge_count": 0,
            },
        },
    )

    with pytest.raises(
        ValueError,
        match="construction identities could not be resolved from the provided record.",
    ):
        resolve_construction_graph_identities(_make_record())


def test_all_valid_batch_success() -> None:
    result = resolve_construction_graph_identity_batch(
        [
            _make_record(event_type="audit", event_id="evt-1", agency_or_court="Agency A"),
            _make_record(event_type="litigation", event_id="evt-2", agency_or_court="Court A"),
        ]
    )

    assert result["ok"] is True
    assert result["total_records"] == 2
    assert result["valid_records"] == 2
    assert result["invalid_records"] == 0
    assert result["results"] == [
        {"index": 0, "ok": True, "identity_count": 4, "error": None},
        {"index": 1, "ok": True, "identity_count": 4, "error": None},
    ]
    assert len(result["identity_sets"]) == 2


def test_mixed_batch_indexed_failure_reporting() -> None:
    invalid_record = _make_record(event_type="audit", event_id="evt-2", agency_or_court="Agency A")
    invalid_record["status"] = "invalid-status"

    result = resolve_construction_graph_identity_batch(
        [
            _make_record(event_type="litigation", event_id="evt-1", agency_or_court="Court A"),
            invalid_record,
        ]
    )

    assert result["ok"] is False
    assert result["total_records"] == 2
    assert result["valid_records"] == 1
    assert result["invalid_records"] == 1
    assert result["results"][0] == {
        "index": 0,
        "ok": True,
        "identity_count": 4,
        "error": None,
    }
    assert result["results"][1] == {
        "index": 1,
        "ok": False,
        "identity_count": None,
        "error": "status must be one of the supported statuses.",
    }


def test_empty_batch_allowed() -> None:
    result = resolve_construction_graph_identity_batch([])

    assert result == {
        "ok": True,
        "total_records": 0,
        "valid_records": 0,
        "invalid_records": 0,
        "results": [],
        "identity_sets": [],
    }


def test_non_list_batch_raises() -> None:
    with pytest.raises(ValueError, match=r"^records must be a list\.$"):
        resolve_construction_graph_identity_batch({})  # type: ignore[arg-type]


def test_non_dict_batch_item_raises() -> None:
    with pytest.raises(ValueError, match=r"^records\[1\] must be a dictionary\.$"):
        resolve_construction_graph_identity_batch([_make_record(), "bad-item"])  # type: ignore[list-item]


def test_identity_set_order_preserved_across_batch() -> None:
    result = resolve_construction_graph_identity_batch(
        [
            _make_record(event_type="bond_claim", event_id="evt-10", agency_or_court=None),
            _make_record(event_type="audit", event_id="evt-20", agency_or_court="Agency B"),
        ]
    )

    assert [identity_set["source_event_id"] for identity_set in result["identity_sets"]] == [
        "evt-10",
        "evt-20",
    ]


def test_input_record_not_mutated() -> None:
    record = _make_record(event_type="audit", agency_or_court="Agency A")
    snapshot = copy.deepcopy(record)

    _ = resolve_construction_graph_identities(record)

    assert record == snapshot


def test_output_mutation_does_not_mutate_original_input() -> None:
    record = _make_record(event_type="audit", agency_or_court="Agency A")
    result = resolve_construction_graph_identities(record)

    result["identities"][0]["canonical_name"] = "Mutated"
    result["source_event_id"] = "mutated"

    assert record["event_id"] == "evt-1"
    assert record["company_name"] == "Acme Builders"


def test_repeated_runs_deterministic() -> None:
    record = _make_record(event_type="litigation", agency_or_court="Court A")

    first = resolve_construction_graph_identities(record)
    second = resolve_construction_graph_identities(record)
    first_batch = resolve_construction_graph_identity_batch([record])
    second_batch = resolve_construction_graph_identity_batch([record])

    assert first == second
    assert first_batch == second_batch
