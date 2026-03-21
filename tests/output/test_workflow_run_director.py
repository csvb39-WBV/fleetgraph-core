from pathlib import Path
import copy
import sys


ROOT = Path(__file__).resolve().parents[2]
SRC = ROOT / "src"

if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

import fleetgraph_core.output.workflow_run_director as workflow_module  # noqa: E402
from fleetgraph_core.output.workflow_run_director import (  # noqa: E402
    STAGE_SEQUENCE,
    WORKFLOW_RUN_STATE,
    apply_workflow_run_director,
    validate_workflow_run_inputs,
)


def sample_fg5_mb1_record(
    canonical_id: str = "can_1",
    key: str = "key1",
    name: str = "Org A",
    domain: str = "a.com",
    source: str = "src1",
    rank: int = 1,
) -> dict:
    return {
        "canonical_organization_id": canonical_id,
        "organization_domain_candidate_id": "dom_" + canonical_id,
        "organization_candidate_id": "org_" + canonical_id,
        "candidate_id": "cand_" + canonical_id,
        "seed_id": "seed1",
        "source_id": source,
        "source_label": "Label",
        "canonical_organization_name": name,
        "canonical_organization_key": key,
        "domain_candidate": domain,
        "candidate_state": "canonicalized",
        "relevance_gate_outcome": "relevant",
        "opportunity_rank": rank,
        "contact_enrichment_request_id": "enrichrequest:" + key + ":" + source + ":" + str(rank),
        "contact_enrichment_request": {
            "request_type": "contact_enrichment",
            "canonical_organization_id": canonical_id,
            "canonical_organization_key": key,
            "canonical_organization_name": name,
            "domain_candidate": domain,
            "source_id": source,
            "opportunity_rank": rank,
        },
        "contact_coordination_state": "prepared",
    }


def install_stub_pipeline(monkeypatch, call_log: list[str] | None = None) -> None:
    def stage1(records: list[dict]) -> list[dict]:
        if call_log is not None:
            call_log.append("apply_matched_contact_assembler")
        output = []
        for record in records:
            item = copy.deepcopy(record)
            item["matched_contact"] = {
                "contact_name": "Alice Example",
                "contact_title": "Head of Procurement",
                "contact_email": "alice@" + item["domain_candidate"],
                "contact_source": item["source_id"],
                "contact_match_state": "matched",
            }
            item["contact_assembly_state"] = "assembled"
            output.append(item)
        return output

    def stage2(records: list[dict]) -> list[dict]:
        if call_log is not None:
            call_log.append("apply_lead_record_assembler")
        output = []
        for record in records:
            item = copy.deepcopy(record)
            item["lead_record_id"] = (
                "leadrecord:"
                + item["canonical_organization_key"]
                + ":"
                + item["source_id"]
                + ":"
                + str(item["opportunity_rank"])
            )
            item["lead_record"] = {
                "canonical_organization_id": item["canonical_organization_id"],
                "canonical_organization_key": item["canonical_organization_key"],
                "canonical_organization_name": item["canonical_organization_name"],
                "organization_domain_candidate_id": item["organization_domain_candidate_id"],
                "organization_candidate_id": item["organization_candidate_id"],
                "candidate_id": item["candidate_id"],
                "seed_id": item["seed_id"],
                "source_id": item["source_id"],
                "source_label": item["source_label"],
                "domain_candidate": item["domain_candidate"],
                "opportunity_rank": item["opportunity_rank"],
                "contact_name": item["matched_contact"]["contact_name"],
                "contact_title": item["matched_contact"]["contact_title"],
                "contact_email": item["matched_contact"]["contact_email"],
                "contact_source": item["matched_contact"]["contact_source"],
                "contact_match_state": item["matched_contact"]["contact_match_state"],
            }
            item["lead_record_state"] = "assembled"
            output.append(item)
        return output

    def stage3(records: list[dict]) -> list[dict]:
        if call_log is not None:
            call_log.append("apply_flatfile_delivery_writer")
        output = []
        for record in records:
            item = copy.deepcopy(record)
            item["delivery_row_id"] = (
                "deliveryrow:"
                + item["canonical_organization_key"]
                + ":"
                + item["source_id"]
                + ":"
                + str(item["opportunity_rank"])
            )
            item["delivery_row"] = {
                "lead_record_id": item["lead_record_id"],
                "canonical_organization_id": item["canonical_organization_id"],
                "canonical_organization_key": item["canonical_organization_key"],
                "canonical_organization_name": item["canonical_organization_name"],
                "organization_domain_candidate_id": item["organization_domain_candidate_id"],
                "organization_candidate_id": item["organization_candidate_id"],
                "candidate_id": item["candidate_id"],
                "seed_id": item["seed_id"],
                "source_id": item["source_id"],
                "source_label": item["source_label"],
                "domain_candidate": item["domain_candidate"],
                "opportunity_rank": item["opportunity_rank"],
                "contact_name": item["lead_record"]["contact_name"],
                "contact_title": item["lead_record"]["contact_title"],
                "contact_email": item["lead_record"]["contact_email"],
                "contact_source": item["lead_record"]["contact_source"],
                "contact_match_state": item["lead_record"]["contact_match_state"],
            }
            item["delivery_row_state"] = "flatfile_ready"
            output.append(item)
        return output

    def stage4(records: list[dict]) -> list[dict]:
        if call_log is not None:
            call_log.append("apply_crm_push_gateway")
        output = []
        for record in records:
            output.append(
                {
                    "crm_payload_id": (
                        "crmpayload:"
                        + record["canonical_organization_key"]
                        + ":"
                        + record["source_id"]
                        + ":"
                        + str(record["opportunity_rank"])
                    ),
                    "crm_payload": {
                        "company_name": record["canonical_organization_name"],
                        "internal_org_key": record["canonical_organization_key"],
                        "fleet_opportunity_rank": record["opportunity_rank"],
                    },
                    "crm_payload_state": "gateway_ready",
                    "delivery_row_id": record["delivery_row_id"],
                }
            )
        return output

    monkeypatch.setattr(workflow_module, "apply_matched_contact_assembler", stage1)
    monkeypatch.setattr(workflow_module, "apply_lead_record_assembler", stage2)
    monkeypatch.setattr(workflow_module, "apply_flatfile_delivery_writer", stage3)
    monkeypatch.setattr(workflow_module, "apply_crm_push_gateway", stage4)
    monkeypatch.setattr(
        workflow_module,
        "_STAGE_DEFINITIONS",
        (
            ("apply_matched_contact_assembler", stage1, "contact_assembly_state"),
            ("apply_lead_record_assembler", stage2, "lead_record_state"),
            ("apply_flatfile_delivery_writer", stage3, "delivery_row_state"),
            ("apply_crm_push_gateway", stage4, "crm_payload_state"),
        ),
    )


# ---------------------------------------------------------------------------
# Validation tests
# ---------------------------------------------------------------------------

def test_validate_workflow_run_inputs_accepts_valid_input() -> None:
    validate_workflow_run_inputs(
        [
            sample_fg5_mb1_record("can_1", "key1", rank=1),
            sample_fg5_mb1_record("can_2", "key2", rank=2),
        ]
    )


def test_validate_workflow_run_inputs_rejects_non_list_input() -> None:
    try:
        validate_workflow_run_inputs("invalid")
    except TypeError as error:
        assert str(error) == "records must be a list"
    else:
        raise AssertionError("TypeError was not raised for non-list input")


def test_validate_workflow_run_inputs_rejects_non_dict_record() -> None:
    try:
        validate_workflow_run_inputs(["invalid"])
    except TypeError as error:
        assert str(error) == "each record must be a dictionary"
    else:
        raise AssertionError("TypeError was not raised for non-dict record")


def test_validate_workflow_run_inputs_rejects_invalid_fg5_structure() -> None:
    record = sample_fg5_mb1_record()
    del record["contact_coordination_state"]

    try:
        validate_workflow_run_inputs([record])
    except ValueError as error:
        assert str(error) == "record is missing required fields: contact_coordination_state"
    else:
        raise AssertionError("ValueError was not raised for invalid FG5-MB1 structure")


# ---------------------------------------------------------------------------
# Execution and output contract tests
# ---------------------------------------------------------------------------

def test_apply_workflow_run_director_correct_stage_order(monkeypatch) -> None:
    calls: list[str] = []
    install_stub_pipeline(monkeypatch, calls)

    apply_workflow_run_director([sample_fg5_mb1_record()])

    assert calls == list(STAGE_SEQUENCE)


def test_apply_workflow_run_director_full_pipeline_execution(monkeypatch) -> None:
    install_stub_pipeline(monkeypatch)

    result = apply_workflow_run_director(
        [sample_fg5_mb1_record("can_1", "key1", rank=1), sample_fg5_mb1_record("can_2", "key2", rank=2)]
    )

    assert result["workflow_run_state"] == WORKFLOW_RUN_STATE
    assert len(result["final_results"]) == 2


def test_apply_workflow_run_director_result_has_exact_top_level_fields(monkeypatch) -> None:
    install_stub_pipeline(monkeypatch)

    result = apply_workflow_run_director([sample_fg5_mb1_record()])

    assert set(result.keys()) == {
        "workflow_run_id",
        "workflow_run_state",
        "stage_sequence",
        "stage_results",
        "final_results",
        "input_record_count",
        "output_record_count",
    }


def test_apply_workflow_run_director_stage_sequence_is_fixed(monkeypatch) -> None:
    install_stub_pipeline(monkeypatch)

    result = apply_workflow_run_director([sample_fg5_mb1_record()])

    assert result["stage_sequence"] == list(STAGE_SEQUENCE)


def test_apply_workflow_run_director_counts_are_exact_and_consistent(monkeypatch) -> None:
    install_stub_pipeline(monkeypatch)

    records = [
        sample_fg5_mb1_record("can_1", "key1", rank=1),
        sample_fg5_mb1_record("can_2", "key2", rank=2),
        sample_fg5_mb1_record("can_3", "key3", rank=3),
    ]

    result = apply_workflow_run_director(records)

    assert result["input_record_count"] == 3
    assert result["output_record_count"] == 3
    assert len(result["final_results"]) == 3
    assert len(result["stage_results"]) == 4

    for stage_result in result["stage_results"]:
        assert stage_result["input_count"] == 3
        assert stage_result["output_count"] == 3
        assert stage_result["stage_outcome"] == "success"


def test_apply_workflow_run_director_workflow_run_id_exact_readable_format(monkeypatch) -> None:
    install_stub_pipeline(monkeypatch)

    records = [
        sample_fg5_mb1_record("can_1", "keyA", source="src1", rank=2),
        sample_fg5_mb1_record("can_2", "keyB", source="src2", rank=7),
    ]

    result = apply_workflow_run_director(records)

    assert result["workflow_run_id"] == "workflowrun:2:keyA:src1:2:keyB:src2:7"


def test_apply_workflow_run_director_empty_input_id_is_locked_value(monkeypatch) -> None:
    install_stub_pipeline(monkeypatch)

    result = apply_workflow_run_director([])

    assert result["workflow_run_id"] == "workflowrun:0:empty:empty"


def test_apply_workflow_run_director_stage_results_include_output_state_field(monkeypatch) -> None:
    install_stub_pipeline(monkeypatch)

    result = apply_workflow_run_director([sample_fg5_mb1_record()])

    expected = {
        "apply_matched_contact_assembler": "contact_assembly_state",
        "apply_lead_record_assembler": "lead_record_state",
        "apply_flatfile_delivery_writer": "delivery_row_state",
        "apply_crm_push_gateway": "crm_payload_state",
    }

    for stage_result in result["stage_results"]:
        stage_name = stage_result["stage_name"]
        assert stage_result["output_state_field"] == expected[stage_name]


def test_apply_workflow_run_director_repeated_runs_are_identical(monkeypatch) -> None:
    install_stub_pipeline(monkeypatch)

    records = [
        sample_fg5_mb1_record("can_1", "key1", rank=1),
        sample_fg5_mb1_record("can_2", "key2", rank=2),
    ]

    first = apply_workflow_run_director(records)
    second = apply_workflow_run_director(records)

    assert first == second


# ---------------------------------------------------------------------------
# Edge behavior tests
# ---------------------------------------------------------------------------

def test_apply_workflow_run_director_accepts_empty_input(monkeypatch) -> None:
    install_stub_pipeline(monkeypatch)

    result = apply_workflow_run_director([])

    assert result["workflow_run_state"] == "completed"
    assert result["final_results"] == []
    assert result["input_record_count"] == 0
    assert result["output_record_count"] == 0


def test_apply_workflow_run_director_is_non_mutating(monkeypatch) -> None:
    install_stub_pipeline(monkeypatch)

    records = [
        sample_fg5_mb1_record("can_1", "key1", rank=1),
        sample_fg5_mb1_record("can_2", "key2", rank=2),
    ]
    original = copy.deepcopy(records)

    apply_workflow_run_director(records)

    assert records == original


def test_apply_workflow_run_director_stage_failure_propagates(monkeypatch) -> None:
    install_stub_pipeline(monkeypatch)

    def broken_stage(records: list[dict]) -> list[dict]:
        raise ValueError("stage boom")

    monkeypatch.setattr(workflow_module, "apply_lead_record_assembler", broken_stage)
    monkeypatch.setattr(
        workflow_module,
        "_STAGE_DEFINITIONS",
        (
            (
                "apply_matched_contact_assembler",
                workflow_module.apply_matched_contact_assembler,
                "contact_assembly_state",
            ),
            ("apply_lead_record_assembler", broken_stage, "lead_record_state"),
            (
                "apply_flatfile_delivery_writer",
                workflow_module.apply_flatfile_delivery_writer,
                "delivery_row_state",
            ),
            ("apply_crm_push_gateway", workflow_module.apply_crm_push_gateway, "crm_payload_state"),
        ),
    )

    try:
        apply_workflow_run_director([sample_fg5_mb1_record()])
    except ValueError as error:
        assert str(error) == "stage boom"
    else:
        raise AssertionError("stage failure did not propagate")
