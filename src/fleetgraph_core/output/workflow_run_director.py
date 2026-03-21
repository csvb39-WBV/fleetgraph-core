"""FG7-MB1 deterministic workflow run director."""

from copy import deepcopy
from typing import Any, Callable

from fleetgraph_core.enrichment.matched_contact_assembler import (
    apply_matched_contact_assembler,
    validate_fg5_mb1_records,
)
from fleetgraph_core.output.crm_push_gateway import apply_crm_push_gateway
from fleetgraph_core.output.flatfile_delivery_writer import apply_flatfile_delivery_writer
from fleetgraph_core.output.lead_record_assembler import apply_lead_record_assembler


WORKFLOW_RUN_STATE = "completed"
STAGE_SEQUENCE = (
    "apply_matched_contact_assembler",
    "apply_lead_record_assembler",
    "apply_flatfile_delivery_writer",
    "apply_crm_push_gateway",
)

_STAGE_DEFINITIONS: tuple[tuple[str, Callable[[list[dict[str, Any]]], list[dict[str, Any]]], str], ...] = (
    ("apply_matched_contact_assembler", apply_matched_contact_assembler, "contact_assembly_state"),
    ("apply_lead_record_assembler", apply_lead_record_assembler, "lead_record_state"),
    ("apply_flatfile_delivery_writer", apply_flatfile_delivery_writer, "delivery_row_state"),
    ("apply_crm_push_gateway", apply_crm_push_gateway, "crm_payload_state"),
)


def validate_workflow_run_inputs(records: list[dict[str, Any]]) -> None:
    """Validate exact FG5-MB1 records for workflow execution input."""
    validate_fg5_mb1_records(records)


def _build_workflow_run_id(records: list[dict[str, Any]]) -> str:
    if not records:
        return "workflowrun:0:empty:empty"

    first = records[0]
    last = records[-1]

    first_key = (
        first["canonical_organization_key"]
        + ":"
        + first["source_id"]
        + ":"
        + str(first["opportunity_rank"])
    )
    last_key = (
        last["canonical_organization_key"]
        + ":"
        + last["source_id"]
        + ":"
        + str(last["opportunity_rank"])
    )

    return "workflowrun:" + str(len(records)) + ":" + first_key + ":" + last_key


def apply_workflow_run_director(records: list[dict[str, Any]]) -> dict[str, Any]:
    """Execute the locked stage sequence and return a deterministic run summary."""
    validate_workflow_run_inputs(records)

    workflow_run_id = _build_workflow_run_id(records)

    stage_results: list[dict[str, Any]] = []
    stage_input: list[dict[str, Any]] = deepcopy(records)

    for stage_name, stage_func, output_state_field in _STAGE_DEFINITIONS:
        stage_input_count = len(stage_input)
        stage_output = stage_func(stage_input)

        stage_results.append(
            {
                "stage_name": stage_name,
                "input_count": stage_input_count,
                "output_count": len(stage_output),
                "stage_outcome": "success",
                "output_state_field": output_state_field,
            }
        )

        stage_input = stage_output

    final_results = stage_input

    return {
        "workflow_run_id": workflow_run_id,
        "workflow_run_state": WORKFLOW_RUN_STATE,
        "stage_sequence": list(STAGE_SEQUENCE),
        "stage_results": stage_results,
        "final_results": final_results,
        "input_record_count": len(records),
        "output_record_count": len(final_results),
    }
