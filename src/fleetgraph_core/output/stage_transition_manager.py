"""FG7-MB2 deterministic stage transition manager."""

from typing import Any


WORKFLOW_STAGES = (
    "matched_contact_assembler",
    "lead_record_assembler",
    "flatfile_delivery_writer",
    "crm_push_gateway",
    "workflow_completed",
)

TERMINAL_STAGE = "workflow_completed"

ALLOWED_TRANSITIONS: dict[str | None, str] = {
    None: "matched_contact_assembler",
    "matched_contact_assembler": "lead_record_assembler",
    "lead_record_assembler": "flatfile_delivery_writer",
    "flatfile_delivery_writer": "crm_push_gateway",
    "crm_push_gateway": "workflow_completed",
}


def validate_stage_transition(current_stage: str | None, next_stage: str) -> None:
    """Validate a requested workflow stage transition."""
    if current_stage is not None and not isinstance(current_stage, str):
        raise TypeError("current_stage must be None or a string")

    if not isinstance(next_stage, str):
        raise TypeError("next_stage must be a non-empty string")

    if isinstance(current_stage, str) and current_stage.strip() == "":
        raise ValueError("stage names must be non-empty strings")

    if next_stage.strip() == "":
        raise ValueError("stage names must be non-empty strings")

    if current_stage is not None and current_stage not in WORKFLOW_STAGES:
        raise ValueError(f"unknown stage: {current_stage}")

    if next_stage not in WORKFLOW_STAGES:
        raise ValueError(f"unknown stage: {next_stage}")

    if current_stage == TERMINAL_STAGE:
        raise ValueError("workflow_completed is terminal and cannot transition further")

    expected_next_stage = ALLOWED_TRANSITIONS.get(current_stage)

    if current_stage is None and next_stage != expected_next_stage:
        raise ValueError("invalid transition: start must go to matched_contact_assembler")

    if current_stage is not None and next_stage != expected_next_stage:
        raise ValueError(f"invalid transition: {current_stage} -> {next_stage}")


def _build_transition_id(current_stage: str | None, next_stage: str) -> str:
    if current_stage is None:
        return "transition:start->" + next_stage

    return "transition:" + current_stage + "->" + next_stage


def apply_stage_transition(current_stage: str | None, next_stage: str) -> dict[str, Any]:
    """Apply a validated transition and return deterministic transition metadata."""
    validate_stage_transition(current_stage, next_stage)

    return {
        "from_stage": current_stage,
        "to_stage": next_stage,
        "transition_state": "allowed",
        "is_terminal": next_stage == TERMINAL_STAGE,
        "transition_id": _build_transition_id(current_stage, next_stage),
    }
