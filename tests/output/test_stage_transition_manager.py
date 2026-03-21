from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parents[2]
SRC = ROOT / "src"

if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from fleetgraph_core.output.stage_transition_manager import (  # noqa: E402
    TERMINAL_STAGE,
    apply_stage_transition,
    validate_stage_transition,
)


def test_validate_stage_transition_accepts_start_to_first_stage() -> None:
    validate_stage_transition(None, "matched_contact_assembler")


def test_validate_stage_transition_accepts_each_legal_intermediate_step() -> None:
    validate_stage_transition("matched_contact_assembler", "lead_record_assembler")
    validate_stage_transition("lead_record_assembler", "flatfile_delivery_writer")
    validate_stage_transition("flatfile_delivery_writer", "crm_push_gateway")


def test_validate_stage_transition_accepts_final_to_completed() -> None:
    validate_stage_transition("crm_push_gateway", "workflow_completed")


def test_validate_stage_transition_rejects_invalid_start_transition() -> None:
    try:
        validate_stage_transition(None, "lead_record_assembler")
    except ValueError as error:
        assert str(error) == "invalid transition: start must go to matched_contact_assembler"
    else:
        raise AssertionError("invalid start transition did not raise ValueError")


def test_validate_stage_transition_rejects_skipped_stage() -> None:
    try:
        validate_stage_transition("matched_contact_assembler", "flatfile_delivery_writer")
    except ValueError as error:
        assert str(error) == "invalid transition: matched_contact_assembler -> flatfile_delivery_writer"
    else:
        raise AssertionError("skipped-stage transition did not raise ValueError")


def test_validate_stage_transition_rejects_repeated_stage() -> None:
    try:
        validate_stage_transition("lead_record_assembler", "lead_record_assembler")
    except ValueError as error:
        assert str(error) == "invalid transition: lead_record_assembler -> lead_record_assembler"
    else:
        raise AssertionError("repeated-stage transition did not raise ValueError")


def test_validate_stage_transition_rejects_unknown_current_stage() -> None:
    try:
        validate_stage_transition("unknown_stage", "lead_record_assembler")
    except ValueError as error:
        assert str(error) == "unknown stage: unknown_stage"
    else:
        raise AssertionError("unknown current stage did not raise ValueError")


def test_validate_stage_transition_rejects_unknown_next_stage() -> None:
    try:
        validate_stage_transition("matched_contact_assembler", "unknown_stage")
    except ValueError as error:
        assert str(error) == "unknown stage: unknown_stage"
    else:
        raise AssertionError("unknown next stage did not raise ValueError")


def test_validate_stage_transition_rejects_transition_after_terminal_state() -> None:
    try:
        validate_stage_transition("workflow_completed", "workflow_completed")
    except ValueError as error:
        assert str(error) == "workflow_completed is terminal and cannot transition further"
    else:
        raise AssertionError("terminal transition did not raise ValueError")


def test_validate_stage_transition_rejects_blank_current_stage() -> None:
    try:
        validate_stage_transition("   ", "matched_contact_assembler")
    except ValueError as error:
        assert str(error) == "stage names must be non-empty strings"
    else:
        raise AssertionError("blank current_stage did not raise ValueError")


def test_validate_stage_transition_rejects_blank_next_stage() -> None:
    try:
        validate_stage_transition("matched_contact_assembler", "   ")
    except ValueError as error:
        assert str(error) == "stage names must be non-empty strings"
    else:
        raise AssertionError("blank next_stage did not raise ValueError")


def test_validate_stage_transition_rejects_wrong_current_stage_type() -> None:
    try:
        validate_stage_transition(123, "matched_contact_assembler")  # type: ignore[arg-type]
    except TypeError as error:
        assert str(error) == "current_stage must be None or a string"
    else:
        raise AssertionError("wrong current_stage type did not raise TypeError")


def test_validate_stage_transition_rejects_wrong_next_stage_type() -> None:
    try:
        validate_stage_transition("matched_contact_assembler", 123)  # type: ignore[arg-type]
    except TypeError as error:
        assert str(error) == "next_stage must be a non-empty string"
    else:
        raise AssertionError("wrong next_stage type did not raise TypeError")


def test_apply_stage_transition_result_has_exact_shape() -> None:
    result = apply_stage_transition(None, "matched_contact_assembler")

    assert set(result.keys()) == {
        "from_stage",
        "to_stage",
        "transition_state",
        "is_terminal",
        "transition_id",
    }


def test_apply_stage_transition_sets_correct_terminal_flag() -> None:
    non_terminal = apply_stage_transition("matched_contact_assembler", "lead_record_assembler")
    terminal = apply_stage_transition("crm_push_gateway", "workflow_completed")

    assert non_terminal["is_terminal"] is False
    assert terminal["is_terminal"] is True


def test_apply_stage_transition_sets_allowed_transition_state() -> None:
    result = apply_stage_transition("lead_record_assembler", "flatfile_delivery_writer")

    assert result["transition_state"] == "allowed"


def test_apply_stage_transition_is_deterministic() -> None:
    first = apply_stage_transition("flatfile_delivery_writer", "crm_push_gateway")
    second = apply_stage_transition("flatfile_delivery_writer", "crm_push_gateway")

    assert first == second


def test_apply_stage_transition_builds_expected_transition_id() -> None:
    start_result = apply_stage_transition(None, "matched_contact_assembler")
    final_result = apply_stage_transition("crm_push_gateway", "workflow_completed")

    assert start_result["transition_id"] == "transition:start->matched_contact_assembler"
    assert final_result["transition_id"] == "transition:crm_push_gateway->workflow_completed"


def test_apply_stage_transition_preserves_input_values() -> None:
    current_stage = "matched_contact_assembler"
    next_stage = "lead_record_assembler"

    apply_stage_transition(current_stage, next_stage)

    assert current_stage == "matched_contact_assembler"
    assert next_stage == "lead_record_assembler"


def test_apply_stage_transition_terminal_constant_matches_expected() -> None:
    assert TERMINAL_STAGE == "workflow_completed"
