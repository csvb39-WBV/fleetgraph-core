import pytest
from unittest.mock import patch

from src.fleetgraph_core.pipeline.relationship_signal_pipeline_runner import (
    validate_pipeline_input_records,
    write_relationship_signal_pipeline_output,
    build_pipeline_summary,
    run_relationship_signal_pipeline,
)


_RUNNER_MODULE = "src.fleetgraph_core.pipeline.relationship_signal_pipeline_runner"


def _make_domain_node_unified_record(
    candidate_state: str = "domain_node_unified",
    organization_node_ids=None,
):
    if organization_node_ids is None:
        organization_node_ids = ["org-node-1", "org-node-2"]
    return {
        "unified_domain_id": "unified-domain-1",
        "domain_node_ids": ["domain-node-1"],
        "domain_node_id": "domain-node-1",
        "domain_node_type": "domain",
        "domain_node_label": "example.com",
        "domain_candidate": "example.com",
        "domain_classification": "corporate",
        "edge_tos": ["edge-1"],
        "source_ids": ["source-1"],
        "organization_node_ids": organization_node_ids,
        "unified_organization_ids": ["unified-org-1"],
        "candidate_state": candidate_state,
    }


def _make_payload(record_count: int = 1):
    return {
        "output_type": "relationship_signal_output",
        "output_schema_version": "1.0",
        "record_count": record_count,
        "records": [],
    }


# --- validate_pipeline_input_records ---

def test_validate_pipeline_input_records_accepts_valid() -> None:
    records = [_make_domain_node_unified_record()]
    validate_pipeline_input_records(records)


def test_validate_pipeline_input_records_rejects_empty_list() -> None:
    with pytest.raises(ValueError):
        validate_pipeline_input_records([])


def test_validate_pipeline_input_records_rejects_wrong_candidate_state() -> None:
    records = [_make_domain_node_unified_record(candidate_state="wrong_state")]
    with pytest.raises(ValueError):
        validate_pipeline_input_records(records)


# --- write_relationship_signal_pipeline_output ---

def test_write_pipeline_output_delegates_to_writer() -> None:
    formatted_records = [{"candidate_state": "relationship_signal_formatted"}]
    output_path = "test_output.json"

    with patch(f"{_RUNNER_MODULE}.write_relationship_signal_output") as mock_writer:
        mock_writer.return_value = output_path
        result = write_relationship_signal_pipeline_output(formatted_records, output_path)

    mock_writer.assert_called_once_with(formatted_records, output_path)
    assert result == output_path


# --- build_pipeline_summary ---

def test_build_pipeline_summary_exact_keys() -> None:
    payload = _make_payload(record_count=3)
    summary = build_pipeline_summary("output.json", payload)
    assert set(summary.keys()) == {"output_path", "output_type", "output_schema_version", "record_count"}


def test_build_pipeline_summary_values() -> None:
    payload = _make_payload(record_count=5)
    summary = build_pipeline_summary("results/output.json", payload)
    assert summary["output_path"] == "results/output.json"
    assert summary["output_type"] == "relationship_signal_output"
    assert summary["output_schema_version"] == "1.0"
    assert summary["record_count"] == 5


# --- run_relationship_signal_pipeline ---

def test_run_pipeline_calls_stages_in_order() -> None:
    records = [_make_domain_node_unified_record()]
    payload = _make_payload(record_count=1)

    with patch(f"{_RUNNER_MODULE}.validate_unified_domain_records") as mock_validate, \
         patch(f"{_RUNNER_MODULE}.assemble_shared_domain_links", return_value=["link"]) as mock_links, \
         patch(f"{_RUNNER_MODULE}.assemble_shared_domain_aggregates", return_value=["agg"]) as mock_agg, \
         patch(f"{_RUNNER_MODULE}.assemble_relationship_signals", return_value=["sig"]) as mock_sig, \
         patch(f"{_RUNNER_MODULE}.assemble_formatted_relationship_signals", return_value=["fmt"]) as mock_fmt, \
         patch(f"{_RUNNER_MODULE}.write_relationship_signal_output", return_value="out.json") as mock_write, \
         patch(f"{_RUNNER_MODULE}.load_relationship_signal_output", return_value=payload) as mock_load, \
         patch(f"{_RUNNER_MODULE}.get_relationship_signal_output_summary", return_value={"output_type": "relationship_signal_output", "output_schema_version": "1.0", "record_count": 1}) as mock_summary:

        run_relationship_signal_pipeline(records, "out.json")

    mock_validate.assert_called_once_with(records)
    mock_links.assert_called_once_with(records)
    mock_agg.assert_called_once_with(["link"])
    mock_sig.assert_called_once_with(["agg"])
    mock_fmt.assert_called_once_with(["sig"])
    mock_write.assert_called_once_with(["fmt"], "out.json")
    mock_load.assert_called_once_with("out.json")
    mock_summary.assert_called_once_with(payload)


def test_run_pipeline_returns_summary() -> None:
    records = [_make_domain_node_unified_record()]
    payload = _make_payload(record_count=2)

    with patch(f"{_RUNNER_MODULE}.validate_unified_domain_records"), \
         patch(f"{_RUNNER_MODULE}.assemble_shared_domain_links", return_value=[]), \
         patch(f"{_RUNNER_MODULE}.assemble_shared_domain_aggregates", return_value=[]), \
         patch(f"{_RUNNER_MODULE}.assemble_relationship_signals", return_value=[]), \
         patch(f"{_RUNNER_MODULE}.assemble_formatted_relationship_signals", return_value=[]), \
         patch(f"{_RUNNER_MODULE}.write_relationship_signal_output", return_value="out.json"), \
         patch(f"{_RUNNER_MODULE}.load_relationship_signal_output", return_value=payload), \
         patch(f"{_RUNNER_MODULE}.get_relationship_signal_output_summary", return_value={"output_type": "relationship_signal_output", "output_schema_version": "1.0", "record_count": 2}):

        summary = run_relationship_signal_pipeline(records, "out.json")

    assert set(summary.keys()) == {"output_path", "output_type", "output_schema_version", "record_count"}
    assert summary["output_path"] == "out.json"
    assert summary["output_type"] == "relationship_signal_output"
    assert summary["output_schema_version"] == "1.0"
    assert summary["record_count"] == 2


def test_run_pipeline_default_output_path() -> None:
    records = [_make_domain_node_unified_record()]
    payload = _make_payload(record_count=1)

    with patch(f"{_RUNNER_MODULE}.validate_unified_domain_records"), \
         patch(f"{_RUNNER_MODULE}.assemble_shared_domain_links", return_value=[]), \
         patch(f"{_RUNNER_MODULE}.assemble_shared_domain_aggregates", return_value=[]), \
         patch(f"{_RUNNER_MODULE}.assemble_relationship_signals", return_value=[]), \
         patch(f"{_RUNNER_MODULE}.assemble_formatted_relationship_signals", return_value=[]), \
         patch(f"{_RUNNER_MODULE}.write_relationship_signal_output", return_value="relationship_signals_output.json") as mock_write, \
         patch(f"{_RUNNER_MODULE}.load_relationship_signal_output", return_value=payload), \
         patch(f"{_RUNNER_MODULE}.get_relationship_signal_output_summary", return_value={"output_type": "relationship_signal_output", "output_schema_version": "1.0", "record_count": 1}):

        run_relationship_signal_pipeline(records)

    mock_write.assert_called_once_with([], "relationship_signals_output.json")


def test_run_pipeline_custom_output_path() -> None:
    records = [_make_domain_node_unified_record()]
    payload = _make_payload(record_count=1)
    custom_path = "custom/path/output.json"

    with patch(f"{_RUNNER_MODULE}.validate_unified_domain_records"), \
         patch(f"{_RUNNER_MODULE}.assemble_shared_domain_links", return_value=[]), \
         patch(f"{_RUNNER_MODULE}.assemble_shared_domain_aggregates", return_value=[]), \
         patch(f"{_RUNNER_MODULE}.assemble_relationship_signals", return_value=[]), \
         patch(f"{_RUNNER_MODULE}.assemble_formatted_relationship_signals", return_value=[]), \
         patch(f"{_RUNNER_MODULE}.write_relationship_signal_output", return_value=custom_path) as mock_write, \
         patch(f"{_RUNNER_MODULE}.load_relationship_signal_output", return_value=payload), \
         patch(f"{_RUNNER_MODULE}.get_relationship_signal_output_summary", return_value={"output_type": "relationship_signal_output", "output_schema_version": "1.0", "record_count": 1}):

        run_relationship_signal_pipeline(records, custom_path)

    mock_write.assert_called_once_with([], custom_path)


def test_run_pipeline_propagates_value_error() -> None:
    records = [_make_domain_node_unified_record(candidate_state="wrong")]
    with pytest.raises(ValueError):
        run_relationship_signal_pipeline(records)


def test_run_pipeline_propagates_os_error() -> None:
    records = [_make_domain_node_unified_record()]

    with patch(f"{_RUNNER_MODULE}.validate_unified_domain_records"), \
         patch(f"{_RUNNER_MODULE}.assemble_shared_domain_links", return_value=[]), \
         patch(f"{_RUNNER_MODULE}.assemble_shared_domain_aggregates", return_value=[]), \
         patch(f"{_RUNNER_MODULE}.assemble_relationship_signals", return_value=[]), \
         patch(f"{_RUNNER_MODULE}.assemble_formatted_relationship_signals", return_value=[]), \
         patch(f"{_RUNNER_MODULE}.write_relationship_signal_output", side_effect=OSError("disk full")):

        with pytest.raises(OSError):
            run_relationship_signal_pipeline(records)
