from typing import Any, Dict, List

from src.fleetgraph_core.graph_construction.shared_domain_link_resolver import (
    assemble_shared_domain_links,
    validate_unified_domain_records,
)
from src.fleetgraph_core.graph_construction.shared_domain_link_aggregator import (
    assemble_shared_domain_aggregates,
)
from src.fleetgraph_core.graph_construction.relationship_signal_extractor import (
    assemble_relationship_signals,
)
from src.fleetgraph_core.output.relationship_signal_formatter import (
    assemble_formatted_relationship_signals,
)
from src.fleetgraph_core.output.relationship_signal_output_writer import (
    write_relationship_signal_output,
)
from src.fleetgraph_core.api.relationship_signal_api_reader import (
    get_relationship_signal_output_summary,
    load_relationship_signal_output,
)


def validate_pipeline_input_records(records: List[Dict[str, Any]]) -> None:
    validate_unified_domain_records(records)


def write_relationship_signal_pipeline_output(
    formatted_records: List[Dict[str, Any]],
    output_path: str,
) -> str:
    return write_relationship_signal_output(formatted_records, output_path)


def build_pipeline_summary(output_path: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    summary = get_relationship_signal_output_summary(payload)
    return {
        "output_path": output_path,
        "output_type": summary["output_type"],
        "output_schema_version": summary["output_schema_version"],
        "record_count": summary["record_count"],
    }


def run_relationship_signal_pipeline(
    records: List[Dict[str, Any]],
    output_path: str = "relationship_signals_output.json",
) -> Dict[str, Any]:
    validate_pipeline_input_records(records)
    link_records = assemble_shared_domain_links(records)
    aggregate_records = assemble_shared_domain_aggregates(link_records)
    signal_records = assemble_relationship_signals(aggregate_records)
    formatted_records = assemble_formatted_relationship_signals(signal_records)
    written_path = write_relationship_signal_pipeline_output(formatted_records, output_path)
    payload = load_relationship_signal_output(written_path)
    summary = build_pipeline_summary(written_path, payload)
    return summary
