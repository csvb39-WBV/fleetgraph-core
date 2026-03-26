from __future__ import annotations

import copy
import json
import pathlib
import sys

import pytest


PROJECT_ROOT = pathlib.Path(__file__).resolve().parents[2]
SRC_ROOT = PROJECT_ROOT / "src"

if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))


import fleetgraph_core.export.csv_exporter as csv_exporter_module
from fleetgraph_core.export.csv_exporter import export_csv_output


def test_payload_must_contain_results_when_payload_is_not_dict(
) -> None:
    with pytest.raises(ValueError, match=r"^payload must contain results\.$"):
        export_csv_output("bad-payload", "out")


def test_payload_must_contain_results_when_results_missing(tmp_path: pathlib.Path) -> None:
    with pytest.raises(ValueError, match=r"^payload must contain results\.$"):
        export_csv_output({"request_id": "REQ-001"}, str(tmp_path))


def test_payload_must_contain_results_when_results_not_list(tmp_path: pathlib.Path) -> None:
    with pytest.raises(ValueError, match=r"^payload must contain results\.$"):
        export_csv_output({"results": "bad"}, str(tmp_path))


def test_uses_request_id_filename_when_present(tmp_path: pathlib.Path) -> None:
    result = export_csv_output(
        {"request_id": "REQ-001", "results": []},
        str(tmp_path),
    )

    assert result["filename"] == "output_REQ-001.csv"


def test_uses_fallback_filename_when_request_id_missing(tmp_path: pathlib.Path) -> None:
    result = export_csv_output(
        {"results": []},
        str(tmp_path),
    )

    assert result["filename"] == "output_export.csv"


def test_creates_directory_if_missing(tmp_path: pathlib.Path) -> None:
    output_dir = tmp_path / "nested" / "exports"

    export_csv_output({"request_id": "REQ-001", "results": []}, str(output_dir))

    assert output_dir.exists()


def test_writes_exact_header_and_rows_in_order(tmp_path: pathlib.Path) -> None:
    payload = {
        "request_id": "REQ-001",
        "results": [
            {
                "company_name": "Alpha Co",
                "signal_type": "litigation_risk",
                "priority_score": 45,
                "source_event_id": "EVT-001",
                "event_type": "litigation",
            },
            {
                "company_name": "Beta Co",
                "signal_type": "audit_risk",
                "priority_score": 35,
                "source_event_id": "EVT-002",
                "event_type": "audit",
            },
        ],
    }

    result = export_csv_output(payload, str(tmp_path))

    assert pathlib.Path(result["path"]).read_text(encoding="utf-8") == (
        "company,signal,priority,event_id,type\n"
        "Alpha Co,litigation_risk,45,EVT-001,litigation\n"
        "Beta Co,audit_risk,35,EVT-002,audit\n"
    )


def test_missing_fields_become_empty_strings(tmp_path: pathlib.Path) -> None:
    payload = {
        "results": [
            {
                "company_name": "Alpha Co",
                "source_event_id": "EVT-001",
            }
        ]
    }

    result = export_csv_output(payload, str(tmp_path))

    assert pathlib.Path(result["path"]).read_text(encoding="utf-8") == (
        "company,signal,priority,event_id,type\n"
        "Alpha Co,,,EVT-001,\n"
    )


def test_non_dict_result_item_becomes_empty_row(tmp_path: pathlib.Path) -> None:
    result = export_csv_output({"results": ["bad-item"]}, str(tmp_path))

    assert pathlib.Path(result["path"]).read_text(encoding="utf-8") == (
        "company,signal,priority,event_id,type\n"
        ",,,,\n"
    )


def test_return_envelope_exact_shape(tmp_path: pathlib.Path) -> None:
    result = export_csv_output({"results": []}, str(tmp_path))

    assert result == {
        "ok": True,
        "path": str(tmp_path / "output_export.csv"),
        "filename": "output_export.csv",
    }


def test_file_ends_with_newline(tmp_path: pathlib.Path) -> None:
    result = export_csv_output({"results": []}, str(tmp_path))

    assert pathlib.Path(result["path"]).read_text(encoding="utf-8").endswith("\n")


def test_repeated_exports_with_same_input_produce_same_file_contents(
    tmp_path: pathlib.Path,
) -> None:
    payload = {
        "request_id": "REQ-001",
        "results": [{"company_name": "Alpha Co"}],
    }

    first = export_csv_output(payload, str(tmp_path))
    first_contents = pathlib.Path(first["path"]).read_text(encoding="utf-8")
    second = export_csv_output(payload, str(tmp_path))
    second_contents = pathlib.Path(second["path"]).read_text(encoding="utf-8")

    assert first == second
    assert first_contents == second_contents


def test_payload_remains_unchanged(tmp_path: pathlib.Path) -> None:
    payload = {
        "request_id": "REQ-001",
        "results": [{"company_name": "Alpha Co"}],
    }
    snapshot = copy.deepcopy(payload)

    export_csv_output(payload, str(tmp_path))

    assert payload == snapshot


def test_module_entrypoint_reads_input_json_and_writes_csv(
    tmp_path: pathlib.Path,
) -> None:
    input_path = tmp_path / "input.json"
    output_dir = tmp_path / "exports"
    input_path.write_text(
        json.dumps(
            {
                "request_id": "REQ-001",
                "results": [
                    {
                        "company_name": "Alpha Co",
                        "signal_type": "litigation_risk",
                        "priority_score": 45,
                        "source_event_id": "EVT-001",
                        "event_type": "litigation",
                    }
                ],
            }
        ),
        encoding="utf-8",
    )

    exit_code = csv_exporter_module.main([str(input_path), str(output_dir)])

    assert exit_code == 0
    assert (output_dir / "output_REQ-001.csv").read_text(encoding="utf-8") == (
        "company,signal,priority,event_id,type\n"
        "Alpha Co,litigation_risk,45,EVT-001,litigation\n"
    )


def test_module_entrypoint_propagates_missing_file_error(
    tmp_path: pathlib.Path,
) -> None:
    with pytest.raises(FileNotFoundError):
        csv_exporter_module.main(
            [str(tmp_path / "missing.json"), str(tmp_path / "exports")]
        )


def test_module_entrypoint_propagates_invalid_json_error(
    tmp_path: pathlib.Path,
) -> None:
    input_path = tmp_path / "invalid.json"
    input_path.write_text("{bad json", encoding="utf-8")

    with pytest.raises(json.JSONDecodeError):
        csv_exporter_module.main([str(input_path), str(tmp_path / "exports")])
