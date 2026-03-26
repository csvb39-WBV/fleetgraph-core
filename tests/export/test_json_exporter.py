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


from fleetgraph_core.export.json_exporter import export_json_output


def test_payload_not_dict() -> None:
    with pytest.raises(ValueError, match=r"^payload must be a dictionary\.$"):
        export_json_output("bad-payload", "out")


def test_invalid_output_dir() -> None:
    with pytest.raises(ValueError, match=r"^output_dir must be a non-empty string\.$"):
        export_json_output({"request_id": "REQ-001"}, "")

    with pytest.raises(ValueError, match=r"^output_dir must be a non-empty string\.$"):
        export_json_output({"request_id": "REQ-001"}, "   ")


def test_missing_exportable_identity() -> None:
    with pytest.raises(
        ValueError,
        match=r"^payload must contain an exportable request identity\.$",
    ):
        export_json_output({"other": "value"}, "out")


def test_valid_request_id_path(tmp_path: pathlib.Path) -> None:
    result = export_json_output({"request_id": "REQ-001"}, str(tmp_path))

    assert result == {
        "ok": True,
        "path": str(tmp_path / "output_REQ-001.json"),
        "filename": "output_REQ-001.json",
    }


def test_valid_fallback_response_type_path(tmp_path: pathlib.Path) -> None:
    result = export_json_output({"response_type": "analysis"}, str(tmp_path))

    assert result == {
        "ok": True,
        "path": str(tmp_path / "output_analysis.json"),
        "filename": "output_analysis.json",
    }


def test_creates_directory_if_missing(tmp_path: pathlib.Path) -> None:
    output_dir = tmp_path / "nested" / "exports"

    export_json_output({"request_id": "REQ-001"}, str(output_dir))

    assert output_dir.exists()


def test_writes_exact_payload_to_disk(tmp_path: pathlib.Path) -> None:
    payload = {
        "request_id": "REQ-001",
        "response": {"status": "ok"},
    }

    result = export_json_output(payload, str(tmp_path))
    written = pathlib.Path(result["path"]).read_text(encoding="utf-8")

    assert written == json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=False,
        indent=2,
    ) + "\n"


def test_return_envelope_exact_shape(tmp_path: pathlib.Path) -> None:
    result = export_json_output({"request_id": "REQ-001"}, str(tmp_path))

    assert set(result.keys()) == {"ok", "path", "filename"}


def test_filename_exact(tmp_path: pathlib.Path) -> None:
    result = export_json_output({"request_id": "REQ-001"}, str(tmp_path))

    assert result["filename"] == "output_REQ-001.json"


def test_file_ends_with_newline(tmp_path: pathlib.Path) -> None:
    result = export_json_output({"request_id": "REQ-001"}, str(tmp_path))

    assert pathlib.Path(result["path"]).read_text(encoding="utf-8").endswith("\n")


def test_repeated_exports_with_same_input_produce_same_file_contents(
    tmp_path: pathlib.Path,
) -> None:
    payload = {"request_id": "REQ-001", "value": "same"}

    first = export_json_output(payload, str(tmp_path))
    first_contents = pathlib.Path(first["path"]).read_text(encoding="utf-8")
    second = export_json_output(payload, str(tmp_path))
    second_contents = pathlib.Path(second["path"]).read_text(encoding="utf-8")

    assert first == second
    assert first_contents == second_contents


def test_payload_remains_unchanged(tmp_path: pathlib.Path) -> None:
    payload = {
        "request_id": "REQ-001",
        "nested": {"value": "keep"},
    }
    snapshot = copy.deepcopy(payload)

    export_json_output(payload, str(tmp_path))

    assert payload == snapshot


def test_filename_sanitizes_forward_and_back_slashes_only(
    tmp_path: pathlib.Path,
) -> None:
    result = export_json_output(
        {"request_id": r"REQ/001\A:B"},
        str(tmp_path),
    )

    assert result["filename"] == "output_REQ_001_A:B.json"
