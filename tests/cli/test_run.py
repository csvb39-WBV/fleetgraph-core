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


import fleetgraph_core.cli.run as run_module


def _single_payload() -> dict[str, object]:
    return {
        "response_type": "analysis",
        "record": {"event_id": "EVT-001"},
        "limit": None,
        "minimum_priority": None,
    }


def _batch_payload() -> dict[str, object]:
    return {
        "request_id": "REQ-001",
        "endpoint_id": "analysis",
        "records": [{"event_id": "EVT-001"}],
    }


def _write_json(path: pathlib.Path, payload: object) -> None:
    path.write_text(json.dumps(payload), encoding="utf-8")


def test_dispatches_single_envelope_to_single_adapter(
    tmp_path: pathlib.Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    input_path = tmp_path / "single.json"
    _write_json(input_path, _single_payload())
    calls: list[dict[str, object]] = []

    monkeypatch.setattr(
        run_module,
        "handle_single_record_request",
        lambda payload: calls.append(copy.deepcopy(payload)) or {"ok": True},
    )
    monkeypatch.setattr(
        run_module,
        "apply_batch_endpoint_request",
        lambda payload: {"ok": False},
    )

    run_module.main([str(input_path)])
    capsys.readouterr()

    assert calls == [_single_payload()]


def test_dispatches_batch_envelope_to_batch_adapter(
    tmp_path: pathlib.Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    input_path = tmp_path / "batch.json"
    _write_json(input_path, _batch_payload())
    calls: list[dict[str, object]] = []

    monkeypatch.setattr(
        run_module,
        "apply_batch_endpoint_request",
        lambda payload: calls.append(copy.deepcopy(payload)) or {"ok": True},
    )
    monkeypatch.setattr(
        run_module,
        "handle_single_record_request",
        lambda payload: {"ok": False},
    )

    run_module.main([str(input_path)])
    capsys.readouterr()

    assert calls == [_batch_payload()]


def test_unsupported_envelope_raises_exact_value_error(tmp_path: pathlib.Path) -> None:
    input_path = tmp_path / "unsupported.json"
    _write_json(input_path, {"bad": "payload"})

    with pytest.raises(
        ValueError,
        match=r"^input JSON does not match a supported request envelope\.$",
    ):
        run_module.main([str(input_path)])


def test_invalid_json_propagates_json_decode_error(tmp_path: pathlib.Path) -> None:
    input_path = tmp_path / "invalid.json"
    input_path.write_text("{bad json", encoding="utf-8")

    with pytest.raises(json.JSONDecodeError):
        run_module.main([str(input_path)])


def test_missing_file_propagates_file_error() -> None:
    with pytest.raises(FileNotFoundError):
        run_module.main(["C:\\missing\\input.json"])


def test_stdout_emits_exact_adapter_payload_as_json(
    tmp_path: pathlib.Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    input_path = tmp_path / "single.json"
    _write_json(input_path, _single_payload())
    payload = {
        "ok": True,
        "response": {
            "response_type": "analysis",
            "source_event_id": "EVT-001",
        },
    }

    monkeypatch.setattr(
        run_module,
        "handle_single_record_request",
        lambda request_payload: payload,
    )

    run_module.main([str(input_path)])
    captured = capsys.readouterr()

    assert captured.out == json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=False,
        indent=2,
    ) + "\n"


def test_output_writes_exact_payload_to_file(
    tmp_path: pathlib.Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    input_path = tmp_path / "single.json"
    output_path = tmp_path / "output.json"
    _write_json(input_path, _single_payload())
    payload = {
        "ok": True,
        "response": {
            "response_type": "summary",
            "source_event_id": "EVT-001",
        },
    }

    monkeypatch.setattr(
        run_module,
        "handle_single_record_request",
        lambda request_payload: payload,
    )

    run_module.main([str(input_path), "--output", str(output_path)])
    captured = capsys.readouterr()

    expected = json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=False,
        indent=2,
    ) + "\n"
    assert captured.out == expected
    assert output_path.read_text(encoding="utf-8") == expected


def test_stdout_and_output_file_payloads_match_exactly(
    tmp_path: pathlib.Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    input_path = tmp_path / "single.json"
    output_path = tmp_path / "output.json"
    _write_json(input_path, _single_payload())

    monkeypatch.setattr(
        run_module,
        "handle_single_record_request",
        lambda request_payload: {"ok": True, "response": request_payload},
    )

    run_module.main([str(input_path), "--output", str(output_path)])
    captured = capsys.readouterr()

    assert output_path.read_text(encoding="utf-8") == captured.out


def test_printed_json_ends_with_newline(
    tmp_path: pathlib.Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    input_path = tmp_path / "single.json"
    _write_json(input_path, _single_payload())

    monkeypatch.setattr(
        run_module,
        "handle_single_record_request",
        lambda request_payload: {"ok": True},
    )

    run_module.main([str(input_path)])
    captured = capsys.readouterr()

    assert captured.out.endswith("\n")


def test_repeated_runs_with_same_mocked_output_are_identical(
    tmp_path: pathlib.Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    input_path = tmp_path / "single.json"
    _write_json(input_path, _single_payload())

    monkeypatch.setattr(
        run_module,
        "handle_single_record_request",
        lambda request_payload: {"ok": True, "response": request_payload},
    )

    run_module.main([str(input_path)])
    first = capsys.readouterr().out
    run_module.main([str(input_path)])
    second = capsys.readouterr().out

    assert first == second


def test_loaded_input_object_is_not_mutated_before_adapter_call(
    tmp_path: pathlib.Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    input_path = tmp_path / "single.json"
    payload = _single_payload()
    _write_json(input_path, payload)
    captured_payloads: list[dict[str, object]] = []

    def _capturing_handler(request_payload: dict[str, object]) -> dict[str, object]:
        snapshot = copy.deepcopy(request_payload)
        captured_payloads.append(snapshot)
        request_payload["response_type"] = "mutated"
        request_payload["record"]["event_id"] = "MUTATED"
        return {"ok": True}

    monkeypatch.setattr(run_module, "handle_single_record_request", _capturing_handler)

    run_module.main([str(input_path)])
    capsys.readouterr()

    assert captured_payloads == [payload]
    assert json.loads(input_path.read_text(encoding="utf-8")) == payload


def test_dispatch_request_protects_in_memory_payload_from_adapter_mutation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    payload = _single_payload()
    snapshot = copy.deepcopy(payload)

    def _mutating_handler(request_payload: dict[str, object]) -> dict[str, object]:
        request_payload["response_type"] = "mutated"
        request_payload["record"]["event_id"] = "MUTATED"
        return {"ok": True}

    monkeypatch.setattr(run_module, "handle_single_record_request", _mutating_handler)

    result = run_module._dispatch_request(payload)

    assert result == {"ok": True}
    assert payload == snapshot
