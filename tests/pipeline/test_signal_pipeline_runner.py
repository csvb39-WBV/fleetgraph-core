from __future__ import annotations

import copy
import pathlib
import sys

import pytest


PROJECT_ROOT = pathlib.Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


from src.fleetgraph.pipeline.signal_pipeline_runner import run_signal_pipeline


def _signal(
    *,
    company: str,
    signal_type: str,
    event_summary: str,
    raw_text: str,
) -> dict[str, object]:
    return {
        "company": company,
        "signal_type": signal_type,
        "event_summary": event_summary,
        "source": "public-record.example",
        "date_detected": "2026-03-27",
        "confidence_score": None,
        "priority": None,
        "raw_text": raw_text,
    }


def _acquisition_runner(_: dict) -> list[dict[str, object]]:
    return [
        _signal(
            company="Atlas Build Co",
            signal_type="litigation",
            event_summary="Lawsuit filed",
            raw_text="Construction contractor lawsuit filed against Atlas Build Co.",
        ),
        _signal(
            company="Beacon Masonry",
            signal_type="audit",
            event_summary="Audit investigation announced",
            raw_text="Audit investigation announced for Beacon Masonry.",
        ),
        _signal(
            company="Civic Review LLC",
            signal_type="government",
            event_summary="Review opened",
            raw_text="Routine review opened for Civic Review LLC.",
        ),
    ]


def test_pipeline_determinism(tmp_path: pathlib.Path) -> None:
    request = {"run_date": "2026-03-27"}

    first = run_signal_pipeline(request, _acquisition_runner, tmp_path)
    second = run_signal_pipeline(request, _acquisition_runner, tmp_path)

    assert first == second


def test_pipeline_outputs_only_high_quality_signals(tmp_path: pathlib.Path) -> None:
    result = run_signal_pipeline({"run_date": "2026-03-27"}, _acquisition_runner, tmp_path)

    assert result["retained_signal_count"] == 2
    assert result["exported_signal_count"] == 2
    assert [signal["company"] for signal in result["primary_signals"]] == [
        "Atlas Build Co",
        "Beacon Masonry",
    ]


def test_pipeline_input_validation(tmp_path: pathlib.Path) -> None:
    with pytest.raises(ValueError):
        run_signal_pipeline([], _acquisition_runner, tmp_path)


def test_pipeline_no_input_mutation(tmp_path: pathlib.Path) -> None:
    request = {"run_date": "2026-03-27"}
    snapshot = copy.deepcopy(request)

    _ = run_signal_pipeline(request, _acquisition_runner, tmp_path)

    assert request == snapshot
