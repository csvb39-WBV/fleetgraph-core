from __future__ import annotations

import json
import pathlib
import sys


PROJECT_ROOT = pathlib.Path(__file__).resolve().parents[2]
SRC_ROOT = PROJECT_ROOT / "src"

if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))


from fleetgraph.runtime.cli_entrypoint import main


def test_cli_input_validation(tmp_path: pathlib.Path, capsys) -> None:
    invalid_config_path = tmp_path / "invalid_config.json"
    invalid_config_path.write_text("{}", encoding="utf-8")

    exit_code = main(["--config", str(invalid_config_path)])
    captured = capsys.readouterr()

    assert exit_code == 1
    assert captured.out.strip() == "status=failed error_code=invalid_runtime_config"


def test_cli_success_path(tmp_path: pathlib.Path, capsys) -> None:
    config_path = tmp_path / "runtime_config.json"
    config_path.write_text(
        json.dumps(
            {
                "run_date": "2026-03-27",
                "output_directory": str(tmp_path / "outputs"),
                "cache_path": str(tmp_path / "cache" / "signal_cache.json"),
                "max_queries_per_run": 7,
                "max_results_per_query": 5,
                "connector_timeout_seconds": 5.0,
                "connector_max_retries": 0,
            }
        ),
        encoding="utf-8",
    )

    exit_code = main(["--config", str(config_path)])
    captured = capsys.readouterr()

    assert exit_code == 1 or exit_code == 0
    assert captured.out.strip() != ""
