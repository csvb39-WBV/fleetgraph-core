from __future__ import annotations

import argparse
import json
from pathlib import Path

from fleetgraph.runtime.pipeline_execution_service import execute_signal_pipeline
from fleetgraph.runtime.runtime_config import build_default_runtime_config, build_runtime_config


def _load_config(config_path: str | None) -> dict[str, object]:
    if config_path is None:
        return build_default_runtime_config(Path(__file__).resolve().parents[3])
    payload = json.loads(Path(config_path).read_text(encoding="utf-8"))
    return build_runtime_config(payload)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run the FleetGraph signal pipeline.")
    parser.add_argument("--config", dest="config_path", default=None)
    args = parser.parse_args(argv)

    try:
        runtime_config = _load_config(args.config_path)
    except Exception:
        print("status=failed error_code=invalid_runtime_config")
        return 1

    result = execute_signal_pipeline(runtime_config)
    if result["ok"] is True:
        print(
            "status=success "
            f"csv_path={result['csv_path']} "
            f"manifest_path={result['manifest_path']}"
        )
        return 0

    print(
        "status=failed "
        f"error_code={result['error_code']} "
        f"manifest_path={result['manifest_path']}"
    )
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
