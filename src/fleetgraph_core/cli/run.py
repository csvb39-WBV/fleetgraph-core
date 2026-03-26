from __future__ import annotations

import argparse
import json
import sys
from copy import deepcopy
from pathlib import Path

from fleetgraph_core.api.batch_endpoint_adapter import apply_batch_endpoint_request
from fleetgraph_core.api.single_record_endpoint import handle_single_record_request


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument("input_json_path")
    parser.add_argument("--output", dest="output_json_path")
    return parser


def _load_input(path: str) -> object:
    return json.loads(Path(path).read_text(encoding="utf-8"))


def _dispatch_request(payload: object) -> dict[str, object]:
    if not isinstance(payload, dict):
        raise ValueError("input JSON does not match a supported request envelope.")

    if set(payload.keys()) == {"request_id", "endpoint_id", "records"}:
        return apply_batch_endpoint_request(deepcopy(payload))

    if set(payload.keys()) == {"response_type", "record", "limit", "minimum_priority"}:
        return handle_single_record_request(deepcopy(payload))

    raise ValueError("input JSON does not match a supported request envelope.")


def _serialize_payload(payload: dict[str, object]) -> str:
    return json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=False,
        indent=2,
    ) + "\n"


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)

    payload = _load_input(args.input_json_path)
    result = _dispatch_request(payload)
    serialized = _serialize_payload(result)

    sys.stdout.write(serialized)

    if args.output_json_path is not None:
        Path(args.output_json_path).write_text(serialized, encoding="utf-8")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
