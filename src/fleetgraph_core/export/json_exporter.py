from __future__ import annotations

import json
from pathlib import Path


def _validate_output_dir(output_dir: str) -> str:
    if not isinstance(output_dir, str) or output_dir.strip() == "":
        raise ValueError("output_dir must be a non-empty string.")
    return output_dir


def _validate_identity(payload: dict[str, object]) -> str:
    request_id = payload.get("request_id")
    if isinstance(request_id, str) and request_id.strip() != "":
        return f"output_{request_id.replace('/', '_').replace(chr(92), '_')}.json"

    response_type = payload.get("response_type")
    if isinstance(response_type, str) and response_type.strip() != "":
        return f"output_{response_type.replace('/', '_').replace(chr(92), '_')}.json"

    raise ValueError("payload must contain an exportable request identity.")


def export_json_output(payload: dict[str, object], output_dir: str) -> dict[str, object]:
    if not isinstance(payload, dict):
        raise ValueError("payload must be a dictionary.")

    validated_output_dir = _validate_output_dir(output_dir)
    filename = _validate_identity(payload)

    output_path = Path(validated_output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    full_path = output_path / filename
    full_path.write_text(
        json.dumps(
            payload,
            ensure_ascii=False,
            sort_keys=False,
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )

    return {
        "ok": True,
        "path": str(full_path),
        "filename": filename,
    }
