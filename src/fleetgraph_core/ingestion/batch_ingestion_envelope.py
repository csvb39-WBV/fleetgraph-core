"""
D13-MB5 Batch Ingestion Envelope Builder.

Builds a deterministic closed-schema ingestion envelope for batching
multiple documents into a single ingestion unit.

Pure in-memory Python with strict validation.
No timestamps, no filesystem access, no external dependencies, no mutation.
"""

from __future__ import annotations

from typing import Any

_REQUIRED_INPUT_KEYS: frozenset[str] = frozenset({
    "batch_id",
    "documents",
})

_REQUIRED_DOCUMENT_KEYS: frozenset[str] = frozenset({
    "document_id",
    "content",
})

_EXPECTED_OUTPUT_KEYS: tuple[str, ...] = (
    "batch_id",
    "document_count",
    "documents",
)

_EXPECTED_OUTPUT_DOCUMENT_KEYS: tuple[str, ...] = (
    "document_id",
    "content",
)


def _require_closed_schema(obj: dict[str, Any], required_fields: frozenset[str], label: str) -> None:
    present = set(obj.keys())

    missing = required_fields - present
    if missing:
        raise ValueError(f"{label} is missing required fields: {', '.join(sorted(missing))}")

    extra = present - required_fields
    if extra:
        raise ValueError(f"{label} contains unexpected fields: {', '.join(sorted(extra))}")


def build_batch_ingestion_envelope(
    envelope_input: dict[str, Any],
) -> dict[str, Any]:
    """Build deterministic ingestion envelope from validated batch input."""
    if not isinstance(envelope_input, dict):
        raise TypeError("envelope_input must be a dict")

    _require_closed_schema(envelope_input, _REQUIRED_INPUT_KEYS, "envelope_input")

    batch_id = envelope_input["batch_id"]
    if not isinstance(batch_id, str):
        raise TypeError("envelope_input field 'batch_id' must be a str")
    if not batch_id:
        raise ValueError("envelope_input field 'batch_id' must be a non-empty string")

    documents = envelope_input["documents"]
    if not isinstance(documents, list):
        raise TypeError("envelope_input field 'documents' must be a list")
    if not documents:
        raise ValueError("envelope_input field 'documents' must be a non-empty list")

    normalized_documents: list[dict[str, str]] = []
    for index, document in enumerate(documents):
        if not isinstance(document, dict):
            raise TypeError(
                f"envelope_input field 'documents' entry at index {index} must be a dict"
            )

        _require_closed_schema(
            document,
            _REQUIRED_DOCUMENT_KEYS,
            f"envelope_input field 'documents' entry at index {index}",
        )

        document_id = document["document_id"]
        if not isinstance(document_id, str):
            raise TypeError(
                "envelope_input field 'documents' entry at index "
                f"{index} field 'document_id' must be a str"
            )
        if not document_id:
            raise ValueError(
                "envelope_input field 'documents' entry at index "
                f"{index} field 'document_id' must be a non-empty string"
            )

        content = document["content"]
        if not isinstance(content, str):
            raise TypeError(
                "envelope_input field 'documents' entry at index "
                f"{index} field 'content' must be a str"
            )

        # Preserve original order and copy each entry to avoid mutating input objects.
        output_document = {
            "document_id": document_id,
            "content": content,
        }
        if tuple(output_document.keys()) != _EXPECTED_OUTPUT_DOCUMENT_KEYS:
            raise RuntimeError("internal error: output document schema mismatch")
        normalized_documents.append(output_document)

    response: dict[str, Any] = {
        "batch_id": batch_id,
        "document_count": len(normalized_documents),
        "documents": normalized_documents,
    }

    if tuple(response.keys()) != _EXPECTED_OUTPUT_KEYS:
        raise RuntimeError("internal error: envelope response schema mismatch")

    return response
