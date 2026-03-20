"""Deterministic signal source catalog construction."""

from copy import deepcopy


REQUIRED_SOURCE_FIELDS = (
    "source_id",
    "source_label",
    "base_url",
    "channel_type",
    "is_active",
)


def build_source_identity(signal_source: dict) -> tuple:
    """Build a deterministic source identity tuple."""
    if not isinstance(signal_source, dict):
        raise TypeError("signal_source must be a dictionary")

    return (
        signal_source["source_id"],
        signal_source["source_label"],
        signal_source["base_url"],
        signal_source["channel_type"],
        signal_source["is_active"],
    )


def validate_signal_sources(signal_sources: list[dict]) -> None:
    """Validate source definitions for catalog construction."""
    if not isinstance(signal_sources, list):
        raise TypeError("signal_sources must be a list")

    seen_source_ids = set()

    for signal_source in signal_sources:
        if not isinstance(signal_source, dict):
            raise TypeError("each signal source must be a dictionary")

        field_names = set(signal_source.keys())
        required_field_names = set(REQUIRED_SOURCE_FIELDS)

        missing_fields = sorted(required_field_names - field_names)
        extra_fields = sorted(field_names - required_field_names)

        if missing_fields:
            raise ValueError(
                "signal source is missing required fields: "
                + ", ".join(missing_fields)
            )

        if extra_fields:
            raise ValueError(
                "signal source contains unknown fields: "
                + ", ".join(extra_fields)
            )

        for field_name in ("source_id", "source_label", "base_url", "channel_type"):
            field_value = signal_source[field_name]
            if not isinstance(field_value, str):
                raise TypeError(f"{field_name} must be a non-empty string")
            if field_value.strip() == "":
                raise ValueError(f"{field_name} must be a non-empty string")

        if not isinstance(signal_source["is_active"], bool):
            raise TypeError("is_active must be a boolean")

        source_id = signal_source["source_id"].strip()
        if source_id in seen_source_ids:
            raise ValueError(f"duplicate source_id detected: {source_id}")

        seen_source_ids.add(source_id)


def build_signal_source_catalog(signal_sources: list[dict]) -> list[dict]:
    """Build a canonical, deterministic source catalog."""
    validate_signal_sources(signal_sources)

    canonical_sources = []
    for signal_source in signal_sources:
        canonical_source = {
            "source_id": signal_source["source_id"].strip(),
            "source_label": signal_source["source_label"].strip(),
            "base_url": signal_source["base_url"].strip(),
            "channel_type": signal_source["channel_type"].strip(),
            "is_active": signal_source["is_active"],
        }
        canonical_sources.append(canonical_source)

    sorted_sources = sorted(canonical_sources, key=build_source_identity)
    return deepcopy(sorted_sources)
