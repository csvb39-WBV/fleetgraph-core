"""Deterministic raw signal candidate intake."""

from copy import deepcopy
import hashlib


SEED_FIELDS = (
    "seed_id",
    "source_id",
    "source_label",
    "base_url",
    "query_text",
    "collection_state",
)

RAW_SIGNAL_FIELDS = (
    "raw_signal_text",
    "raw_signal_timestamp",
)

CANDIDATE_FIELDS = (
    "candidate_id",
    "seed_id",
    "source_id",
    "source_label",
    "base_url",
    "query_text",
    "raw_signal_text",
    "raw_signal_timestamp",
    "collection_state",
)


def validate_seed_records(seed_records: list[dict]) -> None:
    """Validate FG1-MB1 seed records for candidate intake."""
    if not isinstance(seed_records, list):
        raise TypeError("seed_records must be a list")

    seen_seed_ids = set()

    for seed_record in seed_records:
        if not isinstance(seed_record, dict):
            raise TypeError("each seed record must be a dictionary")

        field_names = set(seed_record.keys())
        required_field_names = set(SEED_FIELDS)

        missing_fields = sorted(required_field_names - field_names)
        extra_fields = sorted(field_names - required_field_names)

        if missing_fields:
            raise ValueError(
                "seed record is missing required fields: "
                + ", ".join(missing_fields)
            )

        if extra_fields:
            raise ValueError(
                "seed record contains unknown fields: "
                + ", ".join(extra_fields)
            )

        for field_name in SEED_FIELDS:
            field_value = seed_record[field_name]
            if not isinstance(field_value, str):
                raise TypeError(f"{field_name} must be a non-empty string")
            if field_value.strip() == "":
                raise ValueError(f"{field_name} must be a non-empty string")

        if seed_record["collection_state"].strip() != "pending_collection":
            raise ValueError(
                "collection_state must be exactly 'pending_collection'"
            )

        normalized_seed_id = seed_record["seed_id"].strip()
        if normalized_seed_id in seen_seed_ids:
            raise ValueError(f"duplicate seed_id detected: {normalized_seed_id}")

        seen_seed_ids.add(normalized_seed_id)


def simulate_raw_signal(seed_record: dict) -> dict:
    """Simulate a deterministic minimal raw signal from a valid seed record."""
    validate_seed_records([seed_record])

    normalized_seed_record = {}
    for field_name in SEED_FIELDS:
        normalized_seed_record[field_name] = seed_record[field_name].strip()

    raw_signal_text = (
        f"{normalized_seed_record['source_label']} | "
        f"{normalized_seed_record['query_text']} | "
        f"{normalized_seed_record['base_url']}"
    )

    digest_source = (
        normalized_seed_record["seed_id"]
        + "::"
        + normalized_seed_record["source_id"]
        + "::"
        + normalized_seed_record["query_text"]
    )
    digest = hashlib.sha256(digest_source.encode("utf-8")).hexdigest()

    hour = int(digest[0:2], 16) % 24
    minute = int(digest[2:4], 16) % 60
    second = int(digest[4:6], 16) % 60
    raw_signal_timestamp = (
        f"2024-01-01T{hour:02d}:{minute:02d}:{second:02d}Z"
    )

    return {
        "raw_signal_text": raw_signal_text,
        "raw_signal_timestamp": raw_signal_timestamp,
    }


def build_signal_candidate(raw_signal: dict, seed_record: dict) -> dict:
    """Build one canonical collected candidate from a raw signal and seed record."""
    validate_seed_records([seed_record])

    if not isinstance(raw_signal, dict):
        raise TypeError("raw_signal must be a dictionary")

    raw_signal_field_names = set(raw_signal.keys())
    required_raw_signal_field_names = set(RAW_SIGNAL_FIELDS)

    missing_fields = sorted(required_raw_signal_field_names - raw_signal_field_names)
    extra_fields = sorted(raw_signal_field_names - required_raw_signal_field_names)

    if missing_fields:
        raise ValueError(
            "raw_signal is missing required fields: "
            + ", ".join(missing_fields)
        )

    if extra_fields:
        raise ValueError(
            "raw_signal contains unknown fields: "
            + ", ".join(extra_fields)
        )

    for field_name in RAW_SIGNAL_FIELDS:
        field_value = raw_signal[field_name]
        if not isinstance(field_value, str):
            raise TypeError(f"{field_name} must be a non-empty string")
        if field_value.strip() == "":
            raise ValueError(f"{field_name} must be a non-empty string")

    normalized_seed_record = {}
    for field_name in SEED_FIELDS:
        normalized_seed_record[field_name] = seed_record[field_name].strip()

    normalized_raw_signal = {}
    for field_name in RAW_SIGNAL_FIELDS:
        normalized_raw_signal[field_name] = raw_signal[field_name].strip()

    candidate_id = (
        normalized_seed_record["seed_id"]
        + "::"
        + normalized_raw_signal["raw_signal_timestamp"]
    )

    candidate = {
        "candidate_id": candidate_id,
        "seed_id": normalized_seed_record["seed_id"],
        "source_id": normalized_seed_record["source_id"],
        "source_label": normalized_seed_record["source_label"],
        "base_url": normalized_seed_record["base_url"],
        "query_text": normalized_seed_record["query_text"],
        "raw_signal_text": normalized_raw_signal["raw_signal_text"],
        "raw_signal_timestamp": normalized_raw_signal["raw_signal_timestamp"],
        "collection_state": "collected",
    }

    if tuple(candidate.keys()) != CANDIDATE_FIELDS:
        raise ValueError(
            "candidate fields must match the required contract exactly"
        )

    return deepcopy(candidate)


def build_candidate_identity(candidate: dict) -> tuple:
    """Build a deterministic candidate identity tuple."""
    if not isinstance(candidate, dict):
        raise TypeError("candidate must be a dictionary")

    field_names = set(candidate.keys())
    required_field_names = set(CANDIDATE_FIELDS)

    missing_fields = sorted(required_field_names - field_names)
    extra_fields = sorted(field_names - required_field_names)

    if missing_fields:
        raise ValueError(
            "candidate is missing required fields: "
            + ", ".join(missing_fields)
        )

    if extra_fields:
        raise ValueError(
            "candidate contains unknown fields: "
            + ", ".join(extra_fields)
        )

    return (
        candidate["source_id"],
        candidate["query_text"],
        candidate["seed_id"],
        candidate["raw_signal_timestamp"],
        candidate["candidate_id"],
    )


def assemble_signal_candidates(seed_records: list[dict]) -> list[dict]:
    """Assemble deterministic collected candidates from valid seed records."""
    validate_seed_records(seed_records)

    canonical_candidates = []
    seen_candidate_ids = set()

    for seed_record in deepcopy(seed_records):
        raw_signal = simulate_raw_signal(seed_record)
        candidate = build_signal_candidate(raw_signal, seed_record)

        candidate_id = candidate["candidate_id"]
        if candidate_id in seen_candidate_ids:
            raise ValueError(f"duplicate candidate_id detected: {candidate_id}")

        seen_candidate_ids.add(candidate_id)
        canonical_candidates.append(candidate)

    sorted_candidates = sorted(canonical_candidates, key=build_candidate_identity)
    return deepcopy(sorted_candidates)
