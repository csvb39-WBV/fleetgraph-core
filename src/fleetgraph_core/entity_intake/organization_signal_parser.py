"""Deterministic organization signal parsing."""

from copy import deepcopy


SIGNAL_CANDIDATE_FIELDS = (
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

ORGANIZATION_CANDIDATE_FIELDS = (
    "organization_candidate_id",
    "candidate_id",
    "seed_id",
    "source_id",
    "source_label",
    "base_url",
    "query_text",
    "organization_name",
    "raw_signal_text",
    "raw_signal_timestamp",
    "candidate_state",
)


def validate_signal_candidates(signal_candidates: list[dict]) -> None:
    """Validate FG1-MB2 signal candidates for organization parsing."""
    if not isinstance(signal_candidates, list):
        raise TypeError("signal_candidates must be a list")

    seen_candidate_ids = set()

    for signal_candidate in signal_candidates:
        if not isinstance(signal_candidate, dict):
            raise TypeError("each signal candidate must be a dictionary")

        field_names = set(signal_candidate.keys())
        required_field_names = set(SIGNAL_CANDIDATE_FIELDS)

        missing_fields = sorted(required_field_names - field_names)
        extra_fields = sorted(field_names - required_field_names)

        if missing_fields:
            raise ValueError(
                "signal candidate is missing required fields: "
                + ", ".join(missing_fields)
            )

        if extra_fields:
            raise ValueError(
                "signal candidate contains unknown fields: "
                + ", ".join(extra_fields)
            )

        for field_name in SIGNAL_CANDIDATE_FIELDS:
            field_value = signal_candidate[field_name]
            if not isinstance(field_value, str):
                raise TypeError(f"{field_name} must be a non-empty string")
            if field_value.strip() == "":
                raise ValueError(f"{field_name} must be a non-empty string")

        if signal_candidate["collection_state"].strip() != "collected":
            raise ValueError("collection_state must be exactly 'collected'")

        normalized_candidate_id = signal_candidate["candidate_id"].strip()
        if normalized_candidate_id in seen_candidate_ids:
            raise ValueError(
                f"duplicate candidate_id detected: {normalized_candidate_id}"
            )

        seen_candidate_ids.add(normalized_candidate_id)


def parse_organization_name(signal_candidate: dict) -> str:
    """Parse an organization name deterministically from raw_signal_text."""
    validate_signal_candidates([signal_candidate])

    raw_signal_text = signal_candidate["raw_signal_text"].strip()
    raw_signal_parts = raw_signal_text.split("|")

    if len(raw_signal_parts) < 2:
        raise ValueError(
            "no valid organization name could be derived from raw_signal_text"
        )

    organization_name = raw_signal_parts[0].strip()
    if organization_name == "":
        raise ValueError(
            "no valid organization name could be derived from raw_signal_text"
        )

    return organization_name


def build_organization_candidate(signal_candidate: dict, organization_name: str) -> dict:
    """Build one canonical parsed organization candidate."""
    validate_signal_candidates([signal_candidate])

    if not isinstance(organization_name, str):
        raise TypeError("organization_name must be a non-empty string")
    if organization_name.strip() == "":
        raise ValueError("organization_name must be a non-empty string")

    normalized_signal_candidate = {}
    for field_name in SIGNAL_CANDIDATE_FIELDS:
        normalized_signal_candidate[field_name] = signal_candidate[field_name].strip()

    normalized_organization_name = organization_name.strip()

    organization_candidate = {
        "organization_candidate_id": (
            normalized_signal_candidate["candidate_id"]
            + "::"
            + normalized_organization_name
        ),
        "candidate_id": normalized_signal_candidate["candidate_id"],
        "seed_id": normalized_signal_candidate["seed_id"],
        "source_id": normalized_signal_candidate["source_id"],
        "source_label": normalized_signal_candidate["source_label"],
        "base_url": normalized_signal_candidate["base_url"],
        "query_text": normalized_signal_candidate["query_text"],
        "organization_name": normalized_organization_name,
        "raw_signal_text": normalized_signal_candidate["raw_signal_text"],
        "raw_signal_timestamp": normalized_signal_candidate["raw_signal_timestamp"],
        "candidate_state": "parsed",
    }

    if tuple(organization_candidate.keys()) != ORGANIZATION_CANDIDATE_FIELDS:
        raise ValueError(
            "organization candidate fields must match the required contract exactly"
        )

    return deepcopy(organization_candidate)


def build_organization_candidate_identity(organization_candidate: dict) -> tuple:
    """Build a deterministic identity tuple for organization candidates."""
    if not isinstance(organization_candidate, dict):
        raise TypeError("organization_candidate must be a dictionary")

    field_names = set(organization_candidate.keys())
    required_field_names = set(ORGANIZATION_CANDIDATE_FIELDS)

    missing_fields = sorted(required_field_names - field_names)
    extra_fields = sorted(field_names - required_field_names)

    if missing_fields:
        raise ValueError(
            "organization candidate is missing required fields: "
            + ", ".join(missing_fields)
        )

    if extra_fields:
        raise ValueError(
            "organization candidate contains unknown fields: "
            + ", ".join(extra_fields)
        )

    return (
        organization_candidate["organization_name"],
        organization_candidate["candidate_id"],
        organization_candidate["organization_candidate_id"],
    )


def assemble_organization_candidates(signal_candidates: list[dict]) -> list[dict]:
    """Assemble deterministic parsed organization candidates."""
    validate_signal_candidates(signal_candidates)

    organization_candidates = []
    seen_organization_candidate_ids = set()

    for signal_candidate in deepcopy(signal_candidates):
        organization_name = parse_organization_name(signal_candidate)
        organization_candidate = build_organization_candidate(
            signal_candidate,
            organization_name,
        )

        organization_candidate_id = organization_candidate["organization_candidate_id"]
        if organization_candidate_id in seen_organization_candidate_ids:
            raise ValueError(
                "duplicate organization_candidate_id detected: "
                + organization_candidate_id
            )

        seen_organization_candidate_ids.add(organization_candidate_id)
        organization_candidates.append(organization_candidate)

    sorted_candidates = sorted(
        organization_candidates,
        key=build_organization_candidate_identity,
    )
    return deepcopy(sorted_candidates)
