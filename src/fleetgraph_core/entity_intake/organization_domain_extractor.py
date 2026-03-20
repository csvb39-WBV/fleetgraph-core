"""Deterministic organization domain extraction."""

from copy import deepcopy


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

ORGANIZATION_DOMAIN_CANDIDATE_FIELDS = (
    "organization_domain_candidate_id",
    "organization_candidate_id",
    "candidate_id",
    "seed_id",
    "source_id",
    "source_label",
    "organization_name",
    "domain_candidate",
    "base_url",
    "query_text",
    "candidate_state",
)


def validate_organization_candidates(organization_candidates: list[dict]) -> None:
    """Validate FG2-MB1 organization candidates for domain derivation."""
    if not isinstance(organization_candidates, list):
        raise TypeError("organization_candidates must be a list")

    seen_organization_candidate_ids = set()

    for organization_candidate in organization_candidates:
        if not isinstance(organization_candidate, dict):
            raise TypeError("each organization candidate must be a dictionary")

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

        for field_name in ORGANIZATION_CANDIDATE_FIELDS:
            field_value = organization_candidate[field_name]
            if not isinstance(field_value, str):
                raise TypeError(f"{field_name} must be a non-empty string")
            if field_value.strip() == "":
                raise ValueError(f"{field_name} must be a non-empty string")

        if organization_candidate["candidate_state"].strip() != "parsed":
            raise ValueError("candidate_state must be exactly 'parsed'")

        normalized_organization_candidate_id = organization_candidate[
            "organization_candidate_id"
        ].strip()
        if normalized_organization_candidate_id in seen_organization_candidate_ids:
            raise ValueError(
                "duplicate organization_candidate_id detected: "
                + normalized_organization_candidate_id
            )

        seen_organization_candidate_ids.add(normalized_organization_candidate_id)


def derive_domain_candidate(organization_candidate: dict) -> str:
    """Derive a deterministic domain candidate from an organization candidate."""
    validate_organization_candidates([organization_candidate])

    base_url = organization_candidate["base_url"].strip().lower()

    domain_candidate = base_url
    if domain_candidate.startswith("https://"):
        domain_candidate = domain_candidate[len("https://") :]
    elif domain_candidate.startswith("http://"):
        domain_candidate = domain_candidate[len("http://") :]

    domain_candidate = domain_candidate.split("/")[0].strip()

    if domain_candidate == "":
        raise ValueError("no valid domain candidate could be derived")
    if "." not in domain_candidate:
        raise ValueError("no valid domain candidate could be derived")

    return domain_candidate


def build_organization_domain_candidate(
    organization_candidate: dict,
    domain_candidate: str,
) -> dict:
    """Build one canonical organization domain candidate."""
    validate_organization_candidates([organization_candidate])

    if not isinstance(domain_candidate, str):
        raise TypeError("domain_candidate must be a non-empty string")
    if domain_candidate.strip() == "":
        raise ValueError("domain_candidate must be a non-empty string")

    normalized_organization_candidate = {}
    for field_name in ORGANIZATION_CANDIDATE_FIELDS:
        normalized_organization_candidate[field_name] = organization_candidate[
            field_name
        ].strip()

    normalized_domain_candidate = domain_candidate.strip().lower()

    domain_candidate_record = {
        "organization_domain_candidate_id": (
            normalized_organization_candidate["organization_candidate_id"]
            + "::"
            + normalized_domain_candidate
        ),
        "organization_candidate_id": normalized_organization_candidate[
            "organization_candidate_id"
        ],
        "candidate_id": normalized_organization_candidate["candidate_id"],
        "seed_id": normalized_organization_candidate["seed_id"],
        "source_id": normalized_organization_candidate["source_id"],
        "source_label": normalized_organization_candidate["source_label"],
        "organization_name": normalized_organization_candidate["organization_name"],
        "domain_candidate": normalized_domain_candidate,
        "base_url": normalized_organization_candidate["base_url"],
        "query_text": normalized_organization_candidate["query_text"],
        "candidate_state": "domain_derived",
    }

    if tuple(domain_candidate_record.keys()) != ORGANIZATION_DOMAIN_CANDIDATE_FIELDS:
        raise ValueError(
            "organization domain candidate fields must match the required contract exactly"
        )

    return deepcopy(domain_candidate_record)


def build_organization_domain_candidate_identity(domain_candidate_record: dict) -> tuple:
    """Build a deterministic identity tuple for domain candidates."""
    if not isinstance(domain_candidate_record, dict):
        raise TypeError("domain_candidate_record must be a dictionary")

    field_names = set(domain_candidate_record.keys())
    required_field_names = set(ORGANIZATION_DOMAIN_CANDIDATE_FIELDS)

    missing_fields = sorted(required_field_names - field_names)
    extra_fields = sorted(field_names - required_field_names)

    if missing_fields:
        raise ValueError(
            "organization domain candidate is missing required fields: "
            + ", ".join(missing_fields)
        )

    if extra_fields:
        raise ValueError(
            "organization domain candidate contains unknown fields: "
            + ", ".join(extra_fields)
        )

    return (
        domain_candidate_record["domain_candidate"],
        domain_candidate_record["organization_candidate_id"],
        domain_candidate_record["organization_domain_candidate_id"],
    )


def assemble_organization_domain_candidates(
    organization_candidates: list[dict],
) -> list[dict]:
    """Assemble deterministic organization domain candidates."""
    validate_organization_candidates(organization_candidates)

    domain_candidate_records = []
    seen_organization_domain_candidate_ids = set()

    for organization_candidate in deepcopy(organization_candidates):
        domain_candidate = derive_domain_candidate(organization_candidate)
        domain_candidate_record = build_organization_domain_candidate(
            organization_candidate,
            domain_candidate,
        )

        organization_domain_candidate_id = domain_candidate_record[
            "organization_domain_candidate_id"
        ]
        if organization_domain_candidate_id in seen_organization_domain_candidate_ids:
            raise ValueError(
                "duplicate organization_domain_candidate_id detected: "
                + organization_domain_candidate_id
            )

        seen_organization_domain_candidate_ids.add(
            organization_domain_candidate_id
        )
        domain_candidate_records.append(domain_candidate_record)

    sorted_records = sorted(
        domain_candidate_records,
        key=build_organization_domain_candidate_identity,
    )
    return deepcopy(sorted_records)
