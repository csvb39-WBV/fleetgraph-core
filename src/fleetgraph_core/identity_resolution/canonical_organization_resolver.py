"""Deterministic canonical organization resolution."""

from copy import deepcopy


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

CANONICAL_ORGANIZATION_FIELDS = (
    "canonical_organization_id",
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


def validate_organization_domain_candidates(domain_candidates: list[dict]) -> None:
    """Validate FG3-MB1 organization domain candidates for canonical resolution."""
    if not isinstance(domain_candidates, list):
        raise TypeError("domain_candidates must be a list")

    for candidate in domain_candidates:
        if not isinstance(candidate, dict):
            raise TypeError("each organization domain candidate must be a dictionary")

        field_names = set(candidate.keys())
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

        for field_name in ORGANIZATION_DOMAIN_CANDIDATE_FIELDS:
            field_value = candidate[field_name]
            if not isinstance(field_value, str):
                raise TypeError(f"{field_name} must be a non-empty string")
            if field_value.strip() == "":
                raise ValueError(f"{field_name} must be a non-empty string")

        if candidate["candidate_state"].strip() != "domain_derived":
            raise ValueError("candidate_state must be exactly 'domain_derived'")


def normalize_canonical_organization_name(domain_candidate: dict) -> str:
    """Normalize the canonical organization name from a domain candidate."""
    validate_organization_domain_candidates([domain_candidate])
    return domain_candidate["organization_name"].strip()


def resolve_canonical_organization_key(domain_candidate: dict, canonical_name: str) -> str:
    """Resolve the canonical organization key deterministically."""
    validate_organization_domain_candidates([domain_candidate])

    if not isinstance(canonical_name, str):
        raise TypeError("canonical_name must be a non-empty string")
    if canonical_name.strip() == "":
        raise ValueError("canonical_name must be a non-empty string")

    return domain_candidate["organization_domain_candidate_id"].strip() + "::canonical"


def build_canonical_organization_record(domain_candidate: dict, canonical_name: str, canonical_key: str) -> dict:
    """Build one canonical organization record."""
    validate_organization_domain_candidates([domain_candidate])

    if not isinstance(canonical_name, str):
        raise TypeError("canonical_name must be a non-empty string")
    if canonical_name.strip() == "":
        raise ValueError("canonical_name must be a non-empty string")

    if not isinstance(canonical_key, str):
        raise TypeError("canonical_key must be a non-empty string")
    if canonical_key.strip() == "":
        raise ValueError("canonical_key must be a non-empty string")

    normalized_input = {}
    for field_name in ORGANIZATION_DOMAIN_CANDIDATE_FIELDS:
        normalized_input[field_name] = domain_candidate[field_name].strip()

    canonical_record = {
        "canonical_organization_id": canonical_key,
        "organization_domain_candidate_id": normalized_input["organization_domain_candidate_id"],
        "organization_candidate_id": normalized_input["organization_candidate_id"],
        "candidate_id": normalized_input["candidate_id"],
        "seed_id": normalized_input["seed_id"],
        "source_id": normalized_input["source_id"],
        "source_label": normalized_input["source_label"],
        "organization_name": canonical_name,
        "domain_candidate": normalized_input["domain_candidate"],
        "base_url": normalized_input["base_url"],
        "query_text": normalized_input["query_text"],
        "candidate_state": "canonicalized",
    }

    if tuple(canonical_record.keys()) != CANONICAL_ORGANIZATION_FIELDS:
        raise ValueError(
            "canonical organization fields must match the required contract exactly"
        )

    return deepcopy(canonical_record)


def build_canonical_organization_identity(canonical_record: dict) -> tuple:
    """Build deterministic identity tuple for canonical organization records."""
    if not isinstance(canonical_record, dict):
        raise TypeError("canonical_record must be a dictionary")

    field_names = set(canonical_record.keys())
    required_field_names = set(CANONICAL_ORGANIZATION_FIELDS)

    missing_fields = sorted(required_field_names - field_names)
    extra_fields = sorted(field_names - required_field_names)

    if missing_fields:
        raise ValueError(
            "canonical organization is missing required fields: " + ", ".join(missing_fields)
        )

    if extra_fields:
        raise ValueError(
            "canonical organization contains unknown fields: " + ", ".join(extra_fields)
        )

    return (
        canonical_record["domain_candidate"],
        canonical_record["candidate_id"],
        canonical_record["canonical_organization_id"],
    )


def assemble_canonical_organizations(domain_candidates: list[dict]) -> list[dict]:
    """Assemble deterministic canonical organization records."""
    validate_organization_domain_candidates(domain_candidates)

    records = []
    seen_canonical_ids = set()

    for domain_candidate in deepcopy(domain_candidates):
        canonical_name = normalize_canonical_organization_name(domain_candidate)
        canonical_key = resolve_canonical_organization_key(domain_candidate, canonical_name)
        canonical_record = build_canonical_organization_record(domain_candidate, canonical_name, canonical_key)

        canonical_id = canonical_record["canonical_organization_id"]
        if canonical_id in seen_canonical_ids:
            raise ValueError("duplicate canonical_organization_id detected: " + canonical_id)

        seen_canonical_ids.add(canonical_id)
        records.append(canonical_record)

    sorted_records = sorted(records, key=build_canonical_organization_identity)
    return deepcopy(sorted_records)
