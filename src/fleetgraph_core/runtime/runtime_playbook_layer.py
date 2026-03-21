"""
MB8 Fleet Sales Playbook Layer.

Defines and validates fleet sales playbooks and deterministically transforms
playbook + target accounts into MB6-A runtime inputs.

Domain-specific scope: fleet outbound/expansion/reengagement playbooks.
No persistence, no randomness, no timestamps, no UUIDs.
"""

from copy import deepcopy

_PLAYBOOK_REQUIRED_FIELDS: frozenset[str] = frozenset({
    "playbook_id",
    "playbook_type",
    "customer_type",
    "execution_mode",
})

_PLAYBOOK_TYPE_ALLOWED: frozenset[str] = frozenset({
    "fleet_outbound",
    "fleet_expansion",
    "fleet_reengagement",
})

_EXECUTION_MODE_ALLOWED: frozenset[str] = frozenset({
    "sequential",
    "parallel",
})

_TARGET_ACCOUNT_REQUIRED_FIELDS: frozenset[str] = frozenset({
    "account_id",
    "organization_name",
    "domain",
})


def _validate_non_empty_string(value: object, field_name: str) -> None:
    if not isinstance(value, str):
        raise TypeError(f"{field_name} must be a string")
    if not value.strip():
        raise ValueError(f"{field_name} must not be empty or whitespace-only")


def validate_fleet_playbook(playbook: dict[str, object]) -> None:
    """Validate a closed-schema fleet playbook definition."""
    if not isinstance(playbook, dict):
        raise TypeError("playbook must be a dict")

    present = set(playbook.keys())

    missing = _PLAYBOOK_REQUIRED_FIELDS - present
    if missing:
        raise ValueError(f"playbook is missing required fields: {', '.join(sorted(missing))}")

    extra = present - _PLAYBOOK_REQUIRED_FIELDS
    if extra:
        raise ValueError(f"playbook contains unexpected fields: {', '.join(sorted(extra))}")

    _validate_non_empty_string(playbook["playbook_id"], "playbook field 'playbook_id'")
    _validate_non_empty_string(playbook["playbook_type"], "playbook field 'playbook_type'")
    _validate_non_empty_string(playbook["customer_type"], "playbook field 'customer_type'")
    _validate_non_empty_string(playbook["execution_mode"], "playbook field 'execution_mode'")

    playbook_type = playbook["playbook_type"]
    if playbook_type not in _PLAYBOOK_TYPE_ALLOWED:
        raise ValueError(
            "playbook field 'playbook_type' must be one of: "
            "fleet_expansion, fleet_outbound, fleet_reengagement"
        )

    execution_mode = playbook["execution_mode"]
    if execution_mode not in _EXECUTION_MODE_ALLOWED:
        raise ValueError("playbook field 'execution_mode' must be one of: parallel, sequential")


def _validate_target_accounts(target_accounts: list[dict[str, object]]) -> None:
    if not isinstance(target_accounts, list):
        raise TypeError("target_accounts must be a list")

    for i, account in enumerate(target_accounts):
        if not isinstance(account, dict):
            raise TypeError(f"target_accounts[{i}] must be a dict")

        present = set(account.keys())

        missing = _TARGET_ACCOUNT_REQUIRED_FIELDS - present
        if missing:
            raise ValueError(
                f"target_accounts[{i}] is missing required fields: {', '.join(sorted(missing))}"
            )

        extra = present - _TARGET_ACCOUNT_REQUIRED_FIELDS
        if extra:
            raise ValueError(
                f"target_accounts[{i}] contains unexpected fields: {', '.join(sorted(extra))}"
            )

        _validate_non_empty_string(account["account_id"], f"target_accounts[{i}] field 'account_id'")
        _validate_non_empty_string(
            account["organization_name"],
            f"target_accounts[{i}] field 'organization_name'",
        )
        _validate_non_empty_string(account["domain"], f"target_accounts[{i}] field 'domain'")


def build_runtime_inputs_from_playbook(
    playbook: dict[str, object],
    target_accounts: list[dict[str, object]],
) -> dict[str, object]:
    """
    Build MB6-A compatible runtime_template and scheduled_batches from a
    fleet playbook and ordered target accounts.
    """
    validate_fleet_playbook(playbook)
    _validate_target_accounts(target_accounts)

    playbook_id = playbook["playbook_id"]
    playbook_type = playbook["playbook_type"]
    execution_mode = playbook["execution_mode"]

    runtime_template = {
        "template_id": playbook_id,
        "template_scope": playbook_type,
        "default_schedule_id": playbook_id,
        "default_schedule_scope": playbook_type,
    }

    runtime_records: list[dict[str, object]] = []
    for index, account in enumerate(target_accounts):
        runtime_records.append(
            {
                "canonical_organization_key": account["domain"],
                "source_id": account["account_id"],
                "opportunity_rank": index + 1,
            }
        )

    if execution_mode == "sequential":
        scheduled_batches = [[record] for record in runtime_records]
    else:
        scheduled_batches = [runtime_records]

    return {
        "runtime_template": deepcopy(runtime_template),
        "scheduled_batches": deepcopy(scheduled_batches),
    }
