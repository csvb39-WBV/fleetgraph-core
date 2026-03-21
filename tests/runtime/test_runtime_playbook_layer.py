"""
Test suite for MB8 Fleet Sales Playbook Layer.
"""

from copy import deepcopy

import pytest

from fleetgraph_core.runtime.runtime_playbook_layer import (
    build_runtime_inputs_from_playbook,
    validate_fleet_playbook,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def make_playbook(
    playbook_id: str = "pb_001",
    playbook_type: str = "fleet_outbound",
    customer_type: str = "upfitter",
    execution_mode: str = "sequential",
) -> dict:
    return {
        "playbook_id": playbook_id,
        "playbook_type": playbook_type,
        "customer_type": customer_type,
        "execution_mode": execution_mode,
    }


def make_target_account(
    account_id: str = "acct_001",
    organization_name: str = "Acme Fleet",
    domain: str = "acme.com",
) -> dict:
    return {
        "account_id": account_id,
        "organization_name": organization_name,
        "domain": domain,
    }


# ---------------------------------------------------------------------------
# Playbook validation
# ---------------------------------------------------------------------------


class TestPlaybookValidation:
    def test_non_dict_playbook_rejected(self):
        with pytest.raises(TypeError, match="playbook must be a dict"):
            validate_fleet_playbook("not a dict")

    def test_missing_fields_rejected(self):
        for field in ["playbook_id", "playbook_type", "customer_type", "execution_mode"]:
            playbook = make_playbook()
            del playbook[field]
            with pytest.raises(ValueError, match=field):
                validate_fleet_playbook(playbook)

    def test_extra_fields_rejected(self):
        playbook = make_playbook()
        playbook["extra"] = "x"

        with pytest.raises(ValueError, match="unexpected fields"):
            validate_fleet_playbook(playbook)

    def test_wrong_type_fields_rejected(self):
        playbook = make_playbook(playbook_id=123)
        with pytest.raises(TypeError, match="playbook_id"):
            validate_fleet_playbook(playbook)

        playbook = make_playbook(playbook_type=123)
        with pytest.raises(TypeError, match="playbook_type"):
            validate_fleet_playbook(playbook)

        playbook = make_playbook(customer_type=123)
        with pytest.raises(TypeError, match="customer_type"):
            validate_fleet_playbook(playbook)

        playbook = make_playbook(execution_mode=123)
        with pytest.raises(TypeError, match="execution_mode"):
            validate_fleet_playbook(playbook)

    def test_empty_string_fields_rejected(self):
        playbook = make_playbook(playbook_id="")
        with pytest.raises(ValueError, match="playbook_id"):
            validate_fleet_playbook(playbook)

        playbook = make_playbook(playbook_type="   ")
        with pytest.raises(ValueError, match="playbook_type"):
            validate_fleet_playbook(playbook)

        playbook = make_playbook(customer_type="\t")
        with pytest.raises(ValueError, match="customer_type"):
            validate_fleet_playbook(playbook)

        playbook = make_playbook(execution_mode="\n")
        with pytest.raises(ValueError, match="execution_mode"):
            validate_fleet_playbook(playbook)

    def test_invalid_playbook_type_enum_rejected(self):
        playbook = make_playbook(playbook_type="not_allowed")

        with pytest.raises(ValueError, match="playbook_type"):
            validate_fleet_playbook(playbook)

    def test_invalid_execution_mode_enum_rejected(self):
        playbook = make_playbook(execution_mode="batched")

        with pytest.raises(ValueError, match="execution_mode"):
            validate_fleet_playbook(playbook)

    def test_valid_playbook_passes(self):
        validate_fleet_playbook(make_playbook())


# ---------------------------------------------------------------------------
# Target account validation
# ---------------------------------------------------------------------------


class TestTargetAccountValidation:
    def test_non_list_target_accounts_rejected(self):
        with pytest.raises(TypeError, match="target_accounts must be a list"):
            build_runtime_inputs_from_playbook(make_playbook(), {"not": "a list"})

    def test_non_dict_account_rejected(self):
        with pytest.raises(TypeError, match=r"target_accounts\[0\] must be a dict"):
            build_runtime_inputs_from_playbook(make_playbook(), ["not a dict"])

    def test_missing_account_fields_rejected(self):
        for field in ["account_id", "organization_name", "domain"]:
            account = make_target_account()
            del account[field]
            with pytest.raises(ValueError, match=field):
                build_runtime_inputs_from_playbook(make_playbook(), [account])

    def test_extra_account_fields_rejected(self):
        account = make_target_account()
        account["extra"] = "x"

        with pytest.raises(ValueError, match="unexpected fields"):
            build_runtime_inputs_from_playbook(make_playbook(), [account])

    def test_empty_account_fields_rejected(self):
        account = make_target_account(account_id="")
        with pytest.raises(ValueError, match="account_id"):
            build_runtime_inputs_from_playbook(make_playbook(), [account])

        account = make_target_account(organization_name="   ")
        with pytest.raises(ValueError, match="organization_name"):
            build_runtime_inputs_from_playbook(make_playbook(), [account])

        account = make_target_account(domain="\t")
        with pytest.raises(ValueError, match="domain"):
            build_runtime_inputs_from_playbook(make_playbook(), [account])


# ---------------------------------------------------------------------------
# Core behavior
# ---------------------------------------------------------------------------


class TestCoreBehavior:
    def test_sequential_mode_one_batch_per_account(self):
        playbook = make_playbook(execution_mode="sequential")
        accounts = [
            make_target_account(account_id="a1", domain="a.com"),
            make_target_account(account_id="a2", domain="b.com"),
            make_target_account(account_id="a3", domain="c.com"),
        ]

        result = build_runtime_inputs_from_playbook(playbook, accounts)

        assert len(result["scheduled_batches"]) == 3
        assert len(result["scheduled_batches"][0]) == 1
        assert len(result["scheduled_batches"][1]) == 1
        assert len(result["scheduled_batches"][2]) == 1

    def test_parallel_mode_single_batch(self):
        playbook = make_playbook(execution_mode="parallel")
        accounts = [
            make_target_account(account_id="a1", domain="a.com"),
            make_target_account(account_id="a2", domain="b.com"),
            make_target_account(account_id="a3", domain="c.com"),
        ]

        result = build_runtime_inputs_from_playbook(playbook, accounts)

        assert len(result["scheduled_batches"]) == 1
        assert len(result["scheduled_batches"][0]) == 3

    def test_runtime_record_mapping_is_correct(self):
        playbook = make_playbook(execution_mode="parallel")
        accounts = [
            make_target_account(account_id="acct_1", domain="dom1.com"),
            make_target_account(account_id="acct_2", domain="dom2.com"),
        ]

        result = build_runtime_inputs_from_playbook(playbook, accounts)
        records = result["scheduled_batches"][0]

        assert records[0] == {
            "canonical_organization_key": "dom1.com",
            "source_id": "acct_1",
            "opportunity_rank": 1,
        }
        assert records[1] == {
            "canonical_organization_key": "dom2.com",
            "source_id": "acct_2",
            "opportunity_rank": 2,
        }

    def test_opportunity_rank_assignment_deterministic_by_index(self):
        playbook = make_playbook(execution_mode="parallel")
        accounts = [
            make_target_account(account_id="acct_a", domain="a.com"),
            make_target_account(account_id="acct_b", domain="b.com"),
            make_target_account(account_id="acct_c", domain="c.com"),
        ]

        result = build_runtime_inputs_from_playbook(playbook, accounts)
        records = result["scheduled_batches"][0]

        assert records[0]["opportunity_rank"] == 1
        assert records[1]["opportunity_rank"] == 2
        assert records[2]["opportunity_rank"] == 3

    def test_runtime_template_mapping_is_correct(self):
        playbook = make_playbook(
            playbook_id="pb_special",
            playbook_type="fleet_expansion",
            execution_mode="parallel",
        )

        result = build_runtime_inputs_from_playbook(playbook, [make_target_account()])

        assert result["runtime_template"] == {
            "template_id": "pb_special",
            "template_scope": "fleet_expansion",
            "default_schedule_id": "pb_special",
            "default_schedule_scope": "fleet_expansion",
        }

    def test_output_has_exact_fields_only(self):
        result = build_runtime_inputs_from_playbook(make_playbook(), [make_target_account()])

        assert set(result.keys()) == {"runtime_template", "scheduled_batches"}
        assert set(result["runtime_template"].keys()) == {
            "template_id",
            "template_scope",
            "default_schedule_id",
            "default_schedule_scope",
        }

    def test_preserves_input_order_exactly(self):
        playbook = make_playbook(execution_mode="parallel")
        accounts = [
            make_target_account(account_id="acct_1", domain="one.com"),
            make_target_account(account_id="acct_2", domain="two.com"),
            make_target_account(account_id="acct_3", domain="three.com"),
        ]

        result = build_runtime_inputs_from_playbook(playbook, accounts)
        records = result["scheduled_batches"][0]

        assert records[0]["source_id"] == "acct_1"
        assert records[1]["source_id"] == "acct_2"
        assert records[2]["source_id"] == "acct_3"


# ---------------------------------------------------------------------------
# Safety
# ---------------------------------------------------------------------------


class TestSafety:
    def test_playbook_not_mutated(self):
        playbook = make_playbook()
        playbook_before = deepcopy(playbook)

        build_runtime_inputs_from_playbook(playbook, [make_target_account()])

        assert playbook == playbook_before

    def test_target_accounts_not_mutated(self):
        accounts = [
            make_target_account(account_id="acct_1", domain="one.com"),
            make_target_account(account_id="acct_2", domain="two.com"),
        ]
        accounts_before = deepcopy(accounts)

        build_runtime_inputs_from_playbook(make_playbook(), accounts)

        assert accounts == accounts_before

    def test_output_is_deep_copy(self):
        playbook = make_playbook(execution_mode="parallel")
        accounts = [make_target_account(account_id="acct_1", domain="one.com")]

        result = build_runtime_inputs_from_playbook(playbook, accounts)

        # Mutate inputs after call; output must remain unchanged
        playbook["playbook_id"] = "mutated"
        accounts[0]["account_id"] = "mutated_acct"
        accounts[0]["domain"] = "mutated.com"

        assert result["runtime_template"]["template_id"] == "pb_001"
        assert result["scheduled_batches"][0][0]["source_id"] == "acct_1"
        assert result["scheduled_batches"][0][0]["canonical_organization_key"] == "one.com"
