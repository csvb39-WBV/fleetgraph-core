"""
Test suite for MB6-A Configuration / Template Layer.

Validates:
- Closed-schema validation of runtime_template (missing/extra/wrong-type fields,
  empty and whitespace-only strings)
- scheduled_batches shape validation (non-list, non-list batch, non-dict record)
- Core behavior: correct field mapping, exact output schema, order preservation
- Empty batch list allowed
- Input safety: template and batches not mutated; returned batches are deep copies
"""

from copy import deepcopy

import pytest

from fleetgraph_core.runtime.runtime_configuration_layer import (
    build_schedule_request_from_template,
    validate_runtime_template,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def make_template(
    template_id: str = "tmpl_001",
    template_scope: str = "test_scope",
    default_schedule_id: str = "sched_001",
    default_schedule_scope: str = "sched_scope",
) -> dict:
    return {
        "template_id": template_id,
        "template_scope": template_scope,
        "default_schedule_id": default_schedule_id,
        "default_schedule_scope": default_schedule_scope,
    }


def make_record(org_key: str = "org_a", source_id: str = "src_1", rank: int = 1) -> dict:
    return {
        "canonical_organization_key": org_key,
        "source_id": source_id,
        "opportunity_rank": rank,
    }


# ---------------------------------------------------------------------------
# Template validation — validate_runtime_template
# ---------------------------------------------------------------------------


class TestValidateRuntimeTemplate:
    """Closed-schema validation of runtime_template."""

    def test_non_dict_template_rejected(self):
        with pytest.raises(TypeError, match="runtime_template must be a dict"):
            validate_runtime_template("not a dict")

        with pytest.raises(TypeError, match="runtime_template must be a dict"):
            validate_runtime_template(["tmpl", "scope"])

    def test_missing_template_id_rejected(self):
        tmpl = make_template()
        del tmpl["template_id"]

        with pytest.raises(ValueError, match="template_id"):
            validate_runtime_template(tmpl)

    def test_missing_template_scope_rejected(self):
        tmpl = make_template()
        del tmpl["template_scope"]

        with pytest.raises(ValueError, match="template_scope"):
            validate_runtime_template(tmpl)

    def test_missing_default_schedule_id_rejected(self):
        tmpl = make_template()
        del tmpl["default_schedule_id"]

        with pytest.raises(ValueError, match="default_schedule_id"):
            validate_runtime_template(tmpl)

    def test_missing_default_schedule_scope_rejected(self):
        tmpl = make_template()
        del tmpl["default_schedule_scope"]

        with pytest.raises(ValueError, match="default_schedule_scope"):
            validate_runtime_template(tmpl)

    def test_extra_field_rejected(self):
        tmpl = make_template()
        tmpl["extra_field"] = "not_allowed"

        with pytest.raises(ValueError, match="unexpected fields"):
            validate_runtime_template(tmpl)

    def test_wrong_type_template_id_rejected(self):
        tmpl = make_template()
        tmpl["template_id"] = 123

        with pytest.raises(TypeError, match="template_id"):
            validate_runtime_template(tmpl)

    def test_wrong_type_template_scope_rejected(self):
        tmpl = make_template()
        tmpl["template_scope"] = 999

        with pytest.raises(TypeError, match="template_scope"):
            validate_runtime_template(tmpl)

    def test_wrong_type_default_schedule_id_rejected(self):
        tmpl = make_template()
        tmpl["default_schedule_id"] = []

        with pytest.raises(TypeError, match="default_schedule_id"):
            validate_runtime_template(tmpl)

    def test_wrong_type_default_schedule_scope_rejected(self):
        tmpl = make_template()
        tmpl["default_schedule_scope"] = {}

        with pytest.raises(TypeError, match="default_schedule_scope"):
            validate_runtime_template(tmpl)

    def test_empty_template_id_rejected(self):
        tmpl = make_template(template_id="")

        with pytest.raises(ValueError, match="template_id"):
            validate_runtime_template(tmpl)

    def test_whitespace_only_template_id_rejected(self):
        tmpl = make_template(template_id="   ")

        with pytest.raises(ValueError, match="template_id"):
            validate_runtime_template(tmpl)

    def test_empty_template_scope_rejected(self):
        tmpl = make_template(template_scope="")

        with pytest.raises(ValueError, match="template_scope"):
            validate_runtime_template(tmpl)

    def test_whitespace_only_template_scope_rejected(self):
        tmpl = make_template(template_scope="\t")

        with pytest.raises(ValueError, match="template_scope"):
            validate_runtime_template(tmpl)

    def test_empty_default_schedule_id_rejected(self):
        tmpl = make_template(default_schedule_id="")

        with pytest.raises(ValueError, match="default_schedule_id"):
            validate_runtime_template(tmpl)

    def test_whitespace_only_default_schedule_id_rejected(self):
        tmpl = make_template(default_schedule_id="  ")

        with pytest.raises(ValueError, match="default_schedule_id"):
            validate_runtime_template(tmpl)

    def test_empty_default_schedule_scope_rejected(self):
        tmpl = make_template(default_schedule_scope="")

        with pytest.raises(ValueError, match="default_schedule_scope"):
            validate_runtime_template(tmpl)

    def test_whitespace_only_default_schedule_scope_rejected(self):
        tmpl = make_template(default_schedule_scope="\n")

        with pytest.raises(ValueError, match="default_schedule_scope"):
            validate_runtime_template(tmpl)

    def test_valid_template_passes_without_error(self):
        tmpl = make_template()
        validate_runtime_template(tmpl)  # must not raise


# ---------------------------------------------------------------------------
# Batch validation — build_schedule_request_from_template
# ---------------------------------------------------------------------------


class TestBatchValidation:
    """scheduled_batches shape validation."""

    def test_non_list_scheduled_batches_rejected(self):
        tmpl = make_template()

        with pytest.raises(TypeError, match="scheduled_batches must be a list"):
            build_schedule_request_from_template(tmpl, {"not": "a list"})

    def test_non_list_batch_entry_rejected(self):
        tmpl = make_template()

        with pytest.raises(TypeError, match=r"scheduled_batches\[0\] must be a list"):
            build_schedule_request_from_template(tmpl, [{"not": "a list"}])

    def test_non_dict_record_entry_rejected(self):
        tmpl = make_template()

        with pytest.raises(TypeError, match=r"scheduled_batches\[0\]\[0\] must be a dict"):
            build_schedule_request_from_template(tmpl, [["not a dict"]])

    def test_empty_scheduled_batches_allowed(self):
        tmpl = make_template()

        result = build_schedule_request_from_template(tmpl, [])

        assert result["scheduled_batches"] == []

    def test_empty_inner_batch_allowed(self):
        tmpl = make_template()

        result = build_schedule_request_from_template(tmpl, [[]])

        assert result["scheduled_batches"] == [[]]


# ---------------------------------------------------------------------------
# Core behavior
# ---------------------------------------------------------------------------


class TestCoreBehavior:
    """Nominal schedule_request construction."""

    def test_valid_template_and_batches_produce_schedule_request(self):
        tmpl = make_template()
        batches = [[make_record()]]

        result = build_schedule_request_from_template(tmpl, batches)

        assert isinstance(result, dict)

    def test_output_has_exact_required_fields_only(self):
        tmpl = make_template()
        batches = [[make_record()]]

        result = build_schedule_request_from_template(tmpl, batches)

        assert set(result.keys()) == {"schedule_id", "schedule_scope", "scheduled_batches"}

    def test_schedule_id_mapped_from_default_schedule_id(self):
        tmpl = make_template(default_schedule_id="my_schedule")

        result = build_schedule_request_from_template(tmpl, [])

        assert result["schedule_id"] == "my_schedule"

    def test_schedule_scope_mapped_from_default_schedule_scope(self):
        tmpl = make_template(default_schedule_scope="my_scope")

        result = build_schedule_request_from_template(tmpl, [])

        assert result["schedule_scope"] == "my_scope"

    def test_batch_order_preserved(self):
        tmpl = make_template()
        batch1 = [make_record(org_key="org_a")]
        batch2 = [make_record(org_key="org_b")]
        batch3 = [make_record(org_key="org_c")]
        batches = [batch1, batch2, batch3]

        result = build_schedule_request_from_template(tmpl, batches)

        assert result["scheduled_batches"][0][0]["canonical_organization_key"] == "org_a"
        assert result["scheduled_batches"][1][0]["canonical_organization_key"] == "org_b"
        assert result["scheduled_batches"][2][0]["canonical_organization_key"] == "org_c"

    def test_record_order_preserved_within_batch(self):
        tmpl = make_template()
        batch = [
            make_record(org_key="org_first"),
            make_record(org_key="org_second"),
            make_record(org_key="org_third"),
        ]

        result = build_schedule_request_from_template(tmpl, [batch])

        records = result["scheduled_batches"][0]
        assert records[0]["canonical_organization_key"] == "org_first"
        assert records[1]["canonical_organization_key"] == "org_second"
        assert records[2]["canonical_organization_key"] == "org_third"

    def test_empty_batch_list_produces_valid_empty_schedule_request(self):
        tmpl = make_template(
            default_schedule_id="empty_sched",
            default_schedule_scope="empty_scope",
        )

        result = build_schedule_request_from_template(tmpl, [])

        assert result["schedule_id"] == "empty_sched"
        assert result["schedule_scope"] == "empty_scope"
        assert result["scheduled_batches"] == []

    def test_multiple_batches_with_multiple_records(self):
        tmpl = make_template()
        batches = [
            [make_record(org_key="a1"), make_record(org_key="a2")],
            [make_record(org_key="b1")],
        ]

        result = build_schedule_request_from_template(tmpl, batches)

        assert len(result["scheduled_batches"]) == 2
        assert len(result["scheduled_batches"][0]) == 2
        assert len(result["scheduled_batches"][1]) == 1


# ---------------------------------------------------------------------------
# Safety
# ---------------------------------------------------------------------------


class TestSafety:
    """Input mutation safety and deep-copy guarantees."""

    def test_template_not_mutated(self):
        tmpl = make_template()
        original_copy = deepcopy(tmpl)

        build_schedule_request_from_template(tmpl, [[make_record()]])

        assert tmpl == original_copy

    def test_scheduled_batches_not_mutated(self):
        tmpl = make_template()
        batches = [[make_record(org_key="org_a")], [make_record(org_key="org_b")]]
        original_copy = deepcopy(batches)

        build_schedule_request_from_template(tmpl, batches)

        assert batches == original_copy

    def test_returned_batches_are_deep_copies_not_shared_references(self):
        tmpl = make_template()
        record = make_record(org_key="original")
        batches = [[record]]

        result = build_schedule_request_from_template(tmpl, batches)

        # Mutate the original record after the call
        record["canonical_organization_key"] = "mutated"

        # Result should be unaffected
        assert result["scheduled_batches"][0][0]["canonical_organization_key"] == "original"

    def test_returned_batch_list_not_same_reference_as_input(self):
        tmpl = make_template()
        batches = [[make_record()]]

        result = build_schedule_request_from_template(tmpl, batches)

        assert result["scheduled_batches"] is not batches

    def test_validate_runtime_template_does_not_mutate_input(self):
        tmpl = make_template()
        original_copy = deepcopy(tmpl)

        validate_runtime_template(tmpl)

        assert tmpl == original_copy
