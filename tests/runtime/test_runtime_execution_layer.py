"""
Test suite for MB1 Runtime Execution Layer.

Validates:
- Records format validation
- Deterministic run_id generation from full FG5-MB1 record set
- Required field validation for run_id construction
- Explicit duplicate rejection via caller-supplied execution registry
- Result envelope structure
- Error handling and propagation
- Caller-managed mandatory idempotency
"""

import pytest
from unittest.mock import patch, MagicMock
from copy import deepcopy

from fleetgraph_core.runtime.runtime_execution_layer import (
    validate_runtime_request,
    apply_runtime_execution,
    _build_run_id_from_records,
)


# Fixtures

@pytest.fixture
def valid_fg5_record():
    """Valid FG5-MB1 format record."""
    return {
        "canonical_organization_id": "org_123",
        "organization_domain_candidate_id": "odc_456",
        "organization_candidate_id": "oc_789",
        "candidate_id": "cand_101",
        "seed_id": "seed_202",
        "source_id": "src_303",
        "source_label": "LinkedIn",
        "canonical_organization_name": "Acme Corp",
        "canonical_organization_key": "acme_corp",
        "domain_candidate": "acme.com",
        "candidate_state": "canonicalized",
        "relevance_gate_outcome": "relevant",
        "opportunity_rank": 1,
        "contact_enrichment_request_id": "enrich_404",
        "contact_enrichment_request": {
            "request_type": "contact_enrichment",
            "canonical_organization_id": "org_123",
            "canonical_organization_key": "acme_corp",
            "canonical_organization_name": "Acme Corp",
            "domain_candidate": "acme.com",
            "source_id": "src_303",
            "opportunity_rank": 1,
        },
        "contact_coordination_state": "prepared",
    }


@pytest.fixture
def valid_fg5_records(valid_fg5_record):
    """Multiple valid FG5-MB1 records."""
    record1 = deepcopy(valid_fg5_record)
    record2 = deepcopy(valid_fg5_record)
    record2["source_id"] = "src_304"
    record2["source_label"] = "ZoomInfo"
    return [record1, record2]


@pytest.fixture
def empty_records():
    """Empty records list."""
    return []


def mock_workflow_result(input_count=2, output_count=2):
    """Create a mock workflow_result dict."""
    final_results = [{"id": str(i)} for i in range(output_count)]
    return {
        "workflow_run_id": "wf_123",
        "workflow_run_state": "completed",
        "stage_sequence": ["stage1", "stage2"],
        "stage_results": [
            {
                "stage_name": "stage1",
                "input_count": input_count,
                "output_count": output_count,
                "stage_outcome": "success",
                "output_state_field": "state",
            }
        ],
        "final_results": final_results,
        "input_record_count": input_count,
        "output_record_count": output_count,
    }


# ============================================================================
# VALIDATION TESTS
# ============================================================================


class TestValidationRequestShape:
    """Test request shape validation."""

    def test_records_not_list_raises_type_error(self):
        """Non-list records raises TypeError."""
        with pytest.raises(TypeError, match="records must be a list"):
            validate_runtime_request({"id": "1"})

        with pytest.raises(TypeError, match="records must be a list"):
            validate_runtime_request("not a list")

    def test_non_dict_record_entry_raises_type_error(self, valid_fg5_record):
        """Non-dict record in list raises TypeError."""
        records = [valid_fg5_record, "not a dict"]
        with pytest.raises(TypeError, match="Record at index 1 must be a dict"):
            validate_runtime_request(records)

    def test_all_record_entries_must_be_dicts(self):
        """All records must be dicts."""
        records = ["not a dict"]
        with pytest.raises(TypeError, match="Record at index 0 must be a dict"):
            validate_runtime_request(records)

    def test_empty_records_valid(self, empty_records):
        """Empty records list is valid format."""
        # Should not raise
        validate_runtime_request(empty_records)


# ============================================================================
# RUN_ID GENERATION TESTS
# ============================================================================


class TestRunIdGeneration:
    """Test deterministic run_id generation from full record set."""

    def test_run_id_deterministic_from_records(self, valid_fg5_records):
        """Same records produce same run_id."""
        run_id1 = _build_run_id_from_records(valid_fg5_records)
        run_id2 = _build_run_id_from_records(deepcopy(valid_fg5_records))

        assert run_id1 == run_id2

    def test_run_id_encodes_all_records(self, valid_fg5_record):
        """Run_id is derived from full record set, not just first/last."""
        record1 = deepcopy(valid_fg5_record)
        record2 = deepcopy(valid_fg5_record)
        record2["source_id"] = "src_different"
        records = [record1, record2]

        run_id = _build_run_id_from_records(records)

        # Should contain count
        assert "runtime:2:" in run_id

    def test_run_id_empty_records(self, empty_records):
        """Empty records produce special run_id."""
        run_id = _build_run_id_from_records(empty_records)

        assert run_id == "runtime:0:empty:empty"

    def test_run_id_single_record(self, valid_fg5_record):
        """Single record produces valid run_id."""
        records = [valid_fg5_record]
        run_id = _build_run_id_from_records(records)

        assert "runtime:1:" in run_id

    def test_different_records_produce_different_run_ids(self, valid_fg5_record):
        """Different records produce different run_ids."""
        records1 = [valid_fg5_record]
        record2 = deepcopy(valid_fg5_record)
        record2["source_id"] = "different_source"
        records2 = [record2]

        run_id1 = _build_run_id_from_records(records1)
        run_id2 = _build_run_id_from_records(records2)

        assert run_id1 != run_id2

    def test_reordered_records_produce_different_run_ids(self, valid_fg5_record):
        """Reordered records produce different run_ids (order matters)."""
        record1 = deepcopy(valid_fg5_record)
        record1["source_id"] = "src_aaa"
        record2 = deepcopy(valid_fg5_record)
        record2["source_id"] = "src_zzz"

        records_ordered = [record1, record2]
        records_reversed = [record2, record1]

        run_id_ordered = _build_run_id_from_records(records_ordered)
        run_id_reversed = _build_run_id_from_records(records_reversed)

        assert run_id_ordered != run_id_reversed


# ============================================================================
# RUN_ID FIELD VALIDATION TESTS
# ============================================================================


class TestRunIdFieldValidation:
    """Test validation of fields required for run_id construction."""

    def test_missing_canonical_organization_key_raises_error(self, valid_fg5_record):
        """Record missing canonical_organization_key raises ValueError."""
        record = deepcopy(valid_fg5_record)
        del record["canonical_organization_key"]
        records = [record]

        with pytest.raises(ValueError) as exc_info:
            _build_run_id_from_records(records)

        error_msg = str(exc_info.value)
        assert "canonical_organization_key" in error_msg
        assert "Record at index 0" in error_msg

    def test_missing_source_id_raises_error(self, valid_fg5_record):
        """Record missing source_id raises ValueError."""
        record = deepcopy(valid_fg5_record)
        del record["source_id"]
        records = [record]

        with pytest.raises(ValueError) as exc_info:
            _build_run_id_from_records(records)

        error_msg = str(exc_info.value)
        assert "source_id" in error_msg
        assert "Record at index 0" in error_msg

    def test_missing_opportunity_rank_raises_error(self, valid_fg5_record):
        """Record missing opportunity_rank raises ValueError."""
        record = deepcopy(valid_fg5_record)
        del record["opportunity_rank"]
        records = [record]

        with pytest.raises(ValueError) as exc_info:
            _build_run_id_from_records(records)

        error_msg = str(exc_info.value)
        assert "opportunity_rank" in error_msg
        assert "Record at index 0" in error_msg

    def test_missing_field_in_middle_record_raises_error(self, valid_fg5_record):
        """Missing field in non-first record is detected."""
        record1 = deepcopy(valid_fg5_record)
        record2 = deepcopy(valid_fg5_record)
        del record2["source_id"]
        records = [record1, record2]

        with pytest.raises(ValueError) as exc_info:
            _build_run_id_from_records(records)

        error_msg = str(exc_info.value)
        assert "Record at index 1" in error_msg
        assert "source_id" in error_msg


# ============================================================================
# CORE EXECUTION TESTS
# ============================================================================


class TestCoreExecution:
    """Test nominal runtime execution."""

    @patch(
        "fleetgraph_core.runtime.runtime_execution_layer.apply_workflow_run_director"
    )
    def test_nominal_execution_returns_result_dict(self, mock_workflow, valid_fg5_records):
        """Valid records produce result dict with all required fields."""
        mock_workflow.return_value = mock_workflow_result(2, 2)
        execution_registry: set[str] = set()

        result = apply_runtime_execution(valid_fg5_records, execution_registry)

        assert isinstance(result, dict)
        assert "run_id" in result
        assert "runtime_state" in result
        assert "workflow_result" in result
        assert "input_record_count" in result
        assert "output_record_count" in result

    @patch(
        "fleetgraph_core.runtime.runtime_execution_layer.apply_workflow_run_director"
    )
    def test_result_has_all_required_fields(self, mock_workflow, valid_fg5_records):
        """Result dict contains exact required keys."""
        mock_workflow.return_value = mock_workflow_result(2, 2)
        execution_registry: set[str] = set()

        result = apply_runtime_execution(valid_fg5_records, execution_registry)

        expected_keys = {
            "run_id",
            "runtime_state",
            "workflow_result",
            "input_record_count",
            "output_record_count",
        }
        assert set(result.keys()) == expected_keys

    @patch(
        "fleetgraph_core.runtime.runtime_execution_layer.apply_workflow_run_director"
    )
    def test_runtime_state_completed_on_success(self, mock_workflow, valid_fg5_records):
        """runtime_state == 'completed' on success."""
        mock_workflow.return_value = mock_workflow_result(2, 2)
        execution_registry: set[str] = set()

        result = apply_runtime_execution(valid_fg5_records, execution_registry)

        assert result["runtime_state"] == "completed"

    @patch(
        "fleetgraph_core.runtime.runtime_execution_layer.apply_workflow_run_director"
    )
    def test_input_record_count_correct(self, mock_workflow, valid_fg5_records):
        """input_record_count matches input records length."""
        mock_workflow.return_value = mock_workflow_result(2, 2)
        execution_registry: set[str] = set()

        result = apply_runtime_execution(valid_fg5_records, execution_registry)

        assert result["input_record_count"] == 2

    @patch(
        "fleetgraph_core.runtime.runtime_execution_layer.apply_workflow_run_director"
    )
    def test_output_record_count_from_final_results(self, mock_workflow, valid_fg5_records):
        """output_record_count derived from workflow_result['final_results']."""
        # Workflow returns 3 output records even though input has 2
        mock_workflow.return_value = mock_workflow_result(input_count=2, output_count=3)
        execution_registry: set[str] = set()

        result = apply_runtime_execution(valid_fg5_records, execution_registry)

        assert result["input_record_count"] == 2
        assert result["output_record_count"] == 3

    @patch(
        "fleetgraph_core.runtime.runtime_execution_layer.apply_workflow_run_director"
    )
    def test_workflow_result_embedded_exactly(self, mock_workflow, valid_fg5_records):
        """workflow_result field contains exact return value."""
        mock_result = mock_workflow_result(2, 2)
        mock_workflow.return_value = mock_result
        execution_registry: set[str] = set()

        result = apply_runtime_execution(valid_fg5_records, execution_registry)

        assert result["workflow_result"] == mock_result

    @patch(
        "fleetgraph_core.runtime.runtime_execution_layer.apply_workflow_run_director"
    )
    def test_empty_records_produces_result(self, mock_workflow, empty_records):
        """Empty records produce valid result."""
        mock_workflow.return_value = mock_workflow_result(input_count=0, output_count=0)
        execution_registry: set[str] = set()

        result = apply_runtime_execution(empty_records, execution_registry)

        assert result["input_record_count"] == 0
        assert result["output_record_count"] == 0
        assert isinstance(result, dict)


# ============================================================================
# DUPLICATE DETECTION TESTS
# ============================================================================


class TestDuplicateDetection:
    """Test explicit duplicate rejection with required execution registry."""

    @patch(
        "fleetgraph_core.runtime.runtime_execution_layer.apply_workflow_run_director"
    )
    def test_duplicate_run_id_raises_value_error(self, mock_workflow, valid_fg5_records):
        """Same run_id in registry raises ValueError."""
        mock_workflow.return_value = mock_workflow_result(2, 2)
        execution_registry: set[str] = set()

        # First execution succeeds
        result1 = apply_runtime_execution(valid_fg5_records, execution_registry)
        run_id = result1["run_id"]

        # Create new registry with this run_id already executed
        duplicate_registry = {run_id}

        # Second execution with same records and duplicate registry raises error
        with pytest.raises(ValueError, match="Duplicate execution detected"):
            apply_runtime_execution(deepcopy(valid_fg5_records), duplicate_registry)

    @patch(
        "fleetgraph_core.runtime.runtime_execution_layer.apply_workflow_run_director"
    )
    def test_duplicate_error_message_explicit(self, mock_workflow, valid_fg5_records):
        """ValueError message on duplicate is explicit."""
        mock_workflow.return_value = mock_workflow_result(2, 2)
        execution_registry: set[str] = set()

        result = apply_runtime_execution(valid_fg5_records, execution_registry)
        run_id = result["run_id"]

        duplicate_registry = {run_id}

        with pytest.raises(ValueError) as exc_info:
            apply_runtime_execution(deepcopy(valid_fg5_records), duplicate_registry)

        error_msg = str(exc_info.value)
        assert run_id in error_msg
        assert "already been executed" in error_msg
        assert "Duplicate execution detected" in error_msg

    @patch(
        "fleetgraph_core.runtime.runtime_execution_layer.apply_workflow_run_director"
    )
    def test_workflow_not_called_on_duplicate(self, mock_workflow, valid_fg5_records):
        """Workflow is NOT called when duplicate detected."""
        mock_workflow.return_value = mock_workflow_result(2, 2)
        execution_registry: set[str] = set()

        # First call
        result1 = apply_runtime_execution(valid_fg5_records, execution_registry)
        run_id = result1["run_id"]
        assert mock_workflow.call_count == 1

        # Prepare duplicate registry
        duplicate_registry = {run_id}

        # Second call with duplicate raises error before workflow
        with pytest.raises(ValueError, match="Duplicate execution detected"):
            apply_runtime_execution(deepcopy(valid_fg5_records), duplicate_registry)

        # Workflow should still be called only once (not called for duplicate attempt)
        assert mock_workflow.call_count == 1

    @patch(
        "fleetgraph_core.runtime.runtime_execution_layer.apply_workflow_run_director"
    )
    def test_caller_managed_registry_required(self, mock_workflow, valid_fg5_records):
        """execution_registry parameter is required (not optional)."""
        mock_workflow.return_value = mock_workflow_result(2, 2)

        execution_registry: set[str] = set()

        # First execution
        result1 = apply_runtime_execution(valid_fg5_records, execution_registry)
        execution_registry.add(result1["run_id"])

        # Second attempt with same records and updated registry raises error
        with pytest.raises(ValueError, match="Duplicate execution detected"):
            apply_runtime_execution(deepcopy(valid_fg5_records), execution_registry)

        # Workflow called only once
        assert mock_workflow.call_count == 1


# ============================================================================
# ERROR HANDLING TESTS
# ============================================================================


class TestErrorHandling:
    """Test error handling and propagation."""

    @patch(
        "fleetgraph_core.runtime.runtime_execution_layer.apply_workflow_run_director"
    )
    def test_workflow_exception_propagates(self, mock_workflow, valid_fg5_records):
        """If workflow raises, exception propagates."""
        mock_workflow.side_effect = RuntimeError("Workflow failed")
        execution_registry: set[str] = set()

        with pytest.raises(RuntimeError, match="Workflow failed"):
            apply_runtime_execution(valid_fg5_records, execution_registry)

    @patch(
        "fleetgraph_core.runtime.runtime_execution_layer.apply_workflow_run_director"
    )
    def test_workflow_exception_no_registry_update(self, mock_workflow, valid_fg5_records):
        """If workflow fails, caller should not add run_id to registry."""
        mock_workflow.side_effect = RuntimeError("Workflow failed")
        execution_registry: set[str] = set()

        # First attempt fails
        with pytest.raises(RuntimeError):
            apply_runtime_execution(valid_fg5_records, execution_registry)

        # Registry is still empty (caller hasn't added run_id)
        assert len(execution_registry) == 0

        # Reset mock and retry - should attempt workflow again
        mock_workflow.side_effect = None
        mock_workflow.return_value = mock_workflow_result(2, 2)

        result = apply_runtime_execution(deepcopy(valid_fg5_records), execution_registry)
        assert result["runtime_state"] == "completed"

        # Workflow called twice (second attempt succeeded)
        assert mock_workflow.call_count == 2


# ============================================================================
# INPUT SAFETY TESTS
# ============================================================================


class TestInputSafety:
    """Test that runtime layer doesn't mutate input data."""

    @patch(
        "fleetgraph_core.runtime.runtime_execution_layer.apply_workflow_run_director"
    )
    def test_input_records_not_mutated(self, mock_workflow, valid_fg5_records):
        """Verify input records are not mutated during execution."""
        mock_workflow.return_value = mock_workflow_result(2, 2)
        execution_registry: set[str] = set()

        # Deep copy original to compare
        records_copy = deepcopy(valid_fg5_records)

        apply_runtime_execution(valid_fg5_records, execution_registry)

        # Records should be identical
        assert valid_fg5_records == records_copy

    @patch(
        "fleetgraph_core.runtime.runtime_execution_layer.apply_workflow_run_director"
    )
    def test_caller_registry_updated_by_caller_not_layer(
        self, mock_workflow, valid_fg5_records
    ):
        """Caller owns registry updates; layer only checks and reports run_id."""
        mock_workflow.return_value = mock_workflow_result(2, 2)
        execution_registry: set[str] = set()

        result = apply_runtime_execution(valid_fg5_records, execution_registry)
        run_id = result["run_id"]

        # Registry still empty - layer doesn't update it
        assert len(execution_registry) == 0

        # Caller adds run_id to registry
        execution_registry.add(run_id)

        # Second attempt detects duplicate because caller added it
        with pytest.raises(ValueError, match="Duplicate execution"):
            apply_runtime_execution(deepcopy(valid_fg5_records), execution_registry)
