"""
Test suite for D12-MB1 CI File Format Validator.
"""

from __future__ import annotations

import sys
from copy import deepcopy
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
CI_SCRIPTS_ROOT = REPO_ROOT / "scripts" / "ci"

if str(CI_SCRIPTS_ROOT) not in sys.path:
    sys.path.insert(0, str(CI_SCRIPTS_ROOT))

from file_format_validator import validate_delivery  # noqa: E402


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_REQUIRED = frozenset({
    "src/fleetgraph_core/runtime/runtime_http_api.py",
    "tests/runtime/test_runtime_http_api.py",
})

_ALLOWED = frozenset({
    "src/fleetgraph_core/runtime/runtime_http_api.py",
    "tests/runtime/test_runtime_http_api.py",
    "src/fleetgraph_core/runtime/runtime_readiness_layer.py",
    "tests/runtime/test_runtime_readiness_layer.py",
})

_VALID_CONTENT = "# non-empty content\n"


def make_valid_delivery() -> dict[str, str]:
    return {path: _VALID_CONTENT for path in _ALLOWED}


# ---------------------------------------------------------------------------
# Pass path
# ---------------------------------------------------------------------------


class TestPassPath:
    def test_valid_delivery_passes(self) -> None:
        result = validate_delivery(make_valid_delivery(), _REQUIRED, _ALLOWED)

        assert result["status"] == "pass"
        assert result["errors"] == []

    def test_required_subset_delivery_passes(self) -> None:
        delivery = {path: _VALID_CONTENT for path in _REQUIRED}

        result = validate_delivery(delivery, _REQUIRED, _ALLOWED)

        assert result["status"] == "pass"
        assert result["errors"] == []


# ---------------------------------------------------------------------------
# Missing required file
# ---------------------------------------------------------------------------


class TestMissingRequiredFile:
    def test_missing_one_required_file_fails(self) -> None:
        delivery = make_valid_delivery()
        del delivery["src/fleetgraph_core/runtime/runtime_http_api.py"]

        result = validate_delivery(delivery, _REQUIRED, _ALLOWED)

        assert result["status"] == "fail"
        assert any(
            "missing required file" in e and "runtime_http_api.py" in e
            for e in result["errors"]
        )

    def test_missing_all_required_files_fails(self) -> None:
        result = validate_delivery({}, _REQUIRED, _ALLOWED)

        assert result["status"] == "fail"
        missing_errors = [e for e in result["errors"] if "missing required file" in e]
        assert len(missing_errors) == len(_REQUIRED)

    def test_missing_error_message_contains_path(self) -> None:
        delivery = make_valid_delivery()
        target = "tests/runtime/test_runtime_http_api.py"
        del delivery[target]

        result = validate_delivery(delivery, _REQUIRED, _ALLOWED)

        assert any(target in e for e in result["errors"])


# ---------------------------------------------------------------------------
# Unexpected extra file
# ---------------------------------------------------------------------------


class TestUnexpectedFile:
    def test_extra_file_outside_allowed_set_fails(self) -> None:
        delivery = make_valid_delivery()
        delivery["src/fleetgraph_core/runtime/sneaky_module.py"] = _VALID_CONTENT

        result = validate_delivery(delivery, _REQUIRED, _ALLOWED)

        assert result["status"] == "fail"
        assert any("unexpected file" in e and "sneaky_module.py" in e for e in result["errors"])

    def test_multiple_extra_files_all_reported(self) -> None:
        delivery = make_valid_delivery()
        delivery["scripts/ci/extra_one.py"] = _VALID_CONTENT
        delivery["scripts/ci/extra_two.py"] = _VALID_CONTENT

        result = validate_delivery(delivery, _REQUIRED, _ALLOWED)

        unexpected_errors = [e for e in result["errors"] if "unexpected file" in e]
        assert len(unexpected_errors) == 2


# ---------------------------------------------------------------------------
# Invalid file path
# ---------------------------------------------------------------------------


class TestInvalidFilePath:
    def test_path_with_parent_traversal_fails(self) -> None:
        delivery = {
            "src/../secret.py": _VALID_CONTENT,
            **{p: _VALID_CONTENT for p in _REQUIRED},
        }

        result = validate_delivery(delivery, _REQUIRED, _ALLOWED)

        assert result["status"] == "fail"
        assert any("invalid file path" in e for e in result["errors"])

    def test_absolute_path_fails(self) -> None:
        delivery = {
            "/etc/passwd": _VALID_CONTENT,
            **{p: _VALID_CONTENT for p in _REQUIRED},
        }

        result = validate_delivery(delivery, _REQUIRED, _ALLOWED)

        assert result["status"] == "fail"
        assert any("invalid file path" in e and "/etc/passwd" in e for e in result["errors"])

    def test_path_with_null_byte_fails(self) -> None:
        delivery = {
            "src/evil\x00file.py": _VALID_CONTENT,
            **{p: _VALID_CONTENT for p in _REQUIRED},
        }

        result = validate_delivery(delivery, _REQUIRED, _ALLOWED)

        assert result["status"] == "fail"
        assert any("invalid file path" in e for e in result["errors"])

    def test_dot_segment_path_fails(self) -> None:
        delivery = {
            "src/./module.py": _VALID_CONTENT,
            **{p: _VALID_CONTENT for p in _REQUIRED},
        }

        result = validate_delivery(delivery, _REQUIRED, _ALLOWED)

        assert result["status"] == "fail"
        assert any("invalid file path" in e for e in result["errors"])


# ---------------------------------------------------------------------------
# Empty file
# ---------------------------------------------------------------------------


class TestEmptyFile:
    def test_file_with_empty_string_fails(self) -> None:
        delivery = {**make_valid_delivery()}
        delivery["src/fleetgraph_core/runtime/runtime_http_api.py"] = ""

        result = validate_delivery(delivery, _REQUIRED, _ALLOWED)

        assert result["status"] == "fail"
        assert any(
            "empty file" in e and "runtime_http_api.py" in e for e in result["errors"]
        )

    def test_file_with_only_whitespace_fails(self) -> None:
        delivery = {**make_valid_delivery()}
        delivery["src/fleetgraph_core/runtime/runtime_http_api.py"] = "   \n\t  "

        result = validate_delivery(delivery, _REQUIRED, _ALLOWED)

        assert result["status"] == "fail"
        assert any("empty file" in e for e in result["errors"])

    def test_file_with_content_passes(self) -> None:
        delivery = {path: "# content\n" for path in _REQUIRED}

        result = validate_delivery(delivery, _REQUIRED, _ALLOWED)

        assert result["status"] == "pass"


# ---------------------------------------------------------------------------
# Output contract
# ---------------------------------------------------------------------------


class TestOutputContract:
    def test_fixed_top_level_key_order(self) -> None:
        result = validate_delivery(make_valid_delivery(), _REQUIRED, _ALLOWED)

        assert tuple(result.keys()) == ("status", "errors")

    def test_fixed_key_order_on_failure(self) -> None:
        result = validate_delivery({}, _REQUIRED, _ALLOWED)

        assert tuple(result.keys()) == ("status", "errors")

    def test_errors_is_list(self) -> None:
        result = validate_delivery(make_valid_delivery(), _REQUIRED, _ALLOWED)

        assert isinstance(result["errors"], list)

    def test_errors_list_is_empty_on_pass(self) -> None:
        result = validate_delivery(make_valid_delivery(), _REQUIRED, _ALLOWED)

        assert result["errors"] == []

    def test_status_values_are_bounded_strings(self) -> None:
        pass_result = validate_delivery(make_valid_delivery(), _REQUIRED, _ALLOWED)
        fail_result = validate_delivery({}, _REQUIRED, _ALLOWED)

        assert pass_result["status"] == "pass"
        assert fail_result["status"] == "fail"


# ---------------------------------------------------------------------------
# Determinism
# ---------------------------------------------------------------------------


class TestDeterminism:
    def test_same_valid_delivery_produces_same_result(self) -> None:
        delivery = make_valid_delivery()

        first = validate_delivery(delivery, _REQUIRED, _ALLOWED)
        second = validate_delivery(delivery, _REQUIRED, _ALLOWED)

        assert first == second

    def test_same_failing_delivery_produces_same_result(self) -> None:
        delivery: dict[str, str] = {}

        first = validate_delivery(delivery, _REQUIRED, _ALLOWED)
        second = validate_delivery(delivery, _REQUIRED, _ALLOWED)

        assert first == second

    def test_error_list_is_sorted_deterministically(self) -> None:
        delivery: dict[str, str] = {}

        result = validate_delivery(delivery, _REQUIRED, _ALLOWED)

        assert result["errors"] == sorted(result["errors"])


# ---------------------------------------------------------------------------
# Input immutability
# ---------------------------------------------------------------------------


class TestInputImmutability:
    def test_delivery_dict_not_mutated(self) -> None:
        delivery = make_valid_delivery()
        delivery_before = deepcopy(delivery)

        validate_delivery(delivery, _REQUIRED, _ALLOWED)

        assert delivery == delivery_before

    def test_required_files_not_mutated(self) -> None:
        required = frozenset(_REQUIRED)

        validate_delivery(make_valid_delivery(), required, _ALLOWED)

        assert required == _REQUIRED

    def test_allowed_files_not_mutated(self) -> None:
        allowed = frozenset(_ALLOWED)

        validate_delivery(make_valid_delivery(), _REQUIRED, allowed)

        assert allowed == _ALLOWED


# ---------------------------------------------------------------------------
# Input type validation
# ---------------------------------------------------------------------------


class TestInputTypeValidation:
    def test_non_dict_delivery_raises(self) -> None:
        with pytest.raises(TypeError, match="delivery must be a dict"):
            validate_delivery("not a dict", _REQUIRED, _ALLOWED)  # type: ignore[arg-type]

    def test_non_frozenset_required_raises(self) -> None:
        with pytest.raises(TypeError, match="required_files must be a frozenset"):
            validate_delivery(make_valid_delivery(), set(_REQUIRED), _ALLOWED)  # type: ignore[arg-type]

    def test_non_frozenset_allowed_raises(self) -> None:
        with pytest.raises(TypeError, match="allowed_files must be a frozenset"):
            validate_delivery(make_valid_delivery(), _REQUIRED, list(_ALLOWED))  # type: ignore[arg-type]
