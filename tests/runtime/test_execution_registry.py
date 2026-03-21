"""Tests for the MB1 execution registry object."""

import pytest

from fleetgraph_core.runtime.execution_registry import ExecutionRegistry


class TestExecutionRegistry:
    """Test execution registry behavior."""

    def test_empty_registry_starts_empty(self):
        registry = ExecutionRegistry()

        assert len(registry) == 0
        assert registry.get_all_run_ids() == set()

    def test_registry_accepts_initial_run_ids(self):
        registry = ExecutionRegistry({"runtime:1:abc", "runtime:2:def"})

        assert len(registry) == 2
        assert registry.has_run("runtime:1:abc")
        assert registry.has_run("runtime:2:def")

    def test_register_adds_run_id(self):
        registry = ExecutionRegistry()

        registry.register_run("runtime:1:abc")

        assert registry.has_run("runtime:1:abc")
        assert "runtime:1:abc" in registry
        assert len(registry) == 1

    def test_get_all_run_ids_returns_copy(self):
        registry = ExecutionRegistry({"runtime:1:abc"})

        snapshot = registry.get_all_run_ids()
        snapshot.add("runtime:2:def")

        assert registry.get_all_run_ids() == {"runtime:1:abc"}

    def test_registering_same_run_id_twice_keeps_unique_membership(self):
        registry = ExecutionRegistry()

        registry.register_run("runtime:1:abc")
        registry.register_run("runtime:1:abc")

        assert len(registry) == 1

    def test_assert_not_executed_raises_value_error_on_duplicate(self):
        registry = ExecutionRegistry({"runtime:1:abc"})

        with pytest.raises(ValueError, match="Duplicate execution detected"):
            registry.assert_not_executed("runtime:1:abc")

    def test_assert_not_executed_allows_new_run_id(self):
        registry = ExecutionRegistry({"runtime:1:abc"})

        registry.assert_not_executed("runtime:2:def")

    def test_constructor_rejects_non_set_initial_run_ids(self):
        with pytest.raises(TypeError, match=r"initial_run_ids must be a set\[str\] or None"):
            ExecutionRegistry(["runtime:1:abc"])

    def test_constructor_rejects_non_string_run_ids(self):
        with pytest.raises(TypeError, match="initial_run_ids must contain only strings"):
            ExecutionRegistry({"runtime:1:abc", 123})

    def test_methods_reject_non_string_run_id(self):
        registry = ExecutionRegistry()

        with pytest.raises(TypeError, match="run_id must be a string"):
            registry.has_run(123)

        with pytest.raises(TypeError, match="run_id must be a string"):
            registry.register_run(123)

        with pytest.raises(TypeError, match="run_id must be a string"):
            registry.assert_not_executed(123)