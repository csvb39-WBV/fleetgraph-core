"""
Execution registry for MB1 runtime coordination.

Provides explicit caller-managed tracking of completed runtime executions.
The runtime layer queries this object for duplicate detection, while the
caller decides when to register successful runs.
"""


class ExecutionRegistry:
    """Track completed runtime run_ids for explicit duplicate detection."""

    def __init__(self, initial_run_ids: set[str] | None = None) -> None:
        if initial_run_ids is not None and not isinstance(initial_run_ids, set):
            raise TypeError("initial_run_ids must be a set[str] or None")

        if initial_run_ids is None:
            self._executed_run_ids: set[str] = set()
        else:
            invalid_run_ids = [run_id for run_id in initial_run_ids if not isinstance(run_id, str)]
            if invalid_run_ids:
                raise TypeError("initial_run_ids must contain only strings")
            self._executed_run_ids = set(initial_run_ids)

    def has_run(self, run_id: str) -> bool:
        """Return whether the given run_id has already been registered."""
        if not isinstance(run_id, str):
            raise TypeError("run_id must be a string")
        return run_id in self._executed_run_ids

    def register_run(self, run_id: str) -> None:
        """Record a completed run_id in the registry."""
        if not isinstance(run_id, str):
            raise TypeError("run_id must be a string")
        self._executed_run_ids.add(run_id)

    def assert_not_executed(self, run_id: str) -> None:
        """Raise ValueError if the provided run_id has already been registered."""
        if not isinstance(run_id, str):
            raise TypeError("run_id must be a string")
        if run_id in self._executed_run_ids:
            raise ValueError(
                f"Duplicate execution detected. Run {run_id} has already been executed. "
                "Add run_id to execution registry only after successful execution."
            )

    def get_all_run_ids(self) -> set[str]:
        """Return a defensive copy of all registered run_ids."""
        return set(self._executed_run_ids)

    def __contains__(self, run_id: object) -> bool:
        return isinstance(run_id, str) and self.has_run(run_id)

    def __len__(self) -> int:
        return len(self._executed_run_ids)