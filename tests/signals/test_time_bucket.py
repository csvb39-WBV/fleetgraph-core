from __future__ import annotations

from datetime import date

import pytest

from fleetgraph.signals.time_bucket import assign_signal_bucket


REFERENCE_DATE = date(2026, 4, 11)


@pytest.mark.parametrize(
    ("days_old", "expected_bucket"),
    [
        (7, "T1"),
        (8, "T2"),
        (30, "T2"),
        (31, "T3A"),
        (365, "T3A"),
        (366, "T3B"),
        (730, "T3B"),
        (731, "T3C"),
        (1095, "T3C"),
        (1096, None),
    ],
)
def test_bucket_boundaries(days_old: int, expected_bucket: str | None) -> None:
    event_date = REFERENCE_DATE.fromordinal(REFERENCE_DATE.toordinal() - days_old)

    assert assign_signal_bucket(event_date, REFERENCE_DATE) == expected_bucket


def test_bucket_assignment_rejects_future_dates() -> None:
    future_date = REFERENCE_DATE.fromordinal(REFERENCE_DATE.toordinal() + 1)

    with pytest.raises(ValueError, match="event_date cannot be in the future"):
        assign_signal_bucket(future_date, REFERENCE_DATE)
