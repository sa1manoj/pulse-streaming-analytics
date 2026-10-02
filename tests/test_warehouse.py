from datetime import datetime, timedelta, timezone

from pulse.warehouse import hours_since


def test_hours_since():
    now = datetime(2025, 1, 2, tzinfo=timezone.utc)
    assert hours_since(now - timedelta(hours=3), now) == 3


def test_no_data_is_infinitely_stale():
    assert hours_since(None) == float("inf")
