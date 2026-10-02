import json
from datetime import datetime, timezone

import pytest

from pulse.events import InvalidEvent, parse_event

GOOD = {
    "event_id": "e1",
    "user_id": "u000001",
    "session_id": "s1",
    "event_type": "purchase",
    "variant": "control",
    "product_id": "P001",
    "price": 19.99,
    "event_ts": "2025-01-01T10:00:00Z",
}


def test_valid_event_round_trips():
    ev = parse_event(json.dumps(GOOD))
    assert ev.event_ts == datetime(2025, 1, 1, 10, tzinfo=timezone.utc)
    assert parse_event(ev.to_json()) == ev


@pytest.mark.parametrize(
    "patch, reason",
    [
        ({"event_type": "hover"}, "unknown event_type"),
        ({"variant": "b"}, "unknown variant"),
        ({"price": None}, "positive price"),
        ({"price": 0}, "positive price"),
        ({"event_id": ""}, "missing fields"),
        ({"event_ts": "yesterday"}, "bad event_ts"),
    ],
)
def test_invalid_events_are_rejected_with_reason(patch, reason):
    with pytest.raises(InvalidEvent, match=reason):
        parse_event({**GOOD, **patch})


def test_non_json_is_rejected():
    with pytest.raises(InvalidEvent, match="not JSON"):
        parse_event("{oops")


def test_naive_timestamp_is_treated_as_utc():
    ev = parse_event({**GOOD, "event_ts": "2025-01-01T10:00:00"})
    assert ev.event_ts.tzinfo is not None
