"""Clickstream event contract.

Every event is a JSON object with these fields. ``event_id`` is a UUID chosen by
the client, so a retried send produces the same id; the stream job uses it to
drop duplicates.
"""
from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import datetime, timezone

EVENT_TYPES = ("page_view", "add_to_cart", "checkout", "purchase")
VARIANTS = ("control", "treatment")

# (name, Spark SQL type) - single source of truth for the Spark schema and dbt docs
FIELDS: list[tuple[str, str]] = [
    ("event_id", "string"),
    ("user_id", "string"),
    ("session_id", "string"),
    ("event_type", "string"),
    ("variant", "string"),
    ("product_id", "string"),
    ("price", "double"),
    ("event_ts", "timestamp"),
]


class InvalidEvent(ValueError):
    pass


@dataclass(frozen=True)
class Event:
    event_id: str
    user_id: str
    session_id: str
    event_type: str
    variant: str
    product_id: str | None
    price: float | None
    event_ts: datetime

    def to_json(self) -> str:
        d = asdict(self)
        d["event_ts"] = self.event_ts.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
        return json.dumps(d)


def parse_event(raw: str | dict) -> Event:
    """Validate one raw event. Raises InvalidEvent with a reason on bad input."""
    try:
        d = json.loads(raw) if isinstance(raw, str) else dict(raw)
    except json.JSONDecodeError as e:
        raise InvalidEvent(f"not JSON: {e}") from e

    required = ("event_id", "user_id", "session_id", "event_type", "variant", "event_ts")
    missing = [f for f in required if not d.get(f)]
    if missing:
        raise InvalidEvent(f"missing fields: {missing}")
    if d["event_type"] not in EVENT_TYPES:
        raise InvalidEvent(f"unknown event_type {d['event_type']!r}")
    if d["variant"] not in VARIANTS:
        raise InvalidEvent(f"unknown variant {d['variant']!r}")

    price = d.get("price")
    if d["event_type"] == "purchase":
        if price is None or float(price) <= 0:
            raise InvalidEvent("purchase needs a positive price")
    try:
        ts = datetime.fromisoformat(str(d["event_ts"]).replace("Z", "+00:00"))
    except ValueError as e:
        raise InvalidEvent(f"bad event_ts: {d['event_ts']!r}") from e
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)

    return Event(
        event_id=str(d["event_id"]),
        user_id=str(d["user_id"]),
        session_id=str(d["session_id"]),
        event_type=d["event_type"],
        variant=d["variant"],
        product_id=d.get("product_id"),
        price=None if price is None else float(price),
        event_ts=ts,
    )
