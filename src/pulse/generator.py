"""Synthetic clickstream generator (all data is simulated).

Users are split 50/50 into ``control`` and ``treatment`` by a stable hash of
user_id. Each session walks the funnel page_view -> add_to_cart -> checkout ->
purchase with drop-off at every step; treatment has a configurable lift on the
checkout -> purchase step so the A/B analysis has something to find.

To exercise the stream job's correctness logic, a fraction of events is:

* **late**: sent with an ``event_ts`` several minutes in the past, and
* **duplicated**: sent twice with the same ``event_id`` (a client retry).

Usage:
    python -m pulse.generator --rate 50 --seconds 600            # send to Kafka
    python -m pulse.generator --rate 50 --seconds 10 --dry-run   # print instead
"""
from __future__ import annotations

import argparse
import hashlib
import random
import time
import uuid
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

from pulse.events import Event

PRODUCTS = [(f"P{i:03d}", round(5 + i * 3.75, 2)) for i in range(1, 41)]


@dataclass
class GeneratorConfig:
    users: int = 5_000
    base_purchase_rate: float = 0.30  # checkout -> purchase, control
    treatment_lift: float = 0.10       # relative lift for treatment (0.10 = +10%)
    late_fraction: float = 0.02
    late_max_minutes: int = 15
    duplicate_fraction: float = 0.01
    seed: int = 42


def variant_for(user_id: str) -> str:
    """Stable 50/50 assignment: the same user always sees the same variant."""
    bucket = int(hashlib.sha256(user_id.encode()).hexdigest(), 16) % 100
    return "treatment" if bucket < 50 else "control"


def _session(rng: random.Random, cfg: GeneratorConfig, now: datetime) -> list[Event]:
    user_id = f"u{rng.randint(1, cfg.users):06d}"
    variant = variant_for(user_id)
    session_id = str(uuid.UUID(int=rng.getrandbits(128)))
    product_id, price = rng.choice(PRODUCTS)

    purchase_rate = cfg.base_purchase_rate * ((1 + cfg.treatment_lift) if variant == "treatment" else 1)
    steps = [("page_view", 1.0), ("add_to_cart", 0.35), ("checkout", 0.6), ("purchase", purchase_rate)]

    events, ts = [], now
    for event_type, p in steps:
        if rng.random() > p:
            break
        ts += timedelta(seconds=rng.randint(2, 40))
        events.append(
            Event(
                event_id=str(uuid.UUID(int=rng.getrandbits(128))),
                user_id=user_id,
                session_id=session_id,
                event_type=event_type,
                variant=variant,
                product_id=product_id,
                price=price if event_type == "purchase" else None,
                event_ts=ts,
            )
        )
    return events


def generate(
    n_sessions: int,
    cfg: GeneratorConfig | None = None,
    start: datetime | None = None,
    spacing_seconds: float | None = None,
) -> Iterator[Event]:
    """Yield events in *send* order, including late and duplicate sends.

    Sessions start ``spacing_seconds`` apart (random 0.5-3s if not given).
    """
    cfg = cfg or GeneratorConfig()
    rng = random.Random(cfg.seed)
    clock = start or datetime(2025, 1, 1, tzinfo=timezone.utc)
    for _ in range(n_sessions):
        clock += timedelta(seconds=spacing_seconds if spacing_seconds is not None else rng.uniform(0.5, 3))
        for ev in _session(rng, cfg, clock):
            if rng.random() < cfg.late_fraction:
                lateness = timedelta(minutes=rng.uniform(1, cfg.late_max_minutes))
                ev = Event(**{**ev.__dict__, "event_ts": ev.event_ts - lateness})
            yield ev
            if rng.random() < cfg.duplicate_fraction:
                yield ev  # client retry: identical payload, same event_id


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Send synthetic clickstream events to Kafka")
    parser.add_argument("--bootstrap", default="localhost:9092")
    parser.add_argument("--topic", default="clickstream")
    parser.add_argument("--rate", type=float, default=20, help="sessions per second")
    parser.add_argument("--seconds", type=int, default=60)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--seed", type=int, default=None)
    args = parser.parse_args(argv)

    cfg = GeneratorConfig(seed=args.seed if args.seed is not None else int(time.time()))
    producer = None
    if not args.dry_run:
        from kafka import KafkaProducer

        producer = KafkaProducer(
            bootstrap_servers=args.bootstrap,
            key_serializer=str.encode,
            value_serializer=str.encode,
            acks="all",
            linger_ms=20,
        )

    sent, deadline = 0, time.time() + args.seconds
    rng = random.Random(cfg.seed)
    while time.time() < deadline:
        tick_start = time.time()
        batch = max(1, int(args.rate))
        tick_cfg = GeneratorConfig(**{**cfg.__dict__, "seed": rng.getrandbits(32)})
        for ev in generate(batch, tick_cfg, start=datetime.now(timezone.utc), spacing_seconds=1 / batch):
            if producer:
                producer.send(args.topic, key=ev.user_id, value=ev.to_json())
            else:
                print(ev.to_json())
            sent += 1
        time.sleep(max(0.0, 1 - (time.time() - tick_start)))
    if producer:
        producer.flush()
    print(f"sent {sent} events to {args.topic if producer else 'stdout'}")


if __name__ == "__main__":
    main()
