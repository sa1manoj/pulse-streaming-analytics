from collections import Counter

from pulse.events import parse_event
from pulse.generator import GeneratorConfig, generate, variant_for


def _events(n=3000, **cfg):
    return list(generate(n, GeneratorConfig(**cfg)))


def test_every_event_satisfies_the_contract():
    for ev in _events(500):
        assert parse_event(ev.to_json()) == ev


def test_assignment_is_stable_and_roughly_even():
    assert variant_for("u000123") == variant_for("u000123")
    split = Counter(variant_for(f"u{i:06d}") for i in range(10_000))
    assert 0.47 < split["treatment"] / 10_000 < 0.53


def test_sessions_never_mix_variants():
    by_session = {}
    for ev in _events(1000):
        assert by_session.setdefault(ev.session_id, ev.variant) == ev.variant


def test_duplicates_and_late_events_are_injected():
    evs = _events(3000, duplicate_fraction=0.05, late_fraction=0.05)
    ids = Counter(e.event_id for e in evs)
    assert any(c > 1 for c in ids.values())
    # a late event has an event_ts earlier than an event sent before it
    assert any(b.event_ts < a.event_ts for a, b in zip(evs, evs[1:], strict=False))


def test_funnel_only_moves_forward_within_a_session():
    order = {"page_view": 0, "add_to_cart": 1, "checkout": 2, "purchase": 3}
    seen = {}
    for ev in _events(1000, duplicate_fraction=0, late_fraction=0):
        prev = seen.get(ev.session_id, -1)
        assert order[ev.event_type] == prev + 1
        seen[ev.session_id] = order[ev.event_type]


def test_generation_is_deterministic():
    assert _events(100) == _events(100)
