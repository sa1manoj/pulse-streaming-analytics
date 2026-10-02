import math

import pandas as pd
import pytest

from pulse.abtest import analyze, required_sessions_per_variant, session_conversions, two_proportion_test
from pulse.fixtures import events_frame
from pulse.generator import GeneratorConfig, generate


def test_known_values():
    # 200/1000 vs 240/1000: pooled p = 0.22, SE = 0.01853 -> z = 2.159, p = 0.0308
    r = two_proportion_test(200, 1000, 240, 1000)
    assert r.abs_diff == pytest.approx(0.04)
    assert r.rel_lift == pytest.approx(0.20)
    assert r.z == pytest.approx(2.1592, abs=1e-3)
    assert r.p_value == pytest.approx(0.0308, abs=1e-3)
    assert r.ci_low < 0.04 < r.ci_high
    assert r.significant


def test_no_difference_is_not_significant():
    r = two_proportion_test(300, 1000, 300, 1000)
    assert r.p_value == pytest.approx(1.0)
    assert not r.significant


def test_sample_ratio_mismatch_invalidates_result():
    r = two_proportion_test(200, 1000, 400, 1600)
    assert r.srm_detected
    assert not r.significant
    assert "sample ratio mismatch" in r.summary()


def test_session_conversion_flags():
    ev = pd.DataFrame(
        {
            "session_id": ["a", "a", "b"],
            "variant": ["control", "control", "treatment"],
            "event_type": ["page_view", "purchase", "page_view"],
        }
    )
    s = session_conversions(ev).set_index("session_id")
    assert bool(s.loc["a", "converted"]) and not bool(s.loc["b", "converted"])


def test_session_in_two_variants_is_an_error():
    ev = pd.DataFrame(
        {"session_id": ["a", "a"], "variant": ["control", "treatment"], "event_type": ["page_view"] * 2}
    )
    with pytest.raises(ValueError, match="both variants"):
        session_conversions(ev)


def test_simulated_lift_is_detected():
    cfg = GeneratorConfig(treatment_lift=0.5, seed=3)
    events = pd.DataFrame([e.__dict__ for e in generate(20_000, cfg)])
    r = analyze(events)
    assert r.treatment_rate > r.control_rate
    assert r.significant


def test_fixture_data_has_no_lift_artifacts_from_duplicates():
    # duplicates are removed by event_id before analysis
    ev = events_frame(2000, seed=1)
    assert ev["event_id"].is_unique


def test_sample_size_matches_textbook_order_of_magnitude():
    # base 10%, +10% relative lift -> roughly 14-15k per variant at 80% power
    n = required_sessions_per_variant(0.10, 0.10)
    assert 14_000 < n < 15_500
    assert math.isfinite(n)
