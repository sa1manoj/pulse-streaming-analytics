"""A/B test analysis: session conversion, control vs. treatment.

Unit of analysis is the session (conversion = the session has a purchase).
Users are assigned by a stable hash, so a user's sessions all share a variant.
Sessions of the same user are not independent; with many users and few
sessions each this is a small effect, and it is called out in the report.

Uses a two-sided two-proportion z-test and a Wald confidence interval for the
absolute difference, plus a sample-ratio-mismatch (SRM) check, because a broken
50/50 split invalidates the comparison regardless of the p-value.
"""
from __future__ import annotations

import math
from dataclasses import dataclass

import pandas as pd
from scipy import stats


@dataclass(frozen=True)
class ABResult:
    control_sessions: int
    treatment_sessions: int
    control_rate: float
    treatment_rate: float
    abs_diff: float
    rel_lift: float
    ci_low: float
    ci_high: float
    z: float
    p_value: float
    srm_p_value: float
    alpha: float = 0.05

    @property
    def significant(self) -> bool:
        return self.p_value < self.alpha and not self.srm_detected

    @property
    def srm_detected(self) -> bool:
        return self.srm_p_value < 0.001

    def summary(self) -> str:
        verdict = "SIGNIFICANT" if self.significant else "not significant"
        if self.srm_detected:
            verdict = "INVALID (sample ratio mismatch)"
        return (
            f"control {self.control_rate:.2%} (n={self.control_sessions:,}) vs "
            f"treatment {self.treatment_rate:.2%} (n={self.treatment_sessions:,}); "
            f"diff {self.abs_diff:+.2%} [{self.ci_low:+.2%}, {self.ci_high:+.2%}], "
            f"lift {self.rel_lift:+.1%}, p={self.p_value:.4f} -> {verdict}"
        )


def session_conversions(events: pd.DataFrame) -> pd.DataFrame:
    """One row per session: variant and whether it converted."""
    s = events.groupby(["session_id", "variant"], as_index=False).agg(
        converted=("event_type", lambda x: bool((x == "purchase").any()))
    )
    dup = s["session_id"].duplicated(keep=False)
    if dup.any():
        raise ValueError(f"{int(dup.sum())} sessions appear in both variants; assignment is broken")
    return s


def two_proportion_test(
    conv_c: int, n_c: int, conv_t: int, n_t: int, alpha: float = 0.05
) -> ABResult:
    if min(n_c, n_t) == 0:
        raise ValueError("both variants need at least one session")
    p_c, p_t = conv_c / n_c, conv_t / n_t
    pooled = (conv_c + conv_t) / (n_c + n_t)
    se_pooled = math.sqrt(pooled * (1 - pooled) * (1 / n_c + 1 / n_t))
    z = (p_t - p_c) / se_pooled if se_pooled > 0 else 0.0
    p_value = 2 * stats.norm.sf(abs(z))

    se_diff = math.sqrt(p_c * (1 - p_c) / n_c + p_t * (1 - p_t) / n_t)
    z_crit = stats.norm.ppf(1 - alpha / 2)
    diff = p_t - p_c
    srm = stats.chisquare([n_c, n_t]).pvalue  # expected 50/50

    return ABResult(
        control_sessions=n_c,
        treatment_sessions=n_t,
        control_rate=p_c,
        treatment_rate=p_t,
        abs_diff=diff,
        rel_lift=diff / p_c if p_c else float("nan"),
        ci_low=diff - z_crit * se_diff,
        ci_high=diff + z_crit * se_diff,
        z=z,
        p_value=float(p_value),
        srm_p_value=float(srm),
        alpha=alpha,
    )


def analyze(events: pd.DataFrame, alpha: float = 0.05) -> ABResult:
    s = session_conversions(events)
    g = s.groupby("variant")["converted"].agg(["sum", "count"])
    c, t = g.loc["control"], g.loc["treatment"]
    return two_proportion_test(int(c["sum"]), int(c["count"]), int(t["sum"]), int(t["count"]), alpha)


def required_sessions_per_variant(base_rate: float, rel_lift: float, alpha=0.05, power=0.8) -> int:
    """Sessions per variant to detect ``rel_lift`` on ``base_rate`` (normal approximation)."""
    p1, p2 = base_rate, base_rate * (1 + rel_lift)
    z_a, z_b = stats.norm.ppf(1 - alpha / 2), stats.norm.ppf(power)
    p_bar = (p1 + p2) / 2
    n = (z_a * math.sqrt(2 * p_bar * (1 - p_bar)) + z_b * math.sqrt(p1 * (1 - p1) + p2 * (1 - p2))) ** 2
    return math.ceil(n / (p2 - p1) ** 2)
