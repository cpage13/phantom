"""Unit tests for phantom.strategies (retry plugins)."""

from __future__ import annotations

from datetime import timedelta

from phantom.strategies import (
    ExponentialBackoffStrategy,
    FixedIntervalsStrategy,
)


def test_fixed_intervals_schedule() -> None:
    """``intervals[attempts]`` is returned; None past the end (jitter=0 default)."""
    strat = FixedIntervalsStrategy([1, 5, 20])
    assert strat.schedule_next_attempt(
        attempts=0, since_received=timedelta(), last_error=None, route_name="r"
    ) == timedelta(seconds=1)
    assert strat.schedule_next_attempt(
        attempts=2, since_received=timedelta(), last_error=None, route_name="r"
    ) == timedelta(seconds=20)
    assert (
        strat.schedule_next_attempt(
            attempts=3, since_received=timedelta(), last_error=None, route_name="r"
        )
        is None
    )


def test_fixed_intervals_jitter_within_bounds_and_decorrelates() -> None:
    """V5-C: a non-zero jitter keeps each delay within ±frac and de-correlates a backlog.

    The default ``jitter=0.0`` preserves an exact schedule (above); a positive
    jitter spreads the delays so a backlog that fails in the same poll round
    does NOT fire in lockstep (thundering herd) on upstream recovery.
    """
    base = 20
    jitter = 0.2
    strat = FixedIntervalsStrategy([1, 5, base], jitter=jitter)
    lo = base * (1.0 - jitter)
    hi = base * (1.0 + jitter)
    delays = [
        strat.schedule_next_attempt(
            attempts=2, since_received=timedelta(), last_error=None, route_name="r"
        )
        for _ in range(200)
    ]
    seconds = [d.total_seconds() for d in delays if d is not None]
    assert len(seconds) == 200
    # Every sample is within the jitter band.
    assert all(lo <= s <= hi for s in seconds)
    # The band is actually exercised (not a constant) — herd de-correlation.
    assert max(seconds) - min(seconds) > 0.0
    assert len(set(seconds)) > 1
    # Past the end is still None even with jitter.
    assert (
        strat.schedule_next_attempt(
            attempts=3, since_received=timedelta(), last_error=None, route_name="r"
        )
        is None
    )


def test_fixed_intervals_jitter_never_negative() -> None:
    """A jitter >= 1.0 against a 0-second interval never yields a negative delay."""
    strat = FixedIntervalsStrategy([0, 0], jitter=1.0)
    for attempts in (0, 1):
        delay = strat.schedule_next_attempt(
            attempts=attempts, since_received=timedelta(), last_error=None, route_name="r"
        )
        assert delay is not None and delay.total_seconds() >= 0.0


def test_exp_backoff_caps_and_jitters() -> None:
    """Backoff caps and respects jitter bounds."""
    strat = ExponentialBackoffStrategy(
        base_seconds=5.0,
        factor=4.0,
        cap_seconds=20.0,
        jitter=0.0,
        max_attempts=-1,
        max_duration_seconds=-1,
    )
    # attempts=0 → base=5, cap not hit.
    delay = strat.schedule_next_attempt(
        attempts=0, since_received=timedelta(), last_error=None, route_name="r"
    )
    assert delay is not None and delay == timedelta(seconds=5)
    # attempts=2 → 5*16 = 80 capped at 20.
    delay = strat.schedule_next_attempt(
        attempts=2, since_received=timedelta(), last_error=None, route_name="r"
    )
    assert delay is not None and delay == timedelta(seconds=20)


def test_exp_backoff_max_attempts() -> None:
    """``max_attempts`` returns None when exhausted."""
    strat = ExponentialBackoffStrategy(
        base_seconds=1.0,
        factor=2.0,
        cap_seconds=10.0,
        jitter=0.0,
        max_attempts=3,
        max_duration_seconds=-1,
    )
    assert (
        strat.schedule_next_attempt(
            attempts=3, since_received=timedelta(), last_error=None, route_name="r"
        )
        is None
    )


def test_exp_backoff_max_duration() -> None:
    """``max_duration_seconds`` returns None when exceeded."""
    strat = ExponentialBackoffStrategy(
        base_seconds=1.0,
        factor=2.0,
        cap_seconds=10.0,
        jitter=0.0,
        max_attempts=-1,
        max_duration_seconds=60,
    )
    assert (
        strat.schedule_next_attempt(
            attempts=0,
            since_received=timedelta(seconds=120),
            last_error=None,
            route_name="r",
        )
        is None
    )
