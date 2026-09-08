"""What a retry strategy is allowed to schedule, and what the contract carries.

Two defects in ``strategies/``:

* **The cap was applied BEFORE jitter, and jitter was unbounded.**
  ``RetryStrategyCfg.jitter`` was ``ge=0.0`` with no ``le``, so an operator
  reading the description "0.2 -> +/-20%" and writing ``jitter: 20`` meaning
  "20 percent" got ``1.0 + uniform(-20, 20)``: about HALF of all scheduled
  retries landed at exactly ``timedelta(0)``, so the sender re-claimed the row
  on its next 250 ms poll and hammered a failing upstream in a tight loop for
  the whole 24 h budget, while the other half returned up to 21x
  ``cap_seconds`` so the cap did not hold either. ``fixed_intervals`` had the
  identical clamp on the same unbounded knob.
* **The fixed-intervals default lived in the builder, not the model.**
  ``strategies/__init__.py`` substituted a five-step schedule when
  ``intervals_seconds`` was empty, but the model declared
  ``default_factory=list`` so ``contracts/settings.schema.json`` carried NO
  default for that property while every sibling did. An implementation built
  from the contract (the ADR-035 acceptance basis) built an empty schedule and
  sent every row to ``stored`` after a SINGLE failure. The same divergence hit
  an operator who set ``intervals_seconds: []`` deliberately to mean "no
  retries" and got five retries instead.
"""

from __future__ import annotations

from datetime import timedelta

import pytest
from phantom.config.settings import RetryStrategyCfg, Settings
from phantom.strategies import (
    ExponentialBackoffStrategy,
    FixedIntervalsStrategy,
    build_retry_strategy,
)
from pydantic import ValidationError

# "20 percent" written as if the knob took percent. The value from the finding.
_PERCENT_MISREAD_JITTER = 20.0

# Enough draws that a half-of-all-schedules effect cannot hide behind luck.
_DRAWS = 2_000

_CAP_SECONDS = 1800.0

# The positive-delay floor every strategy applies last. Restated here rather
# than imported so each assertion below expresses the behaviour it wants
# independently of the constant; ``test_the_floor_constant_is_what_the_tests
# _assert`` pins the two together.
_MIN_DELAY_SECONDS = 1.0


def _schedule(strategy: object, attempts: int) -> timedelta:
    """One scheduling decision, asserted non-``None``."""
    delay = strategy.schedule_next_attempt(  # type: ignore[attr-defined]  # both strategies satisfy UploadStrategy
        attempts=attempts,
        since_received=timedelta(0),
        last_error=None,
        route_name="route",
    )
    assert delay is not None
    return delay


def test_the_jitter_knob_is_bounded_at_one() -> None:
    """The config must refuse the misreading that caused the loop.

    Objective: the knob is a FRACTION. ``jitter: 20`` is the operator reading
    "+/-20%" and writing the percent, and nothing in the model or the exported
    schema said no. Success: 1.0 (+/-100%, the widest spread that still leaves
    the configured schedule recognisable) validates and 20 does not.
    """
    assert RetryStrategyCfg(jitter=1.0).jitter == 1.0

    with pytest.raises(ValidationError):
        RetryStrategyCfg(jitter=_PERCENT_MISREAD_JITTER)


def test_the_exported_contract_carries_the_jitter_bound() -> None:
    """A port built from ``contracts/`` must inherit the same refusal."""
    jitter_schema = Settings.model_json_schema()["$defs"]["RetryStrategyCfg"]["properties"][
        "jitter"
    ]

    assert jitter_schema["maximum"] == 1.0


def test_no_exponential_delay_is_ever_non_positive() -> None:
    """The half-of-all-retries-at-zero failure, at the widest legal jitter.

    Objective: a zero delay puts ``next_attempt_at`` in the past, so the
    sender re-claims the row on its very next poll - a tight loop against a
    failing upstream. At the maximum legal jitter of 1.0 the multiplier
    ``1.0 + uniform(-1, 1)`` reaches zero, so the floor is what makes the
    delay strictly positive rather than the jitter range.

    Success: every one of many draws is at or above the floor.
    """
    strategy = ExponentialBackoffStrategy(
        base_seconds=5.0,
        factor=4.0,
        cap_seconds=_CAP_SECONDS,
        jitter=1.0,
        max_attempts=-1,
        max_duration_seconds=-1,
    )

    delays = [_schedule(strategy, attempts=2).total_seconds() for _ in range(_DRAWS)]

    assert min(delays) >= _MIN_DELAY_SECONDS, f"scheduled {min(delays)}s"


def test_the_cap_bounds_the_jittered_delay_not_the_pre_jitter_one() -> None:
    """``cap_seconds`` must be the ceiling on what is actually scheduled.

    Objective: capping first and jittering second let a jitter factor above 1
    return multiples of the cap, so the documented "max per-attempt delay" was
    not a maximum. Success: with the jitter at its widest and the exponential
    term far past the cap, no draw exceeds the cap.
    """
    strategy = ExponentialBackoffStrategy(
        base_seconds=5.0,
        factor=4.0,
        cap_seconds=_CAP_SECONDS,
        jitter=1.0,
        max_attempts=-1,
        max_duration_seconds=-1,
    )

    delays = [_schedule(strategy, attempts=9).total_seconds() for _ in range(_DRAWS)]

    assert max(delays) <= _CAP_SECONDS, f"scheduled {max(delays)}s past a {_CAP_SECONDS}s cap"


def test_jitter_still_spreads_the_schedule() -> None:
    """The floor and the cap must not flatten jitter into a constant.

    Objective: jitter exists to de-correlate a backlog that failed in the same
    poll round (V5-C thundering herd). A clamp that returned the same value
    every time would satisfy the two bounds above and defeat the knob.
    Success: the draws span a real range.
    """
    strategy = ExponentialBackoffStrategy(
        base_seconds=60.0,
        factor=2.0,
        cap_seconds=_CAP_SECONDS,
        jitter=0.2,
        max_attempts=-1,
        max_duration_seconds=-1,
    )

    delays = {_schedule(strategy, attempts=1).total_seconds() for _ in range(_DRAWS)}

    assert max(delays) - min(delays) > 1.0


def test_a_very_long_lived_row_does_not_overflow_the_exponential() -> None:
    """Unbounded budgets must not eventually crash the sender.

    Objective: ``max_duration_seconds: -1`` is newly reachable from config, so
    a row can now retry indefinitely and ``factor ** attempts`` will pass the
    float range. ``float.__pow__`` raises ``OverflowError`` there, which in a
    sender worker propagates to the supervising TaskGroup and stops the
    process. Success: a huge attempt count still returns a capped delay.
    """
    strategy = ExponentialBackoffStrategy(
        base_seconds=5.0,
        factor=4.0,
        cap_seconds=_CAP_SECONDS,
        jitter=0.2,
        max_attempts=-1,
        max_duration_seconds=-1,
    )

    assert _schedule(strategy, attempts=10_000).total_seconds() <= _CAP_SECONDS


def test_fixed_intervals_never_schedules_a_zero_from_jitter() -> None:
    """The same clamp on the same knob, in the other strategy.

    Objective: ``fixed_intervals`` applied the identical ``max(0.0, ...)``, so
    the same jitter draw produced the same immediate re-claim. Success: with
    the widest legal jitter, no draw against a positive interval falls below
    the floor.
    """
    strategy = FixedIntervalsStrategy([300], jitter=1.0)

    delays = [_schedule(strategy, attempts=0).total_seconds() for _ in range(_DRAWS)]

    assert min(delays) >= _MIN_DELAY_SECONDS, f"scheduled {min(delays)}s"


def test_a_deliberate_zero_interval_keeps_meaning_retry_at_once() -> None:
    """The floor must not overrule an explicit operator choice.

    Objective: an explicit ``0`` entry in ``intervals_seconds`` says "retry
    immediately", and the list is finite so it cannot become a loop. The floor
    exists for jitter-driven zeros, not for a configured one. Success: a zero
    entry still schedules zero even with jitter on.
    """
    strategy = FixedIntervalsStrategy([0, 300], jitter=1.0)

    assert _schedule(strategy, attempts=0) == timedelta(0)


def test_the_fixed_intervals_default_lives_in_the_exported_contract() -> None:
    """The builder's private fallback had to become part of the contract.

    Objective: ``strategies/__init__.py`` substituted ``[1, 5, 20, 60, 300]``
    for an empty list, but the exported schema carried no default for the
    property, so an implementation built from ``contracts/`` (ADR-035) built
    an empty schedule and sent every row to ``stored`` after one failure.

    Success: the model's default IS the five-step schedule and the exported
    schema carries it, like every sibling property.
    """
    assert RetryStrategyCfg().intervals_seconds == [1, 5, 20, 60, 300]

    exported = Settings.model_json_schema()["$defs"]["RetryStrategyCfg"]["properties"][
        "intervals_seconds"
    ]
    assert exported["default"] == [1, 5, 20, 60, 300]


def test_an_explicitly_empty_schedule_means_no_retries() -> None:
    """The other half of moving the default: [] must mean what it says.

    Objective: with the fallback in the builder, an operator who wrote
    ``intervals_seconds: []`` to mean "no retries" silently got five. Success:
    the built strategy gives up on the first scheduling call.
    """
    strategy = build_retry_strategy(RetryStrategyCfg(type="fixed_intervals", intervals_seconds=[]))

    assert (
        strategy.schedule_next_attempt(
            attempts=0,
            since_received=timedelta(0),
            last_error=None,
            route_name="route",
        )
        is None
    )


def test_the_default_config_still_builds_the_five_step_schedule() -> None:
    """Moving the default must not change what a default deployment does.

    Objective: the fallback was real behaviour for every ``fixed_intervals``
    deployment that did not pin a list; relocating it to the model has to
    preserve that exactly. Success: the built strategy's first five delays
    follow the schedule and the sixth call gives up.
    """
    # jitter off so the assertion is on the SCHEDULE rather than on a draw.
    strategy = build_retry_strategy(RetryStrategyCfg(type="fixed_intervals", jitter=0.0))

    scheduled = [_schedule(strategy, attempts=i).total_seconds() for i in range(5)]

    assert scheduled == [1.0, 5.0, 20.0, 60.0, 300.0]
    assert (
        strategy.schedule_next_attempt(
            attempts=5,
            since_received=timedelta(0),
            last_error=None,
            route_name="route",
        )
        is None
    )


def test_a_default_strategy_config_produces_a_sane_first_delay() -> None:
    """End-to-end sanity on the shipped defaults, through the real builder."""
    strategy = build_retry_strategy(RetryStrategyCfg())

    delay = _schedule(strategy, attempts=0).total_seconds()

    assert _MIN_DELAY_SECONDS <= delay <= 6.0


def test_the_floor_constant_is_what_the_tests_assert() -> None:
    """Pin the shared floor to the value every assertion above expects."""
    from phantom.strategies.interface import MIN_RETRY_DELAY_SECONDS

    assert MIN_RETRY_DELAY_SECONDS == _MIN_DELAY_SECONDS
