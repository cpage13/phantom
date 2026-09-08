"""The sender must ask the retry strategy for the rung it actually wants.

Objective: pin that the first retry uses the FIRST configured interval.

THE DEFECT THIS CLOSES. ``_on_retryable_failure`` computed
``attempts = row.attempts + 1`` for the row it persists, and passed that SAME
post-increment value to ``schedule_next_attempt``. Both strategies index the
ladder by attempts ALREADY MADE, and ``FixedIntervalsStrategy``'s own unit test
pins ``attempts=0 -> intervals[0]``, so the call site was not meeting the
contract its callee documents.

A fresh row has ``attempts == 0``. On its first upstream failure the strategy
was therefore asked for rung 1, not rung 0. With ``intervals_seconds: [1, 5, 20]``
the first retry waited 5 seconds instead of 1, ``intervals_seconds[0]`` was dead
config that could never produce a delay, and the third failure ran off the end
of the ladder, so an operator who configured three intervals got two.

Exponential shifted identically: ``base * factor ** attempts`` with a
post-incremented count means the documented example of base 5 and factor 4,
described as "5, 20, 80", actually ran 20, 80, 320, and ``base_seconds`` was
never the delay. Under a broad upstream outage that quadruples the
time-to-first-retry across the whole backlog.

The full unit lane passed with the defect in place, which is the coverage gap
these tests close.
"""

from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from phantom.chain.executor import Failed5xx
from phantom.instances.context import InstanceContext
from phantom.models.upload import UploadRow
from phantom.strategies.exponential_backoff import ExponentialBackoffStrategy
from phantom.strategies.fixed_intervals import FixedIntervalsStrategy
from phantom.workers.sender import Sender

from .test_stored_single_writer import _build_instance


async def _drive_one_failure(
    instance: InstanceContext,
    make_upload_row: Callable[..., UploadRow],
    *,
    attempts: int,
) -> UploadRow:
    """Fail one attempt on a row that has already made ``attempts`` of them."""
    row = make_upload_row(
        state="attempting",
        route_name="files",
        attempts=attempts,
        received_at=datetime.now(tz=UTC),
    )
    await instance.store.insert(row)
    sender = Sender(instance=instance, worker_count=1, poll_interval_ms=250)
    await sender._on_retryable_failure(instance.store, row, Failed5xx(status=503))
    fresh = await instance.store.get(row.chain_id)
    assert fresh is not None
    return fresh


@pytest.mark.asyncio
async def test_the_first_retry_uses_the_first_configured_interval(
    tmp_path: Path, make_upload_row: Callable[..., UploadRow]
) -> None:
    """Objective: a fresh row's first retry waits intervals[0].

    Expected: roughly one second, not five. Before the fix the first rung was
    unreachable and every operator's first configured interval was dead.
    """
    instance = await _build_instance(
        tmp_path, retry_strategy=FixedIntervalsStrategy([1, 5, 20], jitter=0.0)
    )

    fresh = await _drive_one_failure(instance, make_upload_row, attempts=0)

    assert fresh.state == "queued"
    assert fresh.next_attempt_at is not None
    delay = fresh.next_attempt_at - datetime.now(tz=UTC)
    assert timedelta(seconds=0) < delay <= timedelta(seconds=2), (
        f"first retry scheduled {delay}, expected about 1s (intervals[0]); "
        "the ladder is shifted by one rung"
    )


@pytest.mark.asyncio
async def test_the_second_retry_uses_the_second_interval(
    tmp_path: Path, make_upload_row: Callable[..., UploadRow]
) -> None:
    """Objective: the ladder advances by one rung per attempt, not two.

    Expected: about five seconds after one prior attempt. Pins that the fix
    shifted the index rather than merely subtracting a constant somewhere.
    """
    instance = await _build_instance(
        tmp_path, retry_strategy=FixedIntervalsStrategy([1, 5, 20], jitter=0.0)
    )

    fresh = await _drive_one_failure(instance, make_upload_row, attempts=1)

    assert fresh.next_attempt_at is not None
    delay = fresh.next_attempt_at - datetime.now(tz=UTC)
    assert timedelta(seconds=3) < delay <= timedelta(seconds=6), (
        f"second retry scheduled {delay}, expected about 5s (intervals[1])"
    )


@pytest.mark.asyncio
async def test_the_operator_gets_every_configured_interval(
    tmp_path: Path, make_upload_row: Callable[..., UploadRow]
) -> None:
    """Objective: three configured intervals means three retries, not two.

    Expected: the row is still retryable after its third prior attempt would
    have run off a shifted ladder, and parks only once the real ladder is
    exhausted. The shift cost the operator the last rung silently.
    """
    instance = await _build_instance(
        tmp_path, retry_strategy=FixedIntervalsStrategy([1, 5, 20], jitter=0.0)
    )

    third = await _drive_one_failure(instance, make_upload_row, attempts=2)
    assert third.state == "queued", (
        "the third retry was refused; a shifted ladder runs off the end one "
        "rung early and the operator loses a configured interval"
    )

    exhausted = await _drive_one_failure(instance, make_upload_row, attempts=3)
    assert exhausted.state == "stored"
    assert exhausted.next_attempt_at is None


@pytest.mark.asyncio
async def test_exponential_backoff_starts_at_base_seconds(
    tmp_path: Path, make_upload_row: Callable[..., UploadRow]
) -> None:
    """Objective: the documented exponential ladder starts at base_seconds.

    Expected: about five seconds for base 5 and factor 4, which the settings
    description gives as "5, 20, 80". With the shift the real ladder was
    20, 80, 320 and base_seconds was never the delay.
    """
    instance = await _build_instance(
        tmp_path,
        retry_strategy=ExponentialBackoffStrategy(
            base_seconds=5.0,
            factor=4.0,
            cap_seconds=600.0,
            jitter=0.0,
            max_attempts=-1,
            max_duration_seconds=-1,
        ),
    )

    fresh = await _drive_one_failure(instance, make_upload_row, attempts=0)

    assert fresh.next_attempt_at is not None
    delay = fresh.next_attempt_at - datetime.now(tz=UTC)
    assert timedelta(seconds=3) < delay <= timedelta(seconds=6), (
        f"first retry scheduled {delay}, expected about base_seconds (5s)"
    )
