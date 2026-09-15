"""S7-1: the ADR-011 capture rewind is scheduled, budgeted and bounded.

THE DEFECT THIS CLOSES. ``Sender._on_rewind`` wrote ``attempts=0`` with
``next_attempt_at=now`` and never consulted
:meth:`UploadStrategy.schedule_next_attempt`. A rewind cycle therefore had no
attempt budget, no backoff and no wall-clock bound: a three-step chain that
captures at step 1 with a TTL shorter than step 2's duration rewinds at step 3,
re-executes step 1 (one real upstream write per cycle, which ADR-011 says "may
create a duplicate"), succeeds mid-chain, runs step 2, and expires at step 3
again, forever. The row only ever cycles between ``queued`` and ``attempting``,
both in ``SLOT_HOLDING_STATES``, so the crossing is always a no-op and its
saturation charge is never released either: a handful of such rows permanently
shrink ``max_in_flight`` and ``max_in_flight_bytes`` until fresh ingress 503s
with nothing actually in flight.

This is the SIXTH of the six slot-accounting leaks the full-coverage review
found, and it was still open when commit 528af9b claimed all six were closed.

WHAT BOUNDS IT, PRECISELY. The route's ``send_deadline_seconds`` does bound the
cycle when an operator sets one, so the knob is not powerless; it defaults to
``None``, which is where the cycle ran forever. The fix routes the rewind
through the same retry budget every other re-queue uses, whose shipped default
(``exponential_backoff``) carries ``max_duration_seconds=86_400``, measured
from ``received_at`` and therefore unaffected by the mid-chain ``attempts``
reset.

Both tests drive the REAL ``Sender._on_rewind`` against the REAL shipped
strategy, and assert on the columns the write actually persisted.
"""

from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest
from phantom.chain.executor import CaptureExpiredRewind
from phantom.instances.context import InstanceContext
from phantom.models.upload import UploadRow
from phantom.storage.interface import AttemptWriteOutcome, UploadStore
from phantom.strategies import ExponentialBackoffStrategy
from phantom.workers.sender import Sender

pytestmark = pytest.mark.asyncio

# The shipped ``retry.default_strategy`` defaults, spelled out so the test
# reads against the configuration an operator actually gets rather than
# against numbers invented for the test.
_SHIPPED_BASE_SECONDS: float = 5.0
_SHIPPED_FACTOR: float = 4.0
_SHIPPED_CAP_SECONDS: float = 1800.0
_SHIPPED_JITTER: float = 0.2
_UNBOUNDED_SENTINEL: int = -1
_SHIPPED_MAX_DURATION_SECONDS: int = 86_400

# How far past the wall-clock budget the exhausted-budget row sits. Any
# positive margin proves the guard; one hour is wide enough that the assertion
# cannot turn on clock resolution.
_PAST_BUDGET_MARGIN_SECONDS: int = 3_600

# The rewind result the executor hands the sender: a three-step chain whose
# step 3 found the step 1 capture expired.
_REWIND = CaptureExpiredRewind(producing_step="create_upload", rewind_to_step_index=0)


def _shipped_strategy() -> ExponentialBackoffStrategy:
    """The default retry strategy exactly as ``RetryStrategyCfg`` ships it."""
    return ExponentialBackoffStrategy(
        base_seconds=_SHIPPED_BASE_SECONDS,
        factor=_SHIPPED_FACTOR,
        cap_seconds=_SHIPPED_CAP_SECONDS,
        jitter=_SHIPPED_JITTER,
        max_attempts=_UNBOUNDED_SENTINEL,
        max_duration_seconds=_SHIPPED_MAX_DURATION_SECONDS,
    )


def _sender_over(strategy: ExponentialBackoffStrategy) -> Sender:
    """A sender whose only live collaborator is the retry strategy."""
    instance = MagicMock(spec=InstanceContext)
    instance.retry_strategy = strategy
    instance.saturation = AsyncMock()
    return Sender(instance=instance, worker_count=1, poll_interval_ms=1)


def _recording_store() -> tuple[UploadStore, list[dict[str, Any]]]:
    """A store that records every ``record_attempt_result`` call's keywords."""
    calls: list[dict[str, Any]] = []

    async def _record(chain_id: object, **kwargs: Any) -> AttemptWriteOutcome:
        """Capture the write and report it as landed."""
        del chain_id
        calls.append(kwargs)
        return AttemptWriteOutcome(
            rowcount=1,
            new_state=kwargs["new_state"],
            previous_state="attempting",
            previous_body_discarded_at=None,
            body_size_bytes=0,
        )

    store = MagicMock(spec=UploadStore)
    store.record_attempt_result = AsyncMock(side_effect=_record)
    return store, calls


async def test_a_rewind_within_budget_backs_off_and_burns_an_attempt(
    make_upload_row: Callable[..., UploadRow],
) -> None:
    """Objective: a rewind is SCHEDULED, not re-queued for the very next poll.

    Expected: the persisted write carries ``next_attempt_at`` strictly in the
    future (the strategy's delay) and ``attempts`` incremented by one, so the
    cycle has both a backoff and a ladder position. Before the fix the write
    carried ``attempts=0`` and ``next_attempt_at=now``, which made the row due
    on the sender's very next poll (250 ms by default) and re-executed the
    producing step at that rate for as long as the chain kept rewinding.
    """
    row = make_upload_row(state="attempting", attempts=2)
    sender = _sender_over(_shipped_strategy())
    store, calls = _recording_store()

    before = datetime.now(tz=UTC)
    await sender._on_rewind(store, row, _REWIND)

    assert len(calls) == 1, "the rewind issues exactly one attempt write"
    write = calls[0]
    assert write["new_state"] == "queued"
    assert write["attempts"] == row.attempts + 1, (
        "the rewind burns an attempt: it re-executes the producing step, which "
        "ADR-011 says may create a duplicate"
    )
    assert write["next_attempt_at"] > before, (
        "the rewind is scheduled behind the strategy's delay, not made due at once"
    )


async def test_a_rewind_past_the_wall_clock_budget_parks_instead_of_cycling(
    make_upload_row: Callable[..., UploadRow],
) -> None:
    """Objective: the rewind cycle terminates under the SHIPPED default config.

    Expected: a row whose ``received_at`` is older than the strategy's
    ``max_duration_seconds`` is written to terminal ``stored`` rather than back
    to ``queued``. That is what ends the loop: ``stored`` is not re-claimed by
    ``claim_due``, so the worker is freed and the upstream stops receiving one
    create per cycle, and because ``stored`` IS terminal the row's retained slot
    is finally reachable by ``evict_terminal_over_limit`` and the terminal
    retention sweeps, where a row cycling queued/attempting forever was
    reachable by nothing at all.

    Before the fix this row was written back to ``queued`` with ``attempts=0``
    and ``next_attempt_at=now`` no matter how long it had been trying.
    """
    row = make_upload_row(
        state="attempting",
        attempts=0,
        received_at=datetime.now(tz=UTC)
        - timedelta(seconds=_SHIPPED_MAX_DURATION_SECONDS + _PAST_BUDGET_MARGIN_SECONDS),
    )
    sender = _sender_over(_shipped_strategy())
    store, calls = _recording_store()

    await sender._on_rewind(store, row, _REWIND)

    assert len(calls) == 1, "the exhausted rewind issues exactly one attempt write"
    write = calls[0]
    assert write["new_state"] == "stored", (
        "an exhausted rewind budget parks the row; re-queuing it is the "
        "unbounded cycle this test exists to forbid"
    )
    assert write["next_attempt_at"] is None, "a parked row is never due again"
