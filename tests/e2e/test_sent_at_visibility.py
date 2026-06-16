"""sent_at end to end: absent until delivery, stamped once, replay-proof.

Cycle-7 plan 06_09 task 7.1(c). The write-once stamp semantics are
pinned at the unit tier (``test_sent_at_stamp.py``: the CASE WHEN
guard, the sole sender call site, the replay regression against the
store directly); this module proves the same truths over the wire
through the live service + emulator stack and the admin surface:

* ``sent_at`` is NULL on the admin detail while the upload is parked
  behind a failing upstream (attempted, undelivered);
* delivery stamps it exactly once, and the admission defaults ride
  alongside it on the same detail (``group_id`` = chain_id,
  ``multifile_id`` null, ``send_order`` 0);
* the replay-then-resucceed regression END TO END: an admin replay
  re-queues the row, the stamp SURVIVES the replay (observed mid-flight
  while the re-attempt bounces off a held-down upstream), the second
  delivery does NOT move it, and ``updated_at`` keeps moving past it;
* the body-discarded replay REFUSAL end to end (cycle-7 phase 7
  pre-round defender fix): under the suite-default zero retention the
  sender's immediate-discard leg drops the body at delivery, and a
  subsequent admin replay gets the canonical 409
  ``replay_body_discarded`` envelope instead of a re-queue that could
  only corrupt; the row is untouched and ``sent_at`` survives.
"""

from __future__ import annotations

import secrets
from datetime import datetime
from uuid import UUID

import pytest
from phantom_client import (
    ChainAdminDetail,
    PhantomClient,
    PhantomConflictError,
    UploadRow,
)
from phantom_emulator.failure.injection import FailurePolicy, FailureScope

from ._driver import PhantomDriver
from .helpers.assertions import assert_chain_reaches_state
from .helpers.payloads import build_create_file_request
from .helpers.stack import E2EStack, boot_stack
from .helpers.timing import await_until

pytestmark = pytest.mark.e2e

# 5xx error rate for hold-down policies. 1.0 = every upstream call fails.
FORCE_5XX_RATE: float = 1.0

# Body size for the probe uploads; small keeps the default lane quick.
PROBE_BODY_BYTES: int = 1024

# Budget for one upload to reach a terminal state on a healthy upstream
# (e2e retry cadence fixed_intervals [0, 1, 2, 5, 10]).
TERMINAL_BUDGET_SECONDS: float = 30.0

# Budget for the held-down row to record its first failed attempt.
FIRST_ATTEMPT_BUDGET_SECONDS: float = 10.0

# Reaper interval override that effectively disables the reaper, so the
# succeeded row survives the replay + re-poll sequence.
REAPER_DISABLED_INTERVAL_SECONDS: int = 3600

# Succeeded-body retention for the replay test. The suite default is 0
# (the sender's immediate-discard leg deletes the body the moment the
# chain succeeds); pre-defender-fix a replayed row with no body landed
# ``corrupted`` on its next claim, and post-fix the replay is refused
# outright (the test below). Retaining the body for an hour makes
# replay-then-RESUCCEED reachable, which is the regression under test
# here. The metadata window must be >= the body window (boot invariant
# check_retention_floor), so both ride the same value.
REPLAYABLE_BODY_RETENTION_SECONDS: int = 3600

# Budget for the immediate-discard leg's stamp to land after delivery.
# The sender stamps body_discarded_at right after recording succeeded
# (two awaits later), so this is generous slack, not an expected wait.
DISCARD_STAMP_BUDGET_SECONDS: float = 10.0


def _driver_for(stack: E2EStack) -> PhantomDriver:
    """Build the public test driver bound to ``stack``."""
    return PhantomDriver(
        stack.phantom_client,
        files_api=stack.emulator_url,
        get_security_token=stack.fake_security_token,
    )


async def _await_body_discarded(pc: PhantomClient, chain_id: UUID) -> UploadRow:
    """Poll until the row's ``body_discarded_at`` is stamped; return the row.

    Reads the admin LIST surface (the full :class:`UploadRow`, which
    carries the discard accounting; the detail shape does not). The
    ``group_id`` filter resolves the singleton group (admission defaults
    ``group_id`` to the chain_id on optionless submits).
    """
    rows: list[UploadRow] = []

    async def _stamped() -> bool:
        found, _cursor = await pc.list_uploads(group_id=chain_id)
        rows[:] = [r for r in found if r.chain_id == chain_id]
        return bool(rows) and rows[0].body_discarded_at is not None

    await await_until(
        _stamped,
        timeout_seconds=DISCARD_STAMP_BUDGET_SECONDS,
        message="the immediate-discard leg never stamped body_discarded_at",
    )
    return rows[0]


async def _await_attempted(pc: PhantomClient, detail_of: ChainAdminDetail) -> ChainAdminDetail:
    """Poll until the row has at least one recorded attempt; return the detail."""
    snapshots: list[ChainAdminDetail] = [detail_of]

    async def _attempted() -> bool:
        latest = await pc.get_upload(detail_of.chain_id)
        snapshots.append(latest)
        return latest.attempts > 0

    await await_until(
        _attempted,
        timeout_seconds=FIRST_ATTEMPT_BUDGET_SECONDS,
        message="the held-down row never recorded an attempt",
    )
    return snapshots[-1]


async def test_sent_at_absent_until_delivery_then_stamped() -> None:
    """sent_at is null while undelivered and stamped on confirmed delivery.

    Hold the upstream down, submit, watch the row get attempted while
    ``sent_at`` stays null (state and updated_at do NOT cover delivery
    time; the stamp is its own truth); heal the upstream and watch the
    stamp appear, consistent with the singleton group rollup's
    ``last_sent_at``.
    """
    stack = await boot_stack(
        config_overrides={
            "retention": {"reaper_interval_seconds": REAPER_DISABLED_INTERVAL_SECONDS},
        },
    )
    try:
        pc: PhantomClient = stack.phantom_client
        emulator = stack.emulator
        emulator.clear_received()
        emulator.clear_failures()
        emulator.inject_failure(
            FailurePolicy(  # type: ignore[call-arg]  # FailurePolicy fields have defaults; mypy lacks pydantic plugin
                scope=FailureScope.GLOBAL,
                error_rate_5xx=FORCE_5XX_RATE,
            )
        )
        driver = _driver_for(stack)
        result = await driver.in_memory_upload(
            build_create_file_request(file_name="sent-at-parked.bin"),
            secrets.token_bytes(PROBE_BODY_BYTES),
        )

        # Attempted but undelivered: sent_at must still be null.
        parked = await _await_attempted(pc, await pc.get_upload(result.id))
        assert parked.state in {"queued", "attempting"}
        assert parked.sent_at is None, (
            "sent_at must stay null while every delivery attempt bounces; "
            f"got {parked.sent_at!r} at attempts={parked.attempts}"
        )

        # Heal the upstream: delivery stamps sent_at.
        emulator.clear_failures()
        await assert_chain_reaches_state(
            pc, result.id, state="succeeded", timeout_seconds=TERMINAL_BUDGET_SECONDS
        )
        delivered = await pc.get_upload(result.id)
        stamp = delivered.sent_at
        assert stamp is not None, "confirmed delivery must stamp sent_at"
        assert isinstance(stamp, datetime)
        assert delivered.updated_at >= stamp, (
            "the stamp is written with the same write's updated_at; later "
            "row touches may only move updated_at past it"
        )

        # The admission defaults ride the same detail over the wire.
        assert delivered.group_id == result.id, "group_id defaults to chain_id"
        assert delivered.multifile_id is None, "optionless submit stays standalone"
        assert delivered.send_order == 0

        # The singleton group rollup agrees with the member stamp.
        rollup = await pc.get_group_status(result.id)
        assert rollup.last_sent_at == stamp
    finally:
        await stack.tear_down()


async def test_replay_then_resucceed_preserves_sent_at_over_the_wire() -> None:
    """The replay regression end to end: the original stamp survives re-delivery.

    Deliver once (stamp lands); hold the upstream down and replay via
    the admin API; observe the stamp UNMOVED while the replayed row is
    actively bouncing (mid-flight, not just at the end); heal the
    upstream and observe the second delivery preserve the original
    stamp while updated_at moves past it.
    """
    stack = await boot_stack(
        config_overrides={
            "retention": {
                "reaper_interval_seconds": REAPER_DISABLED_INTERVAL_SECONDS,
                "succeeded_body_seconds": REPLAYABLE_BODY_RETENTION_SECONDS,
                "succeeded_metadata_seconds": REPLAYABLE_BODY_RETENTION_SECONDS,
            },
        },
    )
    try:
        pc: PhantomClient = stack.phantom_client
        emulator = stack.emulator
        emulator.clear_received()
        emulator.clear_failures()
        driver = _driver_for(stack)

        # First delivery: the stamp lands.
        result = await driver.in_memory_upload(
            build_create_file_request(file_name="sent-at-replayed.bin"),
            secrets.token_bytes(PROBE_BODY_BYTES),
        )
        await assert_chain_reaches_state(
            pc, result.id, state="succeeded", timeout_seconds=TERMINAL_BUDGET_SECONDS
        )
        first = await pc.get_upload(result.id)
        original_stamp = first.sent_at
        assert original_stamp is not None

        # Hold the upstream down BEFORE the replay so the re-attempt
        # window is observable instead of racing straight to succeeded.
        emulator.inject_failure(
            FailurePolicy(  # type: ignore[call-arg]
                scope=FailureScope.GLOBAL,
                error_rate_5xx=FORCE_5XX_RATE,
            )
        )
        replayed_row = await pc.replay(result.id)
        assert replayed_row.state == "queued", "replay re-queues the settled row"

        # Mid-flight: the row is moving again and the stamp is untouched.
        moving = await _await_attempted(pc, await pc.get_upload(result.id))
        assert moving.state in {"queued", "attempting"}
        assert moving.sent_at == original_stamp, (
            "replay must NOT clear or move sent_at; the stamp records the "
            f"FIRST confirmed delivery (got {moving.sent_at!r}, "
            f"expected {original_stamp!r})"
        )

        # Heal the upstream: the re-delivery completes and the stamp is
        # write-once (the second success must not move it).
        emulator.clear_failures()
        await assert_chain_reaches_state(
            pc, result.id, state="succeeded", timeout_seconds=TERMINAL_BUDGET_SECONDS
        )
        resucceeded = await pc.get_upload(result.id)
        assert resucceeded.sent_at == original_stamp, (
            "the second delivery must preserve the original stamp "
            f"(got {resucceeded.sent_at!r}, expected {original_stamp!r})"
        )
        assert resucceeded.updated_at > first.updated_at, (
            "updated_at keeps moving across the replay cycle; sent_at does not"
        )
        assert resucceeded.attempts >= 1
    finally:
        await stack.tear_down()


async def test_replay_of_discarded_body_refused_409_row_untouched() -> None:
    """Replaying a body-discarded row is refused over the wire; nothing moves.

    Cycle-7 phase 7 pre-round defender fix. Under the suite-default zero
    retention the sender's immediate-discard leg drops the body the
    moment the chain succeeds (the reaper is disabled here, so the stamp
    can only come from that leg). Pre-fix, the admin replay re-queued
    the row and the sender's next claim found no body, landing the row
    in ``corrupted`` while ``sent_at`` survived: an operator action
    laundered into a corruption signal. Post-fix the replay gets the
    canonical 409 ``replay_body_discarded`` envelope (the SDK raises
    :class:`PhantomConflictError` with that code, naming the chain and
    the discard instant) and the row stays field-for-field identical,
    ``sent_at`` preserved.
    """
    stack = await boot_stack(
        config_overrides={
            "retention": {"reaper_interval_seconds": REAPER_DISABLED_INTERVAL_SECONDS},
        },
    )
    try:
        pc: PhantomClient = stack.phantom_client
        emulator = stack.emulator
        emulator.clear_received()
        emulator.clear_failures()
        driver = _driver_for(stack)

        # Deliver once: the stamp lands and the immediate leg discards.
        result = await driver.in_memory_upload(
            build_create_file_request(file_name="sent-at-discarded.bin"),
            secrets.token_bytes(PROBE_BODY_BYTES),
        )
        await assert_chain_reaches_state(
            pc, result.id, state="succeeded", timeout_seconds=TERMINAL_BUDGET_SECONDS
        )
        before = await _await_body_discarded(pc, result.id)
        assert before.state == "succeeded"
        assert before.sent_at is not None
        # The zeroed body_size_bytes accounting is pinned at the unit and
        # contract tiers; the SDK's UploadRow mirror does not carry the
        # size field, so the wire truth here is the stamp itself.

        # The replay is refused with the typed conflict, not re-queued.
        with pytest.raises(PhantomConflictError) as excinfo:
            await pc.replay(result.id)
        refusal = excinfo.value
        assert refusal.status_code == 409
        assert refusal.error_code == "replay_body_discarded"
        assert refusal.details["chain_id"] == str(result.id)
        assert before.body_discarded_at is not None
        assert (
            datetime.fromisoformat(refusal.details["body_discarded_at"]) == before.body_discarded_at
        ), "the envelope names the exact discard instant the row carries"
        assert str(result.id) in refusal.error_message

        # The refusal was a pure read: the row is untouched, stamp intact.
        after_rows, _cursor = await pc.list_uploads(group_id=result.id)
        assert len(after_rows) == 1
        after = after_rows[0]
        assert after.model_dump() == before.model_dump(), (
            "a refused replay must not move the row: state stays succeeded, "
            "sent_at and updated_at unchanged"
        )
        assert after.sent_at == before.sent_at
        assert after.state == "succeeded"
    finally:
        await stack.tear_down()
