"""RAM-ceiling fresh-attempt filter (review finding S12-4).

CONTEXT.md states that the RAM ceiling "is an enforced bound, not a
best-effort gauge: a stalled attempt cannot pin RAM past it". The mechanism
that makes that sentence true is
:func:`phantom.workers.ram_pressure._is_fresh_attempt`. Under pressure the
watcher skips a candidate ONLY when the row is mid-attempt AND that attempt
began inside a time-bounded window of roughly two poll intervals. A row that
has been ``attempting`` for longer is a stalled attempt against a slow or
unreachable upstream, and it is migrated anyway.

Why this file exists: branch coverage over the whole service unit lane
showed ``_is_fresh_attempt`` never called. The sibling reload test seeds its
candidate ``queued`` on purpose, so the filter cannot fire there, and the
only test that exercised it is stress-marked. The default suite excludes
stress, e2e-core excludes it, and so does every per-PR job, so the
mechanism was pinned only by the nightly cron. A regression restoring the
unconditional attempting-skip, which is the exact round 2 bug the stress
test's own docstring records, passed lint, types, unit, integration,
contract, e2e-core and e2e-docker.

The three tests are the two arms of the filter plus the property that its
window is derived from the LIVE poll cadence rather than frozen.

The watcher writes nothing to ``uploads``: it only signals the
:class:`PersistController`. So the observable here is the set of chain_ids
handed to ``enqueue``, and the fakes below are deliberately inert.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid4

from phantom.config.settings import BodyStoreCfg
from phantom.instances.snapshot import InstanceSettingsSnapshot
from phantom.storage.interface import PersistCandidateState
from phantom.workers.ram_pressure import RamPressureWatcher

from .conftest import make_snapshot

# Ceiling under test. 256 KiB.
_CEILING_BYTES: int = 256 * 1024
# Parked RAM, above the ceiling, so every test below is a real breach and
# the sweep always runs. 512 KiB.
_PARKED_RAM_BYTES: int = 512 * 1024
# Default poll cadence. The fresh-attempt window is two poll intervals with
# a one-second floor, so at this cadence the window is 2 s.
_POLL_SECONDS: float = 1.0
# A much slower cadence, so the SAME row age falls inside the window. The
# window here is 60 s.
_SLOW_POLL_SECONDS: float = 30.0

# Attempt ages. The stalled age is far outside the 2 s window at the default
# cadence and far inside the 60 s window at the slow cadence, so the same
# number serves both directions without sitting near a boundary.
_STALLED_ATTEMPT_AGE_SECONDS: int = 45
# A brand-new attempt: the sender is plausibly reading the body right now.
_FRESH_ATTEMPT_AGE_SECONDS: int = 0

assert _PARKED_RAM_BYTES >= _CEILING_BYTES, (
    "parked RAM must breach the ceiling or the sweep never runs and every "
    "assertion below holds vacuously"
)


class _RecordingPersistController:
    """Records every ``enqueue`` chain_id; otherwise inert."""

    def __init__(self) -> None:
        self.enqueued: list[UUID] = []

    async def enqueue(self, chain_id: UUID) -> None:
        """Record the migration request the watcher issued."""
        self.enqueued.append(chain_id)


class _StaticRamBodyStore:
    """RAM body store whose ``total_bytes`` is a fixed parked value."""

    def __init__(self, *, total: int) -> None:
        self._total = total

    async def total_bytes(self) -> int:
        """Return the fixed parked RAM byte total."""
        return self._total


class _OneCandidateStore:
    """Upload store exposing exactly one oldest RAM-resident chain.

    Unlike the sibling reload test's fake, the candidate's state and
    attempt-start stamp are CALLER-CHOSEN, which is what lets these tests
    reach the fresh-attempt filter at all.
    """

    def __init__(self, *, chain_id: UUID, state: str, attempt_age_seconds: float) -> None:
        self._chain_id = chain_id
        self._state = state
        self._updated_at = datetime.now(tz=UTC) - timedelta(seconds=attempt_age_seconds)

    async def list_oldest_ram_bodies(self, limit: int) -> list[UUID]:
        """Return the one oldest RAM-resident chain_id (ignores ``limit``)."""
        del limit
        return [self._chain_id]

    async def get_persist_candidate_state(self, chain_id: UUID) -> PersistCandidateState | None:
        """Return the candidate's state and attempt-start stamp."""
        if chain_id != self._chain_id:
            return None
        return PersistCandidateState(state=self._state, updated_at=self._updated_at)


class _FakeInstance:
    """Minimal instance surface the watcher's ``_check_once`` touches."""

    def __init__(
        self,
        *,
        snapshot: InstanceSettingsSnapshot,
        ram_body_store: _StaticRamBodyStore,
        store: _OneCandidateStore,
    ) -> None:
        self._snapshot = snapshot
        self.ram_body_store = ram_body_store
        self.store = store

    def current_settings(self) -> InstanceSettingsSnapshot:
        """Return the live snapshot the worker reads per tick."""
        return self._snapshot


def _snapshot(*, poll_seconds: float) -> InstanceSettingsSnapshot:
    """Hybrid snapshot carrying the ceiling under test and a poll cadence."""
    return make_snapshot(
        body_store=BodyStoreCfg(
            mode="hybrid",
            ram_ceiling_bytes=_CEILING_BYTES,
            ram_pressure_poll_seconds=poll_seconds,
        )
    )


async def _sweep_once(
    *, state: str, attempt_age_seconds: float, poll_seconds: float
) -> tuple[UUID, list[UUID]]:
    """Run one pressure tick over a single candidate; return what was enqueued.

    Args:
        state: The candidate row's state.
        attempt_age_seconds: How long ago the row's ``updated_at`` was
            stamped, which for an ``attempting`` row is its attempt start.
        poll_seconds: Live poll cadence, which sets the fresh-attempt
            window width.

    Returns:
        ``(candidate chain_id, chain_ids handed to enqueue)``.
    """
    chain_id = uuid4()
    instance = _FakeInstance(
        snapshot=_snapshot(poll_seconds=poll_seconds),
        ram_body_store=_StaticRamBodyStore(total=_PARKED_RAM_BYTES),
        store=_OneCandidateStore(
            chain_id=chain_id, state=state, attempt_age_seconds=attempt_age_seconds
        ),
    )
    controller = _RecordingPersistController()
    watcher = RamPressureWatcher(
        instance=instance,  # type: ignore[arg-type]  # duck-typed minimal surface
        persist_controller=controller,  # type: ignore[arg-type]  # recording stand-in
    )
    await watcher._check_once()
    return chain_id, controller.enqueued


async def test_stalled_attempt_is_migrated_so_the_ceiling_stays_a_bound() -> None:
    """Objective: a stalled attempt cannot pin RAM above the ceiling.

    Expected outcome: a candidate that has been ``attempting`` for far
    longer than the fresh-attempt window is enqueued for migration even
    though it is mid-attempt. This is the half that makes CONTEXT.md's
    "enforced bound, not a best-effort gauge" true.

    Falsifier: restore the unconditional attempting-skip, the round 2 bug,
    and nothing is enqueued while RAM stays over the ceiling, so RED.
    """
    chain_id, enqueued = await _sweep_once(
        state="attempting",
        attempt_age_seconds=_STALLED_ATTEMPT_AGE_SECONDS,
        poll_seconds=_POLL_SECONDS,
    )
    assert enqueued == [chain_id], (
        f"a row stuck in 'attempting' for {_STALLED_ATTEMPT_AGE_SECONDS}s against a "
        f"{_POLL_SECONDS * 2}s fresh window is a stalled attempt and must be migrated "
        f"so the {_CEILING_BYTES}-byte ceiling is enforced; the watcher enqueued {enqueued}"
    )


async def test_fresh_attempt_is_skipped_so_the_sender_is_not_raced() -> None:
    """Objective: a just-started attempt is left alone for one window.

    Expected outcome: a candidate whose attempt began inside the
    fresh-attempt window is NOT enqueued, so the watcher does not race a
    body read the sender has plausibly just begun. This is the arm that
    stops the filter degrading into "always migrate", which would make the
    sibling test above pass for the wrong reason.

    Falsifier: drop the skip entirely and this goes RED.
    """
    _chain_id, enqueued = await _sweep_once(
        state="attempting",
        attempt_age_seconds=_FRESH_ATTEMPT_AGE_SECONDS,
        poll_seconds=_POLL_SECONDS,
    )
    assert enqueued == [], (
        "an attempt that started just now is inside the fresh window and must be "
        f"skipped; the watcher enqueued {enqueued}"
    )


async def test_the_fresh_window_widens_with_the_live_poll_cadence() -> None:
    """Objective: the fresh-attempt window is derived from the LIVE poll interval.

    Expected outcome: the SAME attempt age that counts as stalled at the
    default cadence counts as fresh once the operator reloads a much
    slower ``ram_pressure_poll_seconds``, so the row is skipped. Without
    this, a hard-coded window would satisfy both tests above while
    ignoring the knob ADR-013 lists as read per tick.

    Falsifier: freeze the window to a constant and this goes RED, because
    the row is enqueued at both cadences.
    """
    _chain_id, enqueued = await _sweep_once(
        state="attempting",
        attempt_age_seconds=_STALLED_ATTEMPT_AGE_SECONDS,
        poll_seconds=_SLOW_POLL_SECONDS,
    )
    assert enqueued == [], (
        f"at a {_SLOW_POLL_SECONDS}s poll cadence the fresh window is "
        f"{_SLOW_POLL_SECONDS * 2}s, so a {_STALLED_ATTEMPT_AGE_SECONDS}s-old attempt is "
        f"still fresh and must be skipped; the watcher enqueued {enqueued}"
    )
