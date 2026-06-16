"""AuthKicker — wake ``auth_expired`` rows when the cache lands a fresh token."""

from __future__ import annotations

import asyncio
import contextlib
import logging
from datetime import UTC, datetime

from phantom.instances.context import InstanceContext
from phantom.workers.saturation import AdmissionGranted

logger = logging.getLogger(__name__)

# Periodic-rescan interval. The kicker reacts to cache-write events
# directly, but a parked row whose cache slot was already fresh at
# park time (e.g., the admin push landed before the row finished
# transitioning to ``auth_expired``) would otherwise wait forever
# without a periodic check. One second is short enough that recovery
# stays within the test budgets in plan §3.31 / §3.32 and long enough
# that healthy steady-state idle work stays negligible.
_RESCAN_INTERVAL_SECONDS = 1.0


class AuthKicker:
    """Re-queue ``auth_expired`` rows whose cache slot is fresh.

    Reacts to two signals:

    - **Cache writes** — ``TokenCache.set`` fires the registered
      wake handler. The kicker stores a "rescan requested" event
      rather than the (endpoint, uid) tuple — the load-bearing
      check is "is the cache fresh for this row's slot?" at scan
      time, not at wake time. Without this, a wake that lands
      between the executor's 401 and the sender's
      ``record_attempt_result`` would be consumed before the row
      reached ``auth_expired`` and the row would never wake.
    - **Periodic tick** — every ``_RESCAN_INTERVAL_SECONDS`` the
      kicker scans regardless. Catches the rare ordering where the
      admin push landed before the row's ``auth_expired`` row
      committed and no further cache write follows.

    Rows whose ``body_discarded_at`` is stamped are never woken
    (R6-3): the body was discarded by retention policy, so a re-queue
    could only end in a false ``corrupted``. They stay parked until
    the metadata-retention pass reaps them, matching the guard every
    other H4-carve-out consumer (recovery, the InvariantAuditor,
    ``replay``) already applies.
    """

    def __init__(self, *, instance: InstanceContext) -> None:
        """Construct the kicker.

        Args:
            instance: The instance whose stores and cache to wire.
        """
        self._instance = instance
        # An :class:`asyncio.Event` rather than a queue — the kicker
        # doesn't care how many wake events accumulate, only whether
        # at least one happened since the last scan. The periodic
        # rescan covers the gap between an early wake and a later
        # park.
        self._wake_event = asyncio.Event()
        # Register against the cache's wake hook.
        instance.token_cache.register_wake_handler(self._on_cache_set)

    async def _on_cache_set(self, endpoint: str, uid: str) -> None:
        """Cache-write callback — set the wake event for the next rescan."""
        del endpoint, uid  # The scan matches against current cache state, not the wake's identity.
        self._wake_event.set()

    async def run(self, stop_event: asyncio.Event) -> None:
        """Main loop: rescan on every wake or every interval until stopped."""
        while not stop_event.is_set():
            # Wait for either a cache write or the rescan interval —
            # whichever fires first triggers the next rescan.
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(
                    self._wake_event.wait(),
                    timeout=_RESCAN_INTERVAL_SECONDS,
                )
            self._wake_event.clear()
            try:
                await self._rescan()
            except Exception:
                logger.exception("AuthKicker rescan failed")

    async def _rescan(self) -> None:
        """Re-queue every ``auth_expired`` row whose cache slot is fresh.

        Single persistent store (plan § 2.3.6): one store holds every
        row regardless of body_location.
        """
        now = datetime.now(tz=UTC)
        store = self._instance.store
        rows = await store.list_non_terminal()
        for row in rows:
            if row.state != "auth_expired":
                continue
            if row.body_discarded_at is not None:
                # H4 carve-out (R6-3): the reaper discarded this row's
                # body by retention policy; nothing is left to deliver.
                # Re-queueing would land the row in ``corrupted`` on the
                # sender's next claim (BodyMissingError), turning a
                # policy-aged record into a false storage-fault
                # diagnostic, and would burn a saturation slot. Mirror
                # ``replay``'s up-front refusal: leave the row parked in
                # ``auth_expired`` until the metadata-retention pass
                # reaps it.
                continue
            slot = await self._instance.token_cache.get(row.endpoint, row.uid)
            if slot is None or slot.status != "fresh":
                continue
            # Re-admit through the saturation gate (§3.1 symmetry).
            # The sender released the gate when the row parked into
            # auth_expired; the row must take a slot back before it
            # rejoins the in-flight set. On refusal: log and leave
            # the row in auth_expired — the next rescan tries again.
            result = await self._instance.saturation.admit(row.body_size_bytes)
            if not isinstance(result, AdmissionGranted):
                logger.warning(
                    "AuthKicker refused wake for chain_id=%s (saturated); "
                    "leaving in auth_expired for next rescan",
                    row.chain_id,
                )
                continue
            logger.info(
                "AuthKicker waking row chain_id=%s for endpoint=%s uid=%s",
                row.chain_id,
                row.endpoint,
                row.uid,
            )
            # M-W4-F7 (Phase 2 § 3.2.8): the kicker writes from
            # auth_expired → queued. Pass the expected_state explicitly
            # so the default ``attempting`` predicate doesn't reject the
            # update; the kicker is the only allowed mover of the
            # auth_expired → queued transition.
            #
            # ONE refusal posture spans the admit->write window: the
            # slot admitted above returns on EVERY outcome except a
            # confirmed wake (invariant #16). Two refusal legs exist -
            # the guarded write no-ops (rowcount 0: admin cancel/replay
            # or another kicker tick moved the row first; R9-3), or the
            # write RAISES (a transient storage fault - SQLITE_BUSY
            # past the busy_timeout, or an I/O error on flaky SD
            # storage; R10-2) - and both release before yielding,
            # mirroring the replay route's release-on-exception
            # (routes/admin.py replay_upload). Pre-R10-2 the exception
            # leg propagated with the slot stranded; run() logs and
            # continues while the row stays auth_expired with a fresh
            # token, so every later rescan stranded another slot until
            # the gate saturated and fresh ingress 503'd with no live
            # row behind the count.
            try:
                rowcount = await store.record_attempt_result(
                    row.chain_id,
                    new_state="queued",
                    attempts=row.attempts,
                    next_attempt_at=now,
                    last_error=None,
                    upstream_status=None,
                    upstream_headers_json=None,
                    captured_values=None,
                    current_step_index=None,
                    last_step_completed=None,
                    expected_state="auth_expired",
                )
            except Exception:
                # Release FIRST, then surface the fault. The failed
                # write committed nothing, so the row is untouched and
                # the next rescan retries the wake; run() owns the
                # traceback (logger.exception) - this line adds the
                # chain_id context.
                await self._instance.saturation.release(row.body_size_bytes)
                logger.warning(
                    "AuthKicker wake write failed for chain_id=%s; "
                    "admitted slot returned, row stays auth_expired "
                    "for the next rescan",
                    row.chain_id,
                )
                raise
            if rowcount == 0:
                # The rowcount-0 refusal leg (R9-3): the wake never
                # happened, and whoever moved the row owns its own
                # accounting - this release only undoes OUR admit.
                # Then skip; the next event/poll picks up any further
                # waking work.
                await self._instance.saturation.release(row.body_size_bytes)
                logger.info(
                    "AuthKicker no-op: chain_id=%s - row state changed "
                    "from auth_expired before update; admitted slot "
                    "returned",
                    row.chain_id,
                )
