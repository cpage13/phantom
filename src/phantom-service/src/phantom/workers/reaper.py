"""Reaper - retention sweep + idempotency-index trim.

Plan § 2.3.16. Collapsed to a single persistent
store; the dual-store ``(memory_store, disk_store)`` round-robin is
gone. The time-based ``after_seconds`` force-persist branch is also
gone - the :class:`PersistController` owns retry-linger detection now
(plan § 2.3.11), not the reaper.

The reaper writes:

* :meth:`UploadStore.discard_body_and_zero_accounting` (stamps
  ``body_discarded_at`` AND zeroes ``body_size_bytes``) for rows whose
  body retention has elapsed. The SCHEDULED leg of the one body-discard
  operation (cycle-7 task 4.7); the sender's immediate discard on
  ``succeeded_body_seconds == 0`` is the other leg, and neither carries
  its own variant.
* :meth:`UploadStore.delete_terminal_older_than` (DELETE) for rows
  whose metadata retention has elapsed.
* :meth:`BodyStore.delete` for the body files of rows being body-
  discarded.

Per the single-writer manifest (plan § 0.5), the reaper does NOT
migrate bodies, does NOT write ``body_location``, and does NOT touch
the ``state`` machine columns. Migration is the persist controller's
job; state transitions are the sender's.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
from datetime import UTC, datetime, timedelta

from phantom.config.settings import RetentionCfg
from phantom.instances.context import InstanceContext
from phantom.models.upload import UploadState
from phantom.observability.metrics import MetricsRegistry
from phantom.workers.saturation import SlotDelta

logger = logging.getLogger(__name__)

# Reaper action labels (plan § 4.2.2).
_REAPER_ACTION_BODY_DISCARDED: str = "body_discarded"
_REAPER_ACTION_ROW_DELETED: str = "row_deleted"

# Fallback interval used only when the reaper has no instances to consult.
# Operationally the instance list is non-empty (loaded from YAML); this is
# defensive scaffolding so the loop never busy-spins.
_FALLBACK_INTERVAL_SECONDS = 60


class RetentionConfigError(ValueError):
    """A per-state retention window consulted by the sweep is unresolved.

    The reaper's retention table requires every metadata and body window
    to be a resolved integer (seconds; ``-1`` means never expire).
    ``None`` means the runtime config failed to resolve a window;
    sweeping with it would silently skip retention for that state, so
    the sweep aborts loudly instead. Replaces the former
    ``assert ... is not None`` guards, which ``python -O`` strips.
    """


class Reaper:
    """Periodic retention sweep on the single persistent store.

    The reaper reads its operational config (per-state retention windows,
    sweep interval) from each instance's live snapshot via
    :meth:`InstanceContext.current_settings`. Hot reloads land on the
    next sweep without restarting the loop.
    """

    def __init__(
        self,
        *,
        instances: list[InstanceContext],
        metrics_registry: MetricsRegistry | None = None,
    ) -> None:
        """Construct the reaper.

        Args:
            instances: Every configured instance to sweep.
            metrics_registry: Optional :class:`MetricsRegistry` for
                emit-site wiring (plan § 4.2.2). When ``None`` a
                throwaway registry is constructed so emission is a
                no-op.
        """
        self._instances = instances
        # Metrics surface (plan § 4.2.2). Label values: body_discarded,
        # row_deleted (see module-level constants).
        self._metrics = metrics_registry if metrics_registry is not None else MetricsRegistry()
        self._reaper_actions_total = self._metrics.register_counter(
            "reaper_actions_total",
            "Reaper actions (body_discarded, row_deleted).",
        )

    async def run(self, stop_event: asyncio.Event) -> None:
        """Main loop - sleep, then sweep, until stopped."""
        while not stop_event.is_set():
            try:
                await self._sweep_once()
            except Exception:
                logger.exception("Reaper sweep failed")
            interval = self._current_interval_seconds()
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(stop_event.wait(), timeout=interval)

    def _current_interval_seconds(self) -> int:
        """Return the sweep interval from the first instance's live snapshot.

        Every instance's snapshot shares the same top-level retention
        block by reference (see :func:`phantom.instances.snapshot._build_snapshot`),
        so the first instance is representative. Falls back to
        :data:`_FALLBACK_INTERVAL_SECONDS` when no instances are configured.
        """
        if not self._instances:
            return _FALLBACK_INTERVAL_SECONDS
        return self._instances[0].current_settings().retention.reaper_interval_seconds

    async def _sweep_once(self) -> None:
        """Run a single retention + idempotency-index sweep."""
        now = datetime.now(tz=UTC)
        for instance in self._instances:
            await self._sweep_instance(instance, now)

    async def _sweep_instance(self, instance: InstanceContext, now: datetime) -> None:
        """Sweep the single persistent store for one instance."""
        snapshot = instance.current_settings()
        cfg: RetentionCfg = snapshot.retention
        store = instance.store
        body_store = instance.body_store
        retention_table: tuple[tuple[UploadState, int | None, int | None], ...] = (
            (
                "succeeded",
                cfg.succeeded_metadata_seconds,
                cfg.succeeded_body_seconds,
            ),
            (
                "failed",
                cfg.failed_metadata_seconds,
                cfg.failed_body_seconds,
            ),
            (
                "cancelled",
                cfg.cancelled_metadata_seconds,
                cfg.cancelled_body_seconds,
            ),
            (
                "stored",
                cfg.stored_metadata_seconds,
                cfg.stored_body_seconds,
            ),
            (
                "auth_expired",
                cfg.auth_expired_metadata_seconds,
                cfg.auth_expired_body_seconds,
            ),
            (
                "corrupted",
                cfg.corrupted_metadata_seconds,
                cfg.corrupted_body_seconds,
            ),
            # ADR-032: the body is already discarded at the transition to
            # ``expired`` (expired_body_seconds defaults to 0), so this row
            # only sweeps the retained metadata after expired_metadata_seconds.
            (
                "expired",
                cfg.expired_metadata_seconds,
                cfg.expired_body_seconds,
            ),
        )
        for state, metadata_seconds, body_seconds in retention_table:
            # Sweep-time guard (strategy D5): every window consulted here
            # must be a resolved integer. RetentionCfg types them all as
            # plain int today; the table keeps the historical int | None
            # annotation so this stays a checked runtime contract. The
            # typed raises, unlike the asserts they replaced, survive
            # python -O and fail the sweep loudly instead of reaping with
            # a half-resolved config.
            if metadata_seconds is None:
                raise RetentionConfigError(
                    f"retention metadata window for state {state!r} is unresolved (None); "
                    "every per-state window must resolve to an integer"
                )
            if body_seconds is None:
                raise RetentionConfigError(
                    f"retention body window for state {state!r} is unresolved (None); "
                    "every per-state window must resolve to an integer"
                )
            # Body discard pass: the SCHEDULED leg of the one body-discard
            # operation (cycle-7 task 4.7). The reaper owns deletion when
            # the per-state body retention window is NON-ZERO (the sender
            # already discarded immediately when succeeded_body_seconds ==
            # 0, stamping the row so this pass skips it). Stamp-first per
            # row (R9-5) - the per-row comment below carries the guard,
            # release, and crash posture.
            if body_seconds >= 0:
                cutoff = now - timedelta(seconds=body_seconds)
                chain_ids_to_discard = await store.list_terminal_older_than(state, cutoff)
                for chain_id in chain_ids_to_discard:
                    # R9-5 confirm-then-act: stamp FIRST, atomically
                    # guarded on the swept state + an unstamped row. A
                    # replay or kicker wake that revived the row between
                    # the snapshot above and this write makes the guard
                    # mismatch, and the live row keeps its bodies; a
                    # bulk delete or second sweep cannot induce a
                    # double release (the stamp and the release basis
                    # are captured in one transaction). Body files are
                    # deleted only after a confirmed flip; a crash in
                    # between leaves a stamped row whose files the
                    # metadata pass and the orphan janitor converge
                    # (bounded, self-healing).
                    outcome = await store.discard_body_and_zero_accounting(
                        chain_id, expected_state=state
                    )
                    if not outcome.flipped:
                        continue
                    # R8-4: a stored row's saturation slot represents
                    # the space its buffered body occupies; the discard
                    # frees that space, so the slot releases HERE, on
                    # the in-transaction size (row_holds_slot treats
                    # the stamped row as slotless from now on). The
                    # other states in this pass released at their
                    # terminal transition or auth_expired park.
                    await instance.saturation.settle(
                        SlotDelta.from_discard(outcome, size_bytes=outcome.body_size_bytes)
                    )
                    await body_store.delete(chain_id)
                    await self._reaper_actions_total.inc(label_value=_REAPER_ACTION_BODY_DISCARDED)
            # Metadata-row deletion pass.
            if metadata_seconds >= 0:
                cutoff_meta = now - timedelta(seconds=metadata_seconds)
                removed = await store.delete_terminal_older_than(state, cutoff_meta)
                for entry in removed:
                    # R8-4: a stored row whose body was never separately
                    # discarded (body window longer than the metadata
                    # window, or infinite) still holds its slot at
                    # deletion; release it with the row.
                    await instance.saturation.settle(
                        SlotDelta.from_removal(entry, size_bytes=entry.body_size_bytes)
                    )
                if removed:
                    await self._reaper_actions_total.inc(
                        label_value=_REAPER_ACTION_ROW_DELETED, n=len(removed)
                    )
                    logger.info(
                        "Reaped %d %s rows older than %ds",
                        len(removed),
                        state,
                        metadata_seconds,
                    )

        # Count-cap backstop (V3). The time-based passes above are the primary
        # retention mechanism; ``max_rows`` is an optional hard ceiling on the
        # ``uploads`` table so terminal rows (notably the forever-retained
        # ``stored``/``auth_expired`` metadata) cannot grow it without bound
        # between reaps. ``-1`` = unbounded (the store call is a no-op), so the
        # historical time-only contract is preserved by default. Only
        # fully-terminal rows are evicted (the store enforces this), so the cap
        # never drops an undelivered upload. We delete the evicted rows' bodies
        # here (the reaper owns body deletion); the idempotency-index trim below
        # then drops their now-dangling index entries in the same sweep.
        max_rows = cfg.max_rows
        if max_rows >= 0:
            evicted = await store.evict_terminal_over_limit(max_rows)
            for entry in evicted:
                # R10-D1 (the R8-3 family): the eviction DELETE
                # legalized a same-chain_id re-POST, so the late body
                # delete re-reads the live table immediately before
                # acting and steps aside when a new owner exists - the
                # new row's accepted bytes must not be wiped by the
                # evicted row's cleanup. The step-aside is safe for
                # the new owner because re-admission cleared the
                # chain_id namespace before its put (R11-1), so the
                # evicted row's leftovers cannot poison it (the
                # pre-R11-1 "new row's own body lifecycle removes
                # them" justification was false for differing ref-name
                # sets). Same guard and residual-sliver posture as the
                # admin bulk delete (routes/admin.py); the release
                # stays keyed on the atomically captured eviction
                # accounting.
                if await store.get(entry.chain_id) is None:
                    await body_store.delete(entry.chain_id)
                # R8-4: same rule as the metadata pass - an evicted
                # stored row still holding its slot releases it here.
                await instance.saturation.settle(
                    SlotDelta.from_removal(entry, size_bytes=entry.body_size_bytes)
                )
            if evicted:
                await self._reaper_actions_total.inc(
                    label_value=_REAPER_ACTION_ROW_DELETED, n=len(evicted)
                )
                logger.info(
                    "Reaped %d oldest-terminal rows over the max_rows=%d cap",
                    len(evicted),
                    max_rows,
                )

        # Idempotency-index cleanup. Single-store collapse (plan § 2.3.6):
        # every live chain_id is in this store's ``uploads`` table, so the
        # preserve carve-out is empty by construction. ``cleanup_idempotency_index``
        # drops every index row whose linked upload was reaped.
        await store.cleanup_idempotency_index()
