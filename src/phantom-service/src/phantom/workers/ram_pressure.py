"""RamPressureWatcher — enforce the ``body_store.ram_ceiling_bytes`` cap.

The cap exists in YAML (:attr:`BodyStoreCfg.ram_ceiling_bytes`,
renamed from the pre-Phase-1 ``in_memory_max_bytes``) so operators
can bound the RAM body store's bytes-resident footprint. Without
enforcement the saturation gate's ``max_in_flight_bytes`` (10 GiB
default) was the only ceiling, which exceeded available RAM on small
deployments.

Plan § 2.3.12. The watcher's run loop:

* Reads poll cadence from
  :attr:`BodyStoreCfg.ram_pressure_poll_seconds` on every tick (via the
  ``current_settings`` thunk so hot reload reflects without restart).
* Reads the ceiling from :attr:`BodyStoreCfg.ram_ceiling_bytes` the
  same way, on every tick (R6-2): a hot-reloaded ceiling rebinds
  enforcement immediately and keeps the watcher's gauges aligned with
  the ``/observability/ram_pressure`` surface, which reads the same
  live snapshot. ADR-013 lists all three body-store tuning knobs as
  read-per-tick; the ceiling was previously captured at construction
  and silently ignored reload.
* Samples :meth:`RamBodyStore.total_bytes` from
  :attr:`InstanceContext.ram_body_store` (the RAM half specifically,
  kept on InstanceContext for callers that need the half-specific
  surface).
* When the sample exceeds the ceiling, fetches oldest-first RAM
  chain_ids via the store's :meth:`SqliteUploadStore.list_oldest_ram_bodies`
  helper (plan § 2.3.4) and calls
  :meth:`PersistController.enqueue` on each. The controller dedupes
  duplicate in-flight chain_ids and the next tick re-samples — no
  inline awaits on the migration future.
* Skips a candidate ONLY when it is mid-attempt AND its attempt began
  within a time-bounded "fresh" window (~2x the poll interval). A row
  that has been ``attempting`` longer than that is a stalled attempt
  (slow/unreachable upstream) and is enqueued anyway so the ceiling is
  actually enforced (finding F-1 / F-P8-A). Migrating a stalled-attempt
  body is safe: the :class:`HybridBodyStore` writes+fsyncs to disk
  BEFORE deleting RAM, so the sender's RAM-first read falls back to
  disk, and the controller dedupes a later failure-handler re-enqueue.
  An unconditional skip (the pre-fix behavior) let a slow upstream pin
  every oldest RAM row in ``attempting`` and RAM blew past the ceiling
  unbounded.
* Writes NOTHING to ``uploads``. The PersistController owns the
  ``body_location='file'`` flip per the single-writer manifest
  (plan § 0.5 invariant #6).

The watcher is wired ONLY in ``hybrid`` mode (composition root /
``app.py`` per plan § 2.3.10): ``all_ram`` has no migration target;
``all_disk`` has no RAM to bound. The :class:`PersistController`
constructor argument is therefore mandatory.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
from datetime import UTC, datetime
from typing import TYPE_CHECKING

from phantom.observability.metrics import MetricsRegistry

if TYPE_CHECKING:
    from phantom.instances.context import InstanceContext
    from phantom.workers.persist_controller import PersistController

logger = logging.getLogger(__name__)

# Batch size cap for one pressure event — bounds the amount of work
# done before the watcher re-samples and decides whether to keep
# enqueueing. Picked an order-of-magnitude that comfortably exceeds
# typical RAM-resident chain counts (the watcher just exits the
# inner loop when total_bytes drops back below the cap).
_MAX_ENQUEUE_BATCH_SIZE: int = 64

# Multiplier on the poll interval defining the "fresh attempt" window
# for the time-bounded attempting-skip (finding F-1). A row that
# transitioned to ``attempting`` within this many poll intervals is
# skipped (the sender is plausibly actively reading its body); a row
# that has been ``attempting`` longer is enqueued anyway — the upstream
# is genuinely stalled, RAM must be bounded, and migrating is safe (the
# HybridBodyStore writes+fsyncs to disk BEFORE deleting RAM, so the
# sender's RAM-first read falls back to disk; the controller dedupes
# duplicate enqueues). Two intervals gives a fresh attempt at least one
# full poll cycle of grace before the watcher will migrate underneath it.
_ATTEMPTING_SKIP_POLL_MULTIPLIER: float = 2.0

# Floor on the fresh-attempt window in seconds. Guards against a
# pathologically small poll interval making the window so tight that
# every in-flight attempt is treated as stalled. 1 second comfortably
# exceeds the body-read + first-byte latency of a healthy upstream.
_ATTEMPTING_SKIP_FLOOR_SECONDS: float = 1.0


def _is_fresh_attempt(
    updated_at: datetime,
    *,
    now: datetime,
    window_seconds: float,
) -> bool:
    """Return True if an ``attempting`` row's attempt began recently.

    The attempt-start time is the row's ``updated_at`` (set on the
    queued→attempting flip by ``SqliteUploadStore.claim_due``). A "fresh"
    attempt is one that started within ``window_seconds`` of ``now`` — it
    is skipped by the RAM-pressure sweep so the watcher does not migrate
    a body the sender may be actively reading. An attempt older than the
    window is "stalled" (returns ``False``) and gets enqueued so the RAM
    ceiling is enforced (finding F-1).

    Both datetimes are normalized to UTC; a naive ``updated_at`` is
    assumed to already be UTC wall-clock (defensive — the store reads
    tz-aware values, but this keeps the comparison total).

    Args:
        updated_at: The row's ``updated_at`` (attempt-start time).
        now: The current instant (tz-aware UTC), sampled once per sweep.
        window_seconds: The fresh-attempt window width.

    Returns:
        ``True`` when the attempt is fresh (skip it), ``False`` when it
        is stalled (enqueue it).
    """
    if updated_at.tzinfo is None:
        updated_at = updated_at.replace(tzinfo=UTC)
    age_seconds = (now - updated_at).total_seconds()
    return age_seconds < window_seconds


class RamPressureWatcher:
    """Periodic coroutine that drives RAM→disk migrations under pressure."""

    def __init__(
        self,
        *,
        instance: InstanceContext,
        persist_controller: PersistController,
        metrics_registry: MetricsRegistry | None = None,
    ) -> None:
        """Construct the watcher.

        Every tuning knob (ceiling, poll cadence, fresh-attempt window)
        is read from the instance's live settings snapshot per tick, so
        the constructor takes no config values (R6-2: a
        constructor-captured ceiling silently ignored hot reload; the
        initial-cadence parameter was equally dead).

        Args:
            instance: The instance whose RAM body store, upload store,
                and live settings snapshot to consult.
            persist_controller: Migration target. Mandatory — the
                watcher is only spawned in ``hybrid`` mode (plan
                § 2.3.10). The controller's :meth:`enqueue` is
                idempotent, so duplicate enqueues for an in-flight
                chain are safe.
            metrics_registry: Optional :class:`MetricsRegistry` for
                emit-site wiring (plan § 4.2.2). When ``None`` a
                throwaway registry is constructed so emission is a
                no-op.
        """
        self._instance = instance
        self._persist_controller = persist_controller
        # Metrics surface (plan § 4.2.2).
        self._metrics = metrics_registry if metrics_registry is not None else MetricsRegistry()
        self._ram_bytes_gauge = self._metrics.register_gauge(
            "ram_body_store_bytes",
            "Current RamBodyStore.total_bytes() observation.",
        )
        self._ram_ceiling_gauge = self._metrics.register_gauge(
            "ram_ceiling_bytes",
            "Configured RamBodyStore ceiling (body_store.ram_ceiling_bytes).",
        )
        self._ram_signal_total = self._metrics.register_counter(
            "ram_pressure_signal_total",
            "RAM-pressure-threshold-breach events.",
        )

    async def run(self, stop_event: asyncio.Event) -> None:
        """Main loop — sleep, sample, enqueue on pressure, until stopped."""
        while not stop_event.is_set():
            try:
                await self._check_once()
            except Exception:
                # Broad except so transient I/O does not kill the watcher.
                logger.exception("RamPressureWatcher tick failed")
            # Re-read cadence from live settings so a hot reload
            # ``body_store.ram_pressure_poll_seconds`` propagates without
            # restarting the watcher loop.
            interval = self._instance.current_settings().body_store.ram_pressure_poll_seconds
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(stop_event.wait(), timeout=interval)

    async def _check_once(self) -> None:
        """Sample RAM-tier body bytes; enqueue oldest until under cap."""
        # Read the ceiling from the live snapshot on every tick (ADR-013
        # body-store tuning reloads per worker tick; R6-2 fixed the
        # constructor-captured copy that ignored hot reload). None means
        # resolution has not filled it (defensive; the composition root
        # spawns the watcher only post-resolution); zero is the
        # documented "no cap" sentinel. Either disables the watcher.
        max_bytes = self._instance.current_settings().body_store.ram_ceiling_bytes
        if max_bytes is None or max_bytes <= 0:
            return

        current = await self._instance.ram_body_store.total_bytes()
        # Observe live bytes AND the live ceiling on every tick so the
        # gauge surface carries both numerator and denominator, and the
        # gauges agree with /observability/ram_pressure after a reload.
        await self._ram_bytes_gauge.set(current)
        await self._ram_ceiling_gauge.set(max_bytes)
        if current < max_bytes:
            return

        logger.warning(
            "RAM pressure: %d bytes >= cap %d; enqueueing oldest RAM chains",
            current,
            max_bytes,
        )
        # Pressure event observed — bump signal counter.
        await self._ram_signal_total.inc()
        # list_oldest_ram_bodies queries the persistent store for
        # body_location='ram' chain_ids ordered by received_at. The
        # store knows which rows hold RAM bodies; no body-store walk
        # required.
        candidates = await self._instance.store.list_oldest_ram_bodies(
            limit=_MAX_ENQUEUE_BATCH_SIZE,
        )
        # Time-bounded attempting-skip window (finding F-1). A row that
        # entered ``attempting`` within this window is plausibly being
        # read by the sender right now, so we skip it to avoid racing the
        # body read; a row that has been ``attempting`` LONGER is treated
        # as a stalled attempt (slow/unreachable upstream) and enqueued
        # anyway so the ceiling is actually enforced. ``updated_at`` is
        # the attempt-start time — ``claim_due`` sets it on the
        # queued→attempting flip. Read the live poll interval so a hot
        # reload of ``ram_pressure_poll_seconds`` reshapes the window.
        poll_interval = self._instance.current_settings().body_store.ram_pressure_poll_seconds
        fresh_window_seconds = max(
            _ATTEMPTING_SKIP_FLOOR_SECONDS,
            poll_interval * _ATTEMPTING_SKIP_POLL_MULTIPLIER,
        )
        now = datetime.now(tz=UTC)
        for chain_id in candidates:
            # Skip only rows mid-attempt whose attempt began RECENTLY —
            # the sender may be actively reading the body, and racing the
            # controller's migration against a live read invites a
            # duplicate fsync + RAM-delete window. A stalled attempt
            # (updated_at older than the fresh window) is safe to migrate:
            # the HybridBodyStore writes+fsyncs to disk BEFORE deleting
            # RAM, so the sender's RAM-first read falls back to disk, and
            # the controller dedupes if the sender's failure handler
            # later re-enqueues. Without this bound, a slow upstream that
            # pins every oldest RAM row in ``attempting`` lets RAM blow
            # past the ceiling unbounded (F-P8-A).
            candidate = await self._instance.store.get_persist_candidate_state(chain_id)
            if (
                candidate is not None
                and candidate.state == "attempting"
                and _is_fresh_attempt(
                    candidate.updated_at, now=now, window_seconds=fresh_window_seconds
                )
            ):
                continue
            try:
                # PersistController.enqueue is idempotent and
                # fire-and-forget. We do NOT await the returned future;
                # the next tick re-samples to decide whether more
                # enqueues are needed.
                await self._persist_controller.enqueue(chain_id)
            except Exception:
                logger.exception(
                    "RamPressureWatcher: enqueue failed for chain_id=%s; "
                    "continuing with remaining rows",
                    chain_id,
                )
                continue
            # Optimistic re-sample — the controller may have already
            # cleared some RAM. The sample is O(1) since U11 (a running
            # counter, not a walk of every ref), which is what makes
            # re-sampling once per candidate reasonable rather than a cost.
            current = await self._instance.ram_body_store.total_bytes()
            if current < max_bytes:
                logger.info(
                    "RAM pressure trending down at %d bytes (< cap %d)",
                    current,
                    max_bytes,
                )
                return
        # If we fell through, every persistable row was enqueued but the
        # total is still over the cap (in-flight 'attempting' rows alone
        # exceed it). Log and let the next tick try again.
        logger.warning(
            "RAM pressure unresolved after enqueue sweep: %d bytes still >= cap %d",
            current,
            max_bytes,
        )


__all__ = ["RamPressureWatcher"]
