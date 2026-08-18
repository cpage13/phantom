"""BodyOrphanJanitor - periodic orphan sweep on the body store.

Closes invariant #4 (plan § 0.5): no :class:`FileBodyStore` ref set
exists without a corresponding ``uploads`` row (plan § 2.3.14).

The janitor is wired ONLY in modes where a file body store exists
(``hybrid`` and ``all_disk``). The RAM binding's
:meth:`RamBodyStore.list_orphans` returns ``[]`` by construction (RAM
has no orphans - the dict is purged on chain drop), so the janitor
is a no-op when the body store is RAM-only.

The janitor writes NOTHING to the ``uploads`` table; it operates
exclusively against :class:`BodyStore`. Per the single-writer
manifest (plan § 0.5), no worker other than the canonical owners
mutates ``uploads``.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
from typing import TYPE_CHECKING

from phantom.observability.metrics import MetricsRegistry

if TYPE_CHECKING:
    from collections.abc import Callable
    from uuid import UUID

    from phantom.instances.snapshot import InstanceSettingsSnapshot
    from phantom.storage.interface import BodyStore, UploadStore

logger = logging.getLogger(__name__)


class BodyOrphanJanitor:
    """Periodic sweep: delete body-store entries with no ``uploads`` row.

    Sweep cadence is read on every loop iteration from
    :attr:`InstanceSettingsSnapshot.body_store.body_orphan_sweep_seconds`
    via the ``current_settings`` thunk so a hot reload picks up the new
    cadence without restarting the loop.

    The known-set is a SNAPSHOT of the ``uploads.chain_id`` population
    (per :meth:`UploadStore.list_chain_ids`) taken at the top of the
    sweep, and the disk walk runs after it. A chain admitted or
    migrating between the snapshot and the walk therefore shows up as a
    false orphan (R6-1; the same stale-snapshot family R5-1 fixed in
    the InvariantAuditor). Two guards make the delete safe without any
    filesystem-clock assumption:

    1. **Two-sweep confirmation.** A candidate is deletable only when
       it was also a candidate on the immediately preceding sweep. A
       fresh entry's row is visible to the next sweep's fresh snapshot,
       so it drops out; a real crash leftover persists and is collected
       one cadence later, which the schedule-driven invariant #4
       tolerates by design.
    2. **Live-row re-read.** Immediately before each irreversible
       delete, the live table is re-read; a chain with a row is never
       deleted, however old its files.

    Per-sweep REMOVED count (actual deletions, not candidates) is
    logged at INFO and feeds the ``orphan_body_count_total`` counter
    (plan § 0.5 enforcement mapping).
    """

    def __init__(
        self,
        *,
        store: UploadStore,
        body_store: BodyStore,
        current_settings: Callable[[], InstanceSettingsSnapshot],
        metrics_registry: MetricsRegistry | None = None,
    ) -> None:
        """Construct the janitor.

        Args:
            store: The persistent :class:`UploadStore` whose live
                ``chain_id`` population defines "not an orphan."
            body_store: The mode-selected :class:`BodyStore`. In
                ``hybrid`` mode this is :class:`HybridBodyStore`
                (whose :meth:`list_orphans` proxies to the file half);
                in ``all_disk`` mode it is :class:`FileBodyStore`
                directly.
            current_settings: The live-snapshot thunk. The sweep
                cadence (``body_store.body_orphan_sweep_seconds``) is
                read from it on every loop iteration, per the ADR-031
                canonical distribution mechanism (T1: the previous
                construction-pinned ``period_seconds`` contradicted
                this class's own documented contract and was the one
                worker cadence a hot reload could not retune).
            metrics_registry: Optional :class:`MetricsRegistry` for
                emit-site wiring (plan § 4.2.2). When ``None`` a
                throwaway registry is constructed so emission is a
                no-op.
        """
        self._store = store
        self._body_store = body_store
        self._current_settings = current_settings
        # Candidates seen on the previous sweep (R6-1 two-sweep
        # confirmation). Deleting requires two consecutive sightings, so
        # a chain that merely raced the known-set snapshot is never
        # collected. Reset on process restart by construction, which
        # only defers real-orphan collection by one cadence.
        self._pending_orphans: set[UUID] = set()
        # Metrics surface (plan § 4.2.2). Counts orphans REMOVED per
        # sweep - total across all sweeps since process start.
        self._metrics = metrics_registry if metrics_registry is not None else MetricsRegistry()
        self._orphan_total = self._metrics.register_counter(
            "orphan_body_count_total",
            "Body-store orphans removed by the BodyOrphanJanitor.",
        )

    async def run(self, stop_event: asyncio.Event) -> None:
        """Main loop - sweep, sleep, until stopped.

        The first sweep fires immediately on startup so a fresh-process
        scan catches anything left behind by a prior process. Subsequent
        sweeps fire on the configured cadence.

        Broad ``except`` is intentional: a transient I/O failure (e.g.,
        filesystem disconnect, SQLite busy) must not kill the janitor
        loop. The body-orphan invariant is enforced per-sweep, and a
        skipped sweep just delays detection by one cadence.
        """
        while not stop_event.is_set():
            try:
                await self._sweep_once()
            except Exception:
                logger.exception("BodyOrphanJanitor sweep failed; continuing")
            # Re-read cadence from the live snapshot each iteration so a
            # hot-reloaded body_orphan_sweep_seconds takes effect without
            # restarting the loop (T1 / ADR-031; previously pinned at
            # construction, contradicting this class's documented
            # contract).
            period_seconds = self._current_settings().body_store.body_orphan_sweep_seconds
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(
                    stop_event.wait(),
                    timeout=period_seconds,
                )

    async def _sweep_once(self) -> None:
        """Run one orphan-detection pass.

        Deletion requires BOTH R6-1 guards to pass: the candidate was
        already a candidate on the previous sweep (two-sweep
        confirmation), and the live table has no row for it at delete
        time (live-row re-read). Candidates failing either guard simply
        wait; if they are real orphans the next sweep collects them.
        """
        known = set(await self._store.list_chain_ids())
        candidates = set(await self._body_store.list_orphans(known))
        confirmed = candidates & self._pending_orphans
        removed = 0
        for chain_id in confirmed:
            if await self._store.get(chain_id) is not None:
                # The snapshot was stale: the chain is live, not orphaned.
                continue
            try:
                await self._body_store.delete(chain_id)
            except Exception:
                logger.exception(
                    "BodyOrphanJanitor: failed to delete orphan chain_id=%s",
                    chain_id,
                )
                continue
            removed += 1
        # Carry every still-present candidate (including failed deletes)
        # into the next sweep's confirmation set.
        self._pending_orphans = candidates
        if removed:
            await self._orphan_total.inc(n=removed)
            logger.info(
                "BodyOrphanJanitor sweep removed %d orphan body entries",
                removed,
            )


__all__ = ["BodyOrphanJanitor"]
