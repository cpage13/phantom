"""InvariantAuditor — periodic row walk; counters invariant violations.

Plan § 4.2.3. Runs at low frequency (default 300 s). Walks the live
``uploads`` table; checks each row against the seven §0.5 invariants
that are practically expressible at the row-walk layer; bumps the
per-violation label-value bucket of ``invariant_violation_total`` on each
hit (the counters endpoint surfaces it as a ``name -> {label_value:
count}`` map; there is no ``{label=...}``-style tag on the wire).
Violations also log at ERROR. CI uses the counter to fail the next build
if any bucket is non-zero.

**Which invariants this auditor enforces directly:**

The seven §0.5 invariants split into two classes by enforcement
mechanism:

* **Row-walk-checkable (this module enforces):**
  - #1 — ``body_location='file'`` ⟹ files exist (modulo the
    ``body_discarded_at IS NOT NULL`` H4 carve-out).
  - #3 — ``body_hashes`` ↔ body-store ref set.
* **Counter/gauge observed (workers emit; this module reads only):**
  - #2 — Saturation-bytes basis equals body_size_bytes (the per-write
    discipline in the saturation gate prevents drift; this auditor
    does not cross-check at runtime).
  - #4 — Body-file orphans are GC'd (the
    :class:`BodyOrphanJanitor` enforces; its counter is the surface).
  - #5 — record_attempt_result_no_op_total bump (the sender's
    :meth:`record_attempt_result` rowcount=0 check; counter is the
    surface).
  - #6 — Persist controller is the sole writer of body_location='file'
    (pre-commit grep + the §0.5 single-writer manifest enforce this
    structurally; row-walk verifies the SHAPE of #1 + #3 which would
    fail under any violation).
  - #7 — No ``attempting`` row survives a restart (the
    :func:`run_recovery` function enforces at startup).

The InvariantAuditor's run-time effect is therefore narrow: it walks
live rows and asserts the per-row consistency invariants that cannot
be enforced by structural means. Bump-only on detection; no writes to
``uploads`` (single-writer manifest §0.5).

See plan § 4.2.3 + strategy §3.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
from typing import TYPE_CHECKING

from phantom.observability.metrics import MetricsRegistry
from phantom.storage.interface import TERMINAL_STATES
from phantom.workers.saturation import is_deliverable

if TYPE_CHECKING:
    from collections.abc import Callable
    from uuid import UUID

    from phantom.instances.snapshot import InstanceSettingsSnapshot
    from phantom.storage.interface import BodyStore
    from phantom.storage.sqlite_store import SqliteUploadStore

logger = logging.getLogger(__name__)

# Stable label-value bucket keys for ``invariant_violation_total``.
# Surface bumps for each so the admin endpoint shows a zero-valued
# bucket on a happy-path runtime.
_VIOLATION_MISSING_BODY_FILE: str = "missing_body_file"
_VIOLATION_MISSING_BODY_IN_RAM: str = "missing_body_in_ram"
_VIOLATION_BODY_HASH_SET_MISMATCH: str = "body_hash_set_mismatch"


class InvariantAuditor:
    """Periodic invariant-walk coroutine. Composition-root supervised.

    Constructed in :func:`phantom.app.create_app`'s ``lifespan`` (the
    composition root); spawned under that lifespan's TaskGroup. Writes
    NOTHING to the ``uploads`` table (single-writer manifest,
    plan § 0.5).
    """

    def __init__(
        self,
        *,
        store: SqliteUploadStore,
        body_store: BodyStore,
        current_settings: Callable[[], InstanceSettingsSnapshot],
        metrics_registry: MetricsRegistry | None = None,
    ) -> None:
        """Construct the auditor.

        Args:
            store: Persistent upload store; ``iter_rows()`` provides
                the row walk surface.
            body_store: Mode-selected :class:`BodyStore`; queried via
                ``has_body_ref`` per row's declared ``body_hashes``.
            current_settings: The live-snapshot thunk. The sweep
                cadence (``body_store.invariant_audit_period_seconds``)
                is read from it on every loop iteration per the ADR-031
                canonical mechanism (R9-1: the previous boot-Settings
                capture plus a loop-hoisted period read meant a reload
                never reached this worker's cadence, and the knob was
                missing from the ADR-013 table entirely).
            metrics_registry: Optional :class:`MetricsRegistry` for
                emit-site wiring (plan § 4.2.2). When ``None`` a
                throwaway registry is constructed.
        """
        self._store = store
        self._body_store = body_store
        self._current_settings = current_settings
        self._metrics = metrics_registry if metrics_registry is not None else MetricsRegistry()
        self._violation_counter = self._metrics.register_counter(
            "invariant_violation_total",
            "Per-kind invariant violations observed by the InvariantAuditor.",
        )
        self._audit_runs_total = self._metrics.register_counter(
            "invariant_audit_runs_total",
            "InvariantAuditor sweep iterations (success + failure).",
        )

    async def run(self, stop_event: asyncio.Event) -> None:
        """Main loop — sweep, sleep, until stopped.

        The loop is broad-``except``: a sweep failure logs at ERROR
        + continues the next iteration. The audit is a low-frequency
        diagnostic; one failed iteration just delays detection.

        Args:
            stop_event: Set by the composition root —
                :func:`phantom.app.create_app`'s lifespan — on shutdown.
                The loop exits cleanly when it fires, the same
                ``stop_event``-drain idiom every other lifespan worker
                uses so the supervising :class:`asyncio.TaskGroup` drains
                without waiting on a never-returning task.
        """
        # First sweep fires immediately — a fresh process should not
        # wait one period before the first audit.
        while not stop_event.is_set():
            try:
                await self._sweep_once()
            except Exception:
                logger.exception("InvariantAuditor sweep failed; continuing")
            await self._audit_runs_total.inc()
            # Re-read cadence from the live snapshot every iteration so
            # a hot-reloaded invariant_audit_period_seconds takes effect
            # without restart (R9-1 / ADR-031; previously captured from
            # boot Settings AND hoisted out of the loop).
            period = self._current_settings().body_store.invariant_audit_period_seconds
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(stop_event.wait(), timeout=period)

    async def _row_cleared_since_snapshot(self, chain_id: UUID) -> bool:
        """Re-read the live row; report whether a body-ref miss is legal.

        Round 5 fix (R5-1). ``iter_rows()`` streams ONE consistent
        read-connection snapshot for the whole walk while
        ``has_body_ref`` consults the LIVE body store, so a row that
        legally finishes mid-sweep is otherwise judged on stale fields.
        A miss is legal when the live row is now:

        * gone (reaped between snapshot and check),
        * terminal (delivered; the sender commits the terminal state
          BEFORE deleting bodies, so this re-read deterministically
          clears delivery races), or
        * ``body_discarded_at``-stamped (reaper/admin discard legs).

        A miss on a live, non-terminal, unstamped row stays a TRUE
        violation and the caller bumps it loud.

        Args:
            chain_id: Row identity to re-read from the live store.

        Returns:
            True when the miss must be skipped as a legal mid-sweep
            transition; False when it is a real invariant violation.
        """
        live = await self._store.get(chain_id)
        return live is None or live.state in TERMINAL_STATES or live.body_discarded_at is not None

    async def _sweep_once(self) -> None:
        """Walk every live row; check invariants #1, #3; bump counters.

        Rows whose snapshot is stale (a delivery or discard completed
        mid-sweep) are skipped via the
        :meth:`_row_cleared_since_snapshot` live re-read instead of
        bumping false violations (R5-1).
        """
        async for row in self._store.iter_rows():
            # Terminal carve-out (finding R9-PM-3). A row in a terminal
            # state is FINISHED; its body is legitimately gone (a
            # ``succeeded`` row's body is deleted on delivery without
            # stamping ``body_discarded_at``, so the H4 carve-out below
            # would miss it). Auditing body presence on a finished row
            # would fire spurious ``missing_body_*`` / ``body_hash_set_mismatch``
            # violations on every sweep — operational noise that masks REAL
            # corruption. ``auth_expired`` is NOT terminal (still
            # deliverable; its body MUST be present) so it is still audited.
            if row.state in TERMINAL_STATES:
                continue
            # H4 carve-out: a legitimately absent body, so there is no
            # invariant to check (auth_expired retention path).
            if not is_deliverable(row):
                continue
            # Invariant #1 + #3 checks per body_hash entry.
            present_names: set[str] = set()
            # Lazily resolved on the FIRST miss; one live re-read per
            # row at most (R5-1). True means the row legally finished
            # mid-sweep and the whole row is skipped, mismatch check
            # included.
            live_cleared: bool | None = None
            for name in row.body_hashes:
                present = await self._body_store.has_body_ref(row.chain_id, name)
                if present:
                    present_names.add(name)
                    continue
                if live_cleared is None:
                    live_cleared = await self._row_cleared_since_snapshot(row.chain_id)
                if live_cleared:
                    break
                if row.body_location == "file":
                    # Invariant #1 — body_location='file' claims files
                    # exist on disk.
                    logger.error(
                        "invariant violation: missing_body_file chain_id=%s name=%s",
                        row.chain_id,
                        name,
                    )
                    await self._violation_counter.inc(label_value=_VIOLATION_MISSING_BODY_FILE)
                else:
                    # body_location='ram' — RAM body absent on a row
                    # claiming RAM presence. Could be a transient
                    # post-power-cut state before recovery runs
                    # (strategy §3 "implicit consistency rule"), but
                    # in steady state it indicates an invariant break.
                    logger.error(
                        "invariant violation: missing_body_in_ram chain_id=%s name=%s",
                        row.chain_id,
                        name,
                    )
                    await self._violation_counter.inc(label_value=_VIOLATION_MISSING_BODY_IN_RAM)
            if live_cleared:
                # Legal mid-sweep transition; nothing on this row is a
                # violation, including the emptiness mismatch below.
                continue
            # Invariant #3 — body_hash set cardinality match. If the
            # body store knows extras not in the row's declared
            # body_hashes, the orphan janitor catches them (invariant
            # #4). We only assert the row-declared set is a *subset*
            # of body-store presence; missing names already bumped
            # above. If declared cardinality > present, that's already
            # covered. Add an explicit check for emptiness mismatch.
            declared = set(row.body_hashes.keys())
            if declared and not present_names and row.body_location in ("ram", "file"):
                # All declared body_hashes absent from store — a
                # stronger #3 violation worth tracking distinctly.
                logger.error(
                    "invariant violation: body_hash_set_mismatch chain_id=%s declared=%d present=0",
                    row.chain_id,
                    len(declared),
                )
                await self._violation_counter.inc(label_value=_VIOLATION_BODY_HASH_SET_MISMATCH)


__all__ = ["InvariantAuditor"]
