"""CredentialKicker — wake ``auth_expired`` rows when the credential store lands fresh creds.

A COPY of :class:`phantom.workers.auth_kicker.AuthKicker` (plan §2.3, the
copy-map §6/§7 + §8 row 7). The structure is line-for-line identical to the
bearer auth-recovery loop; exactly three things differ:

1. the freshness oracle is the **CredentialStore** keyed by host alone
   (``signer_creds.get(row.auth_blocked_host or row.endpoint)``, the recorded
   blocked host per D2/F6) instead of the token cache keyed by
   ``(<that same host>, uid)``;
2. the wake-handler is registered on the **CredentialStore** (one-arg
   ``(dest_host)`` handler, not two-arg ``(endpoint, uid)``);
3. the ``auth_mode`` guard skips non-``aws_sigv4`` rows (plan §2.5) so this
   kicker and the ``AuthKicker`` do not fight over the shared ``auth_expired``
   rows / saturation gate.

OPTIONAL by construction: ``InstanceContext.signer_creds`` is ``None`` for any
deployment with no ``aws_sigv4`` route (the default). When it is ``None`` this
kicker registers no wake-handler and its rescan is a no-op, so wiring it on
every instance's TaskGroup costs nothing and forces no change to existing
instance-construction tests.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
from datetime import UTC, datetime

from phantom.instances.context import InstanceContext
from phantom.models.credential import HostCredKey
from phantom.workers._expire import expire_row
from phantom.workers._kicker_auth_mode import row_resolved_route
from phantom.workers.saturation import AdmissionGranted, is_deliverable

logger = logging.getLogger(__name__)

# Periodic-rescan interval. COPY of AuthKicker's rationale: the kicker reacts to
# credential-write events directly, but a row whose cred slot was already fresh
# at park time (the admin cred push landed before the row finished transitioning
# to ``auth_expired``) would otherwise wait forever without a periodic check.
_RESCAN_INTERVAL_SECONDS = 1.0


class CredentialKicker:
    """Re-queue ``auth_expired`` ``aws_sigv4`` rows whose cred slot is fresh.

    A COPY of :class:`phantom.workers.auth_kicker.AuthKicker` with the freshness
    oracle pointed at the credential store and an ``auth_mode`` guard that
    confines it to ``aws_sigv4`` rows. Reacts to the same two signals:

    - **Credential writes** — ``SqliteCredentialStore.set`` fires the registered
      wake handler. The kicker stores a "rescan requested" event rather than the
      host — the load-bearing check is "is the cred slot fresh for this row's
      host?" at scan time, not at wake time.
    - **Periodic tick** — every ``_RESCAN_INTERVAL_SECONDS`` the kicker scans
      regardless, catching the ordering where the push landed before the row's
      ``auth_expired`` row committed and no further cred write follows.

    Rows whose ``body_discarded_at`` is stamped are never woken (R6-3): the body
    was discarded by retention policy, so a re-queue could only end in a false
    ``corrupted``. They stay parked until the metadata-retention pass reaps
    them, matching every other H4-carve-out consumer.

    A no-op when ``instance.signer_creds is None`` (no ``aws_sigv4`` route is
    configured): no wake-handler is registered and ``_rescan`` returns early.
    """

    def __init__(self, *, instance: InstanceContext) -> None:
        """Construct the kicker.

        Args:
            instance: The instance whose stores and credential store to wire.
                When ``instance.signer_creds`` is ``None`` the kicker registers
                no wake-handler and its rescan is a no-op.
        """
        self._instance = instance
        # An :class:`asyncio.Event` rather than a queue — the kicker doesn't
        # care how many wake events accumulate, only whether at least one
        # happened since the last scan. The periodic rescan covers the gap
        # between an early wake and a later park.
        self._wake_event = asyncio.Event()
        # Register against the credential store's wake hook — but only when one
        # exists. With no aws_sigv4 route the store is absent and there is
        # nothing to wake on; the kicker stays inert.
        if instance.signer_creds is not None:
            instance.signer_creds.register_wake_handler(self._on_cred_set)

    async def _on_cred_set(self, dest_host: HostCredKey) -> None:
        """Credential-write callback — set the wake event for the next rescan."""
        del dest_host  # The scan matches against current cred-store state, not the wake's identity.
        self._wake_event.set()

    async def run(self, stop_event: asyncio.Event) -> None:
        """Main loop: rescan on every wake or every interval until stopped."""
        while not stop_event.is_set():
            # Wait for either a credential write or the rescan interval —
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
                logger.exception("CredentialKicker rescan failed")

    async def _rescan(self) -> None:
        """Re-queue every ``auth_expired`` ``aws_sigv4`` row whose cred slot is fresh.

        Single persistent store (plan §2.3.6): one store holds every row
        regardless of body_location. The credential store is the freshness
        oracle.
        """
        signer_creds = self._instance.signer_creds
        if signer_creds is None:
            # No aws_sigv4 route configured for this instance — nothing this
            # kicker can wake. Keeps the per-instance TaskGroup wiring uniform
            # while staying inert in the common (bearer-only) deployment.
            return
        now = datetime.now(tz=UTC)
        store = self._instance.store
        rows = await store.list_non_terminal()
        for row in rows:
            if row.state != "auth_expired":
                continue
            if not is_deliverable(row):
                # H4 carve-out (R6-3); see ``is_deliverable`` for what the
                # stamp means. Leave the row parked in ``auth_expired``.
                continue
            # auth_mode GUARD (plan §2.5): both kickers walk the SAME
            # auth_expired rows on the SAME saturation gate, so each must wake
            # ONLY rows of its kind — here ``aws_sigv4`` — or the two would
            # double-admit one row and race the auth_expired→queued requeue.
            # Placed AFTER the cheap state + body_discarded_at filters and
            # BEFORE the freshness gate. ``row_resolved_route`` resolves the
            # RECORDED blocked host (``row.auth_blocked_host or
            # row.endpoint``, D2/F6) through ``resolve_route``, the SAME
            # expression the freshness probe below uses, so the partition and
            # the wake key share one host axis. It is the only raising call in
            # the copied loop (ValueError on no-match; since F5 froze the
            # route block at boot the cause is a chain admitted with a step
            # whose host matches no route, not a route removed by hot-reload,
            # which can no longer happen). Wrap it per
            # row and SKIP on raise: a single un-routable parked row must NOT
            # abort the rescan pass. One resolve feeds BOTH the auth_mode
            # partition and the deadline sweep.
            try:
                resolved = row_resolved_route(row, self._instance)
            except ValueError:
                logger.warning(
                    "CredentialKicker: no route matches blocked_host=%s for chain_id=%s; "
                    "skipping this row (left in auth_expired for the next rescan)",
                    row.auth_blocked_host or row.endpoint,
                    row.chain_id,
                )
                continue
            if resolved.auth_mode != "aws_sigv4":
                # NOT my kind — the AuthKicker (phantom_bearer) or no kicker
                # ("none") owns this row.
                continue
            # Send-deadline SWEEP (ADR-032 — the reuse-the-loop backstop). A
            # row parked in auth_expired awaiting a cred re-push that NEVER
            # comes has no other bound: this kicker either wakes it (slot fresh)
            # or leaves it parked forever. Placed AFTER the auth_mode guard (so
            # it only sweeps aws_sigv4 rows this kicker owns) and BEFORE the
            # freshness gate (so an over-deadline row gives up even while its
            # cred slot is still bad/absent — the exact "re-push never came"
            # case). The shared expire_row writer flips it terminal-``expired``
            # and discards the body; the row then drops out of the next
            # list_non_terminal (now in TERMINAL_STATES), never woken. The row
            # is ``auth_expired``, whose slot was ALREADY released at park
            # (``_on_auth_failure``), so nothing here may re-release it (a
            # double-free that transiently under-counts in_flight and
            # over-admits past the cap); the writer reads that off the row's
            # own state rather than being told. ``continue``: do not fall
            # through to the wake.
            deadline = resolved.send_deadline_seconds
            if deadline is not None and (now - row.received_at).total_seconds() > deadline:
                await expire_row(
                    self._instance.store,
                    self._instance.saturation,
                    self._instance.body_store,
                    row,
                    expected_state="auth_expired",
                    last_error=f"send_deadline:{deadline}s",
                    upstream_status=None,
                )
                continue
            # Probe the host that ACTUALLY rejected this row (D2/F6), not
            # ``row.endpoint``: the executor signs against the CURRENT step's
            # host while ``endpoint`` is pinned to the FIRST step's, so on a
            # multi-host chain probing the endpoint finds a fresh credential
            # for a host the row is not blocked on and re-queues it into a
            # 1 Hz livelock. The ``or`` is D2's fallback for a row whose
            # column is NULL; under the current schema policy a version bump
            # discards the DB rather than migrating it, so that population is
            # empty and the fallback is defence in depth.
            probe_host = row.auth_blocked_host or row.endpoint
            cred_row = await signer_creds.get(HostCredKey(probe_host))
            if cred_row is None or cred_row.status != "fresh":
                continue
            # Re-admit through the saturation gate (§3.1 symmetry). The sender
            # released the gate when the row parked into auth_expired; the row
            # must take a slot back before it rejoins the in-flight set. On
            # refusal: log and leave the row in auth_expired — the next rescan
            # tries again.
            result = await self._instance.saturation.admit(row.body_size_bytes)
            if not isinstance(result, AdmissionGranted):
                logger.warning(
                    "CredentialKicker refused wake for chain_id=%s (saturated); "
                    "leaving in auth_expired for next rescan",
                    row.chain_id,
                )
                continue
            logger.info(
                "CredentialKicker waking row chain_id=%s for blocked_host=%s",
                row.chain_id,
                probe_host,
            )
            # M-W4-F7 (Phase 2 §3.2.8): the kicker writes from auth_expired →
            # queued. Pass the expected_state explicitly so the default
            # ``attempting`` predicate doesn't reject the update; the kicker is
            # the only allowed mover of the auth_expired → queued transition.
            #
            # ONE refusal posture spans the admit->write window: the slot
            # admitted above returns on EVERY outcome except a confirmed wake
            # (invariant #16). Two refusal legs exist — the guarded write
            # no-ops (rowcount 0: admin cancel/replay or another kicker tick
            # moved the row first; R9-3), or the write RAISES (a transient
            # storage fault; R10-2) — and both release before yielding,
            # mirroring the AuthKicker copy.
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
                # Release FIRST, then surface the fault. The failed write
                # committed nothing, so the row is untouched and the next
                # rescan retries the wake; run() owns the traceback
                # (logger.exception) — this line adds the chain_id context.
                await self._instance.saturation.release(row.body_size_bytes)
                logger.warning(
                    "CredentialKicker wake write failed for chain_id=%s; "
                    "admitted slot returned, row stays auth_expired "
                    "for the next rescan",
                    row.chain_id,
                )
                raise
            if rowcount == 0:
                # The rowcount-0 refusal leg (R9-3): the wake never happened,
                # and whoever moved the row owns its own accounting — this
                # release only undoes OUR admit. Then skip; the next event/poll
                # picks up any further waking work.
                await self._instance.saturation.release(row.body_size_bytes)
                logger.info(
                    "CredentialKicker no-op: chain_id=%s - row state changed "
                    "from auth_expired before update; admitted slot returned",
                    row.chain_id,
                )
