"""Kicker: wake ``auth_expired`` rows when their auth slot goes fresh.

ONE loop, two flavours. Until CL2 this file was two: ``auth_kicker.py`` and
``credential_kicker.py``, the second of which declared itself "A COPY of
:class:`AuthKicker`" in its own module docstring and then enumerated the
differences it meant to keep. Those differences were four axes in disguise,
and they are now the fields of :class:`KickerFlavour`:

1. **the name**, used in every log line and nothing else;
2. **the ``auth_mode`` literal** the flavour owns, compared against the closed
   :data:`phantom.routing.AuthMode` alias so a new mode is a typing error
   rather than a silent no-op;
3. **the freshness oracle**, the store whose slot going ``fresh`` is the wake
   signal (the ``(endpoint, uid)`` token cache, or the host-keyed credential
   store);
4. **the key's arity**, which is the only thing the two stores disagree about
   at lookup time and the only reason the wake log lines differ.

Everything else was already line-for-line identical: the rescan cadence, the
H4 skip, the auth_mode partition, the send-deadline sweep, the re-admit, the
guarded write and both of its refusal legs.

The wake path reads the RECORDED blocked host (``row.auth_blocked_host or
row.endpoint``, D2/F6) exactly ONCE per row and feeds it to both the route
resolution (through :func:`row_resolved_route`) and the freshness probe, so
the partition and the wake key can never sit on different host axes.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Protocol
from uuid import UUID

from phantom.instances.context import InstanceContext
from phantom.models.credential import CredentialStatus, HostCredKey
from phantom.models.token import TokenStatus
from phantom.routing import AuthMode
from phantom.storage.interface import CredentialStore, TokenCache
from phantom.workers._expire import expire_row
from phantom.workers._kicker_auth_mode import row_resolved_route
from phantom.workers.saturation import AdmissionGranted, is_deliverable

logger = logging.getLogger(__name__)

# Periodic-rescan interval. The kicker reacts to store-write events
# directly, but a parked row whose slot was already fresh at park time
# (e.g., the admin push landed before the row finished transitioning
# to ``auth_expired``) would otherwise wait forever without a periodic
# check. One second is short enough that recovery stays within the
# test budgets in plan §3.31 / §3.32 and long enough that healthy
# steady-state idle work stays negligible.
_RESCAN_INTERVAL_SECONDS = 1.0

# The status of one auth slot. The two stores declare the SAME closed member
# set under two names (``TokenStatus``, ``CredentialStatus``); the union names
# the shared type once so the loop's ``!= "fresh"`` test reads against one
# type rather than a store-specific one.
type SlotStatus = TokenStatus | CredentialStatus

# What a store calls when it writes a slot. The kicker does not care WHICH
# slot was written (the load-bearing check is "is this row's slot fresh?" at
# SCAN time, not at wake time), so the flavour's oracle absorbs each store's
# handler signature and the kicker registers a no-argument callback.
type WakeCallback = Callable[[], Awaitable[None]]


class FreshnessOracle(Protocol):
    """The store whose slot going ``fresh`` wakes a parked row.

    Two concrete implementations exist and both are wired in production
    (:class:`TokenCacheOracle`, :class:`CredentialStoreOracle`), which is what
    makes this a seam rather than a hypothetical one.
    """

    @property
    def configured(self) -> bool:
        """False when this deployment wired no such store, making the kicker inert."""
        ...

    def register_wake(self, on_write: WakeCallback) -> None:
        """Subscribe ``on_write`` to the store's slot writes (a no-op when unconfigured)."""
        ...

    async def lookup(self, probe_host: str, uid: str) -> SlotStatus | None:
        """Return the slot's status for this row's key, or ``None`` when absent.

        The status rather than a bool, deliberately: the loop distinguishes an
        ABSENT slot from a PRESENT but stale one, and collapsing both into
        False would lose a distinction the wake logic reads today and a future
        status value would need.
        """
        ...


@dataclass(frozen=True)
class TokenCacheOracle:
    """``phantom_bearer`` freshness: the ``(endpoint, uid)`` token cache."""

    cache: TokenCache

    @property
    def configured(self) -> bool:
        """Always True: every instance wires a token cache."""
        return True

    def register_wake(self, on_write: WakeCallback) -> None:
        """Register on the cache's wake hook, discarding the written slot's identity."""

        async def _handler(endpoint: str, uid: str) -> None:
            del endpoint, uid  # The scan matches against current cache state.
            await on_write()

        self.cache.register_wake_handler(_handler)

    async def lookup(self, probe_host: str, uid: str) -> SlotStatus | None:
        """Read the ``(probe_host, uid)`` slot's status."""
        slot = await self.cache.get(probe_host, uid)
        return slot.status if slot is not None else None


@dataclass(frozen=True)
class CredentialStoreOracle:
    """``aws_sigv4`` freshness: the host-keyed destination-credential store.

    OPTIONAL by construction: ``InstanceContext.signer_creds`` is ``None`` for
    any deployment with no ``aws_sigv4`` route (the default). ``configured`` is
    then False, no wake handler is registered and the rescan returns early, so
    wiring this flavour on every instance's TaskGroup costs nothing.
    """

    store: CredentialStore | None

    @property
    def configured(self) -> bool:
        """False when no ``aws_sigv4`` route is configured for this instance."""
        return self.store is not None

    def register_wake(self, on_write: WakeCallback) -> None:
        """Register on the store's wake hook when one exists; otherwise stay inert."""
        if self.store is None:
            return

        async def _handler(dest_host: HostCredKey) -> None:
            del dest_host  # The scan matches against current store state.
            await on_write()

        self.store.register_wake_handler(_handler)

    async def lookup(self, probe_host: str, uid: str) -> SlotStatus | None:
        """Read the ``probe_host`` slot's status; ``uid`` is DELIBERATELY unused.

        This store is keyed on the destination host ALONE (ADR-033): dropping
        the uid is the credential axis's actual shape, not an oversight, and it
        is stated here because a silently ignored key component is how a
        host-keyed lookup starts matching the wrong row.
        """
        del uid
        if self.store is None:
            return None
        cred_row = await self.store.get(HostCredKey(probe_host))
        return cred_row.status if cred_row is not None else None


@dataclass(frozen=True)
class KickerFlavour:
    """The four axes on which the two kickers actually differ.

    Attributes:
        name: The kicker's operator-visible name, used in every log line.
        auth_mode: The route mode this flavour owns. Rows resolving to any
            other mode belong to the other flavour (or to no kicker at all)
            and are skipped, so the two never fight over one row.
        oracle_for: Builds the flavour's freshness oracle from the instance.
            A factory rather than a bound oracle so the flavours can be
            module-level constants.
        log_key_fields: The slot key's shape as the wake log renders it.
            ``("blocked_host", "uid")`` for the bearer cache, and
            ``("blocked_host",)`` for the host-keyed credential store.
    """

    name: str
    auth_mode: AuthMode
    oracle_for: Callable[[InstanceContext], FreshnessOracle]
    log_key_fields: tuple[str, ...]


PHANTOM_BEARER_FLAVOUR = KickerFlavour(
    name="AuthKicker",
    auth_mode="phantom_bearer",
    oracle_for=lambda instance: TokenCacheOracle(cache=instance.token_cache),
    log_key_fields=("blocked_host", "uid"),
)

AWS_SIGV4_FLAVOUR = KickerFlavour(
    name="CredentialKicker",
    auth_mode="aws_sigv4",
    oracle_for=lambda instance: CredentialStoreOracle(store=instance.signer_creds),
    log_key_fields=("blocked_host",),
)


class Kicker:
    """Re-queue ``auth_expired`` rows whose auth slot is fresh.

    Reacts to two signals:

    - **Store writes.** The flavour's oracle fires the registered wake
      handler. The kicker stores a "rescan requested" event rather than the
      written slot's key: the load-bearing check is "is the slot fresh for
      this row?" at scan time, not at wake time. Without this, a wake that
      lands between the executor's 401 and the sender's
      ``record_attempt_result`` would be consumed before the row reached
      ``auth_expired`` and the row would never wake.
    - **Periodic tick.** Every ``_RESCAN_INTERVAL_SECONDS`` the kicker scans
      regardless. Catches the rare ordering where the push landed before the
      row's ``auth_expired`` row committed and no further write follows.

    Rows whose ``body_discarded_at`` is stamped are never woken (R6-3); see
    :func:`phantom.workers.saturation.is_deliverable`.
    """

    def __init__(self, *, instance: InstanceContext, flavour: KickerFlavour) -> None:
        """Construct the kicker.

        Args:
            instance: The instance whose stores and gate to wire.
            flavour: Which auth mode this kicker owns and which store answers
                its freshness question.
        """
        self._instance = instance
        self._flavour = flavour
        self._oracle = flavour.oracle_for(instance)
        # An :class:`asyncio.Event` rather than a queue: the kicker doesn't
        # care how many wake events accumulate, only whether at least one
        # happened since the last scan. The periodic rescan covers the gap
        # between an early wake and a later park.
        self._wake_event = asyncio.Event()
        self._oracle.register_wake(self._on_slot_write)

    async def _on_slot_write(self) -> None:
        """Store-write callback: set the wake event for the next rescan."""
        self._wake_event.set()

    async def run(self, stop_event: asyncio.Event) -> None:
        """Main loop: rescan on every wake or every interval until stopped."""
        while not stop_event.is_set():
            # Wait for either a store write or the rescan interval: whichever
            # fires first triggers the next rescan.
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(
                    self._wake_event.wait(),
                    timeout=_RESCAN_INTERVAL_SECONDS,
                )
            self._wake_event.clear()
            try:
                await self._rescan()
            except Exception:
                logger.exception("%s rescan failed", self._flavour.name)

    async def _rescan(self) -> None:
        """Re-queue every ``auth_expired`` row of this flavour whose slot is fresh.

        Single persistent store (plan § 2.3.6): one store holds every row
        regardless of body_location. The flavour's oracle is the freshness
        oracle.
        """
        if not self._oracle.configured:
            # No store of this kind for this instance, so there is nothing
            # this flavour can wake. Keeps the per-instance TaskGroup wiring
            # uniform while staying inert in the common deployment.
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
            # The host that ACTUALLY rejected this row (D2/F6), not
            # ``row.endpoint``: the executor authenticates or signs against
            # the CURRENT step's host while ``endpoint`` is pinned to the
            # FIRST step's, so on a multi-host chain probing the endpoint
            # finds a fresh slot for a host the row is not blocked on and
            # re-queues it into a 1 Hz livelock. Computed ONCE here and used
            # by the no-route warning and the freshness probe alike; the
            # route resolution below reads the same expression through
            # ``row_resolved_route``. The ``or`` is D2's fallback for a row
            # whose column is NULL; under the current schema policy a version
            # bump discards the DB rather than migrating it, so that
            # population is empty and the fallback is defence in depth.
            probe_host = row.auth_blocked_host or row.endpoint
            # auth_mode GUARD (plan §2.5): both flavours walk the SAME
            # auth_expired rows on the SAME saturation gate, so each must wake
            # ONLY rows of its kind, or the two would double-admit one row and
            # race the auth_expired->queued requeue. Placed AFTER the cheap
            # state + deliverability filters (a discarded/non-parked row is
            # skipped before any resolve) and BEFORE the freshness gate. It is
            # the only raising call in the loop (resolve_route raises
            # ValueError on no-match; since F5 froze the route block at boot
            # the cause is a chain admitted with a step whose host matches no
            # route, not a route removed by hot-reload, which can no longer
            # happen). Wrap it per row and SKIP on raise: a single un-routable
            # parked row must NOT abort the rescan pass (which would strand
            # every row ordered behind it forever under the 1 s rescan). One
            # resolve feeds BOTH the auth_mode partition and the deadline
            # sweep.
            try:
                resolved = row_resolved_route(row, self._instance)
            except ValueError:
                logger.warning(
                    "%s: no route matches blocked_host=%s for chain_id=%s; "
                    "skipping this row (left in auth_expired for the next rescan)",
                    self._flavour.name,
                    probe_host,
                    row.chain_id,
                )
                continue
            if resolved.auth_mode != self._flavour.auth_mode:
                # NOT my kind: the other flavour, or no kicker at all
                # (``none``), owns this row.
                continue
            # Send-deadline SWEEP (ADR-032, the suspenders to the executor
            # gate's belt). A row parked in auth_expired awaiting a re-push
            # that NEVER comes has no other backstop: this kicker either wakes
            # it (slot fresh) or leaves it parked forever. Placed AFTER the
            # auth_mode guard (so it only sweeps rows this kicker owns) and
            # BEFORE the freshness gate (so an over-deadline row gives up even
            # while its slot is still bad or absent, which is the whole point:
            # it has waited too long). The shared expire_row writer flips it
            # terminal-``expired`` and discards the body; the row then drops
            # out of the next list_non_terminal (it is now in TERMINAL_STATES),
            # so it is never woken. The row is ``auth_expired``, whose slot was
            # ALREADY released at park (``_on_auth_failure``), so nothing here
            # may re-release it (a double-free that transiently under-counts
            # in_flight and over-admits past the cap); the writer reads that
            # off the row's own state rather than being told. ``continue``: do
            # not fall through to the wake.
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
            # ``row.uid`` is unchanged by D2: a chain has one uid, so the
            # recorded host is the only part of the slot key the row was
            # missing. The host-keyed flavour ignores it, deliberately and
            # visibly, inside its own oracle.
            status = await self._oracle.lookup(probe_host, row.uid)
            if status != "fresh":
                continue
            # Re-admit through the saturation gate (§3.1 symmetry). The sender
            # released the gate when the row parked into auth_expired; the row
            # must take a slot back before it rejoins the in-flight set. On
            # refusal: log and leave the row in auth_expired, and the next
            # rescan tries again.
            result = await self._instance.saturation.admit(row.body_size_bytes)
            if not isinstance(result, AdmissionGranted):
                logger.warning(
                    "%s refused wake for chain_id=%s (saturated); "
                    "leaving in auth_expired for next rescan",
                    self._flavour.name,
                    row.chain_id,
                )
                continue
            self._log_wake(row.chain_id, probe_host, row.uid)
            # M-W4-F7 (Phase 2 § 3.2.8): the kicker writes from
            # auth_expired -> queued. Pass the expected_state explicitly so
            # the default ``attempting`` predicate doesn't reject the update;
            # the kicker is the only allowed mover of the auth_expired ->
            # queued transition.
            #
            # ONE refusal posture spans the admit->write window: the slot
            # admitted above returns on EVERY outcome except a confirmed wake
            # (invariant #16). Two refusal legs exist: the guarded write
            # no-ops (rowcount 0: admin cancel/replay or another kicker tick
            # moved the row first; R9-3), or the write RAISES (a transient
            # storage fault, SQLITE_BUSY past the busy_timeout, or an I/O
            # error on flaky SD storage; R10-2), and both release before
            # yielding, mirroring the replay route's release-on-exception
            # (routes/admin.py replay_upload). Pre-R10-2 the exception leg
            # propagated with the slot stranded; run() logs and continues
            # while the row stays auth_expired with a fresh token, so every
            # later rescan stranded another slot until the gate saturated and
            # fresh ingress 503'd with no live row behind the count.
            try:
                write = await store.record_attempt_result(
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
                # (logger.exception) and this line adds the chain_id context.
                await self._instance.saturation.release(row.body_size_bytes)
                logger.warning(
                    "%s wake write failed for chain_id=%s; "
                    "admitted slot returned, row stays auth_expired "
                    "for the next rescan",
                    self._flavour.name,
                    row.chain_id,
                )
                raise
            if not write.landed:
                # The rowcount-0 refusal leg (R9-3): the wake never happened,
                # and whoever moved the row owns its own accounting, so this
                # release only undoes OUR admit. Then skip; the next
                # event/poll picks up any further waking work.
                await self._instance.saturation.release(row.body_size_bytes)
                logger.info(
                    "%s no-op: chain_id=%s - row state changed "
                    "from auth_expired before update; admitted slot "
                    "returned",
                    self._flavour.name,
                    row.chain_id,
                )

    def _log_wake(self, chain_id: UUID, probe_host: str, uid: str) -> None:
        """Log the confirmed wake with the flavour's slot-key shape.

        The ONE place the key's arity shows: the bearer cache is keyed on
        ``(host, uid)`` and logs both, while the credential store is keyed on
        the host alone and has no uid to log. Both renderings are the ones
        their kicker emitted before the merge, character for character, so a
        log-reading operator (or test) sees no change.
        """
        if "uid" in self._flavour.log_key_fields:
            logger.info(
                "%s waking row chain_id=%s for blocked_host=%s uid=%s",
                self._flavour.name,
                chain_id,
                probe_host,
                uid,
            )
            return
        logger.info(
            "%s waking row chain_id=%s for blocked_host=%s",
            self._flavour.name,
            chain_id,
            probe_host,
        )
