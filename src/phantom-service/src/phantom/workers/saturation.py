"""SaturationGate — counter cache consulted synchronously by ingress."""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Final

from phantom.config.settings import SaturationCfg
from phantom.observability.metrics import MetricsRegistry

logger = logging.getLogger(__name__)

# States whose row currently holds a saturation slot (R8-4). Admission
# charges the gate; the sender releases on the terminal transitions it
# drives (succeeded / failed / corrupted / cancelled) and on the
# auth_expired park (the AuthKicker re-admits through the gate on
# wake). ``stored`` deliberately keeps its slot - the buffered body
# still occupies space - until the body is discarded by retention
# policy or the row is removed. Every path that REMOVES a row (admin
# cancel / delete / bulk delete, the reaper's deletion passes) and
# every path that RE-QUEUES a released row (replay) consults this set
# through :func:`row_holds_slot` so the ledger cannot drift in either
# direction.
SLOT_HOLDING_STATES: Final[frozenset[str]] = frozenset({"queued", "attempting", "stored"})


def row_holds_slot(state: str, body_discarded_at: datetime | None) -> bool:
    """Return True when a row in ``state`` currently holds a gate slot.

    The one shared predicate for release-on-removal and
    re-admit-on-replay decisions (R8-4 / R8-6). ``stored`` is the
    special case: its slot is released by the reaper's BODY-DISCARD
    pass (the space the slot represents is freed there), so a stored
    row whose ``body_discarded_at`` is stamped no longer holds one and
    releasing again at its later row-removal would double-free.

    Args:
        state: The row's state at the decision instant.
        body_discarded_at: The row's H4 carve-out stamp at the same
            instant.

    Returns:
        ``True`` when removing the row must release the gate (and,
        equivalently, re-queueing it must NOT re-admit); ``False``
        otherwise.
    """
    if state not in SLOT_HOLDING_STATES:
        return False
    return not (state == "stored" and body_discarded_at is not None)


@dataclass(frozen=True)
class AdmissionGranted:
    """The gate accepted the upload."""


@dataclass(frozen=True)
class AdmissionRefusedSaturation:
    """In-flight row count or in-flight bytes cap exceeded."""


@dataclass(frozen=True)
class AdmissionRefusedDiskPressure:
    """Disk-bytes ceiling reached — observed via the periodic disk probe (§2.3)."""


AdmissionResult = AdmissionGranted | AdmissionRefusedSaturation | AdmissionRefusedDiskPressure
"""Discriminated union of admission outcomes.

Callers dispatch via ``isinstance(result, AdmissionGranted)`` rather
than parsing an enum value; the three refusal classes carry no
additional payload but exist as distinct types so the type system
keeps the cases exhaustive.
"""


class SaturationGate:
    """Tracks in-flight count and bytes; admits or rejects new uploads.

    The gate is consulted synchronously by the ingress handler. Its counters
    are in-process only; boot recovery reconstructs them from persisted rows
    before workers start.

    Disk-pressure accounting is updated by an external probe via
    :meth:`set_disk_usage_bytes`; the probe runs out of band so the gate's
    synchronous admit path never does I/O. That probe re-reads
    ``max_disk_bytes`` off this gate on EVERY tick (F13), so the disk cap
    is genuinely live: a cap pushed here by ``update_caps`` applies at the
    next admit, and the observation it is compared against refreshes
    within one probe interval. While the cap is 0 the probe skips its
    walk, which is safe because :meth:`admit` short-circuits on
    ``max_disk_bytes > 0`` before reading the observation.
    """

    def __init__(
        self,
        *,
        max_in_flight: int,
        max_in_flight_bytes: int,
        max_disk_bytes: int,
        large_body_threshold_bytes: int = 0,
        max_large_in_flight: int = 0,
        metrics_registry: MetricsRegistry | None = None,
    ) -> None:
        """Construct the gate.

        Args:
            max_in_flight: Hard cap on concurrent non-terminal rows.
            max_in_flight_bytes: Hard cap on summed in-flight body bytes.
            max_disk_bytes: Hard cap on total bytes the disk tier may use.
                Zero disables the disk-pressure check (the periodic probe
                is then a no-op).
            large_body_threshold_bytes: Bodies at or above this size are
                counted in the separate large-body class. Zero disables
                the class — no row counts as large.
            max_large_in_flight: Max concurrent in-flight bodies in the
                large class. Ignored when ``large_body_threshold_bytes=0``.
            metrics_registry: Optional :class:`MetricsRegistry` for
                emit-site wiring (plan § 4.2.2). When ``None`` a
                throwaway registry is constructed so emission is a
                no-op.
        """
        self._max_in_flight = max_in_flight
        self._max_in_flight_bytes = max_in_flight_bytes
        self._max_disk_bytes = max_disk_bytes
        self._large_body_threshold_bytes = large_body_threshold_bytes
        self._max_large_in_flight = max_large_in_flight
        self._lock = asyncio.Lock()
        self._in_flight = 0
        self._in_flight_bytes = 0
        self._large_in_flight = 0
        # Outstanding LARGE charges keyed by declared size (R9-6): the
        # charge-time classification memory the release path consults,
        # so a hot-reloaded large_body_threshold_bytes never
        # desynchronizes the class ledger. Bounded by the number of
        # large bodies in flight.
        self._large_charge_sizes: dict[int, int] = {}
        # Last-observed disk usage from the periodic probe. Zero means
        # "no observation yet" — the gate behaves as if disk is empty
        # until the first probe lands.
        self._disk_usage_bytes = 0
        # Metrics surface (plan § 4.2.2). saturation_balance is the
        # plan-canonical Gauge name for current in-flight declared
        # bytes; admin endpoints serialize it via the registry.
        self._metrics = metrics_registry if metrics_registry is not None else MetricsRegistry()
        self._saturation_balance = self._metrics.register_gauge(
            "saturation_balance",
            "Current in-flight declared bytes admitted by the gate.",
        )

    @property
    def in_flight(self) -> int:
        """Current in-flight row count."""
        return self._in_flight

    @property
    def in_flight_bytes(self) -> int:
        """Current in-flight bytes."""
        return self._in_flight_bytes

    @property
    def max_in_flight(self) -> int:
        """Configured row cap."""
        return self._max_in_flight

    @property
    def max_in_flight_bytes(self) -> int:
        """Configured bytes cap."""
        return self._max_in_flight_bytes

    @property
    def max_disk_bytes(self) -> int:
        """Configured disk-usage cap (0 = disabled)."""
        return self._max_disk_bytes

    @property
    def disk_usage_bytes(self) -> int:
        """Most recent disk-usage observation from the probe."""
        return self._disk_usage_bytes

    @property
    def large_in_flight(self) -> int:
        """Current in-flight large-body count (§1.3)."""
        return self._large_in_flight

    @property
    def max_large_in_flight(self) -> int:
        """Configured large-class cap."""
        return self._max_large_in_flight

    @property
    def large_body_threshold_bytes(self) -> int:
        """Configured threshold (0 = large-body class disabled)."""
        return self._large_body_threshold_bytes

    @property
    def saturated(self) -> bool:
        """True if any cap is currently exceeded."""
        return (
            self._in_flight >= self._max_in_flight
            or self._in_flight_bytes >= self._max_in_flight_bytes
        )

    def set_disk_usage_bytes(self, value: int) -> None:
        """Update the cached disk-usage observation (called by DiskPressureProbe).

        Synchronous on purpose: the probe runs in its own coroutine and
        reads ``FileBodyStore.total_bytes`` (which is async); calling this
        setter is the last step. The setter is not lock-protected because
        write/read of a single Python int is atomic under the GIL — the
        cost of contention is reading a slightly-stale value for one
        admit call, which is acceptable.
        """
        self._disk_usage_bytes = max(0, value)

    async def admit(self, declared_bytes: int) -> AdmissionResult:
        """Try to admit a new upload and return the typed outcome.

        Returns:
            :class:`AdmissionGranted` on success;
            :class:`AdmissionRefusedSaturation` if the row/bytes cap
            was hit; :class:`AdmissionRefusedDiskPressure` if the
            configured ``max_disk_bytes`` is set and the cached
            observation is at or above it. Callers dispatch via
            ``isinstance``.
        """
        async with self._lock:
            # Disk pressure is checked first: it's the cheaper signal
            # (read of a cached int) and the producer's operator wants to see
            # disk_pressure when it's the actual cause. The check uses
            # `>=` so a row landing exactly at the cap is still refused —
            # a 0-byte ingress at-cap would otherwise be admitted and
            # then promptly trip ENOSPC when its row arrives on disk.
            if (
                self._max_disk_bytes > 0
                and self._disk_usage_bytes + declared_bytes >= self._max_disk_bytes
            ):
                return AdmissionRefusedDiskPressure()
            if self._in_flight + 1 > self._max_in_flight:
                return AdmissionRefusedSaturation()
            # Bytes cap. A 0 cap means "refuse all" — matching the row
            # cap, where ``max_in_flight=0`` refuses every admission via
            # ``0 + 1 > 0``. Finding A-1: a naive ``+ declared > cap``
            # admitted 0-byte bodies under a 0 cap (``0 + 0 > 0`` is
            # False), an inconsistency with the row cap's zero-semantics.
            # We special-case 0 rather than switch to ``>=`` so a body
            # that *exactly* fills a positive cap is still admitted
            # (``in_flight_bytes + declared == cap`` is fine), preserving
            # the existing "fill to the brim" behavior.
            if self._max_in_flight_bytes == 0:
                return AdmissionRefusedSaturation()
            if self._in_flight_bytes + declared_bytes > self._max_in_flight_bytes:
                return AdmissionRefusedSaturation()
            # Large-body class cap (§1.3): enforce only when the threshold
            # is set (> 0) and the candidate body qualifies as large.
            if (
                self._large_body_threshold_bytes > 0
                and declared_bytes >= self._large_body_threshold_bytes
                and self._large_in_flight + 1 > self._max_large_in_flight
            ):
                return AdmissionRefusedSaturation()
            self._in_flight += 1
            self._in_flight_bytes += declared_bytes
            if self._is_large(declared_bytes):
                # R9-6: remember the classification AT CHARGE TIME. The
                # release must decrement the large class iff THIS charge
                # was large, and the threshold may have been hot-reloaded
                # in between; recomputing at release desynchronized the
                # class ledger in both directions. The multiset of
                # outstanding large charge sizes is the identityless
                # memory: releases pair against it by size (equal-size
                # mis-pairing conserves the count exactly, because
                # release amounts always equal admit amounts per
                # invariant #2).
                self._large_in_flight += 1
                self._large_charge_sizes[declared_bytes] = (
                    self._large_charge_sizes.get(declared_bytes, 0) + 1
                )
        # Emit gauge after releasing the lock — Gauge.set acquires its
        # own asyncio.Lock and we forbid await inside async with lock
        # (plan § 0.3).
        await self._saturation_balance.set(self._in_flight_bytes)
        return AdmissionGranted()

    def _is_large(self, body_bytes: int) -> bool:
        """Classify ``body_bytes`` against the large-body threshold."""
        threshold = self._large_body_threshold_bytes
        return threshold > 0 and body_bytes >= threshold

    async def reconcile_admit(self, actual_bytes: int) -> None:
        """Charge the ledger for a row that is ALREADY live, bypassing caps.

        Used only when the row already exists and therefore cannot be refused:
        boot reconstruction charges every persisted slot-holding row, and the
        R9-4 replay race repairs a re-queued row whose pre-fetched decision
        lost to sender release. Every ordinary admission MUST go through
        :meth:`admit`.
        """
        async with self._lock:
            self._in_flight += 1
            self._in_flight_bytes += actual_bytes
            if self._is_large(actual_bytes):
                self._large_in_flight += 1
                self._large_charge_sizes[actual_bytes] = (
                    self._large_charge_sizes.get(actual_bytes, 0) + 1
                )
        # Emit after releasing the lock (plan § 0.3 forbids await
        # inside async with lock).
        await self._saturation_balance.set(self._in_flight_bytes)

    async def release(self, actual_bytes: int) -> None:
        """Decrement counters when the row leaves the in-flight set.

        The large class decrements against the charge-time
        classification remembered by :meth:`admit` (R9-6), never by
        reclassifying ``actual_bytes`` against the current threshold: a
        hot-reloaded threshold otherwise stranded the counter (raise:
        large charges never released, the class cap refusing fresh
        large work forever) or over-drained it (lower: small charges
        releasing other rows' large slots).
        """
        async with self._lock:
            self._in_flight = max(0, self._in_flight - 1)
            self._in_flight_bytes = max(0, self._in_flight_bytes - actual_bytes)
            outstanding = self._large_charge_sizes.get(actual_bytes, 0)
            if outstanding > 0:
                if outstanding == 1:
                    del self._large_charge_sizes[actual_bytes]
                else:
                    self._large_charge_sizes[actual_bytes] = outstanding - 1
                self._large_in_flight = max(0, self._large_in_flight - 1)
        # Emit after releasing the lock (plan § 0.3 forbids await
        # inside async with lock).
        await self._saturation_balance.set(self._in_flight_bytes)

    async def update_caps(self, snapshot_saturation: SaturationCfg) -> None:
        """Update the gate's caps from a fresh :class:`SaturationCfg`.

        Called by the hot-reload handler after the snapshot is swapped.
        The update is protected by the same lock as admit/release, so a
        cap change cannot interleave with a half-completed admit.

        In-flight counters (``_in_flight``, ``_in_flight_bytes``,
        ``_large_in_flight``) are NOT reset — they reflect actual current
        state. Newly-arriving admits are evaluated against the new caps
        immediately. A changed ``large_body_threshold_bytes`` applies to
        NEW classifications only: in-flight large charges release
        against their remembered charge-time classification (R9-6), so
        the class ledger stays exact across the transition.

        Args:
            snapshot_saturation: The freshly-loaded ``SaturationCfg``
                sub-block from the validated reloaded Settings. Every
                probe-fillable field is guaranteed non-None by the
                Settings validator.
        """
        # Validator (Settings._resolve_defaults) fills every probe-fillable
        # field; assert non-None so the gate's int-only state stays well-typed.
        assert snapshot_saturation.max_in_flight is not None
        assert snapshot_saturation.max_in_flight_bytes is not None
        assert snapshot_saturation.max_disk_bytes is not None
        assert snapshot_saturation.large_body_threshold_bytes is not None
        assert snapshot_saturation.max_large_in_flight is not None
        async with self._lock:
            self._max_in_flight = snapshot_saturation.max_in_flight
            self._max_in_flight_bytes = snapshot_saturation.max_in_flight_bytes
            self._max_disk_bytes = snapshot_saturation.max_disk_bytes
            self._large_body_threshold_bytes = snapshot_saturation.large_body_threshold_bytes
            self._max_large_in_flight = snapshot_saturation.max_large_in_flight
