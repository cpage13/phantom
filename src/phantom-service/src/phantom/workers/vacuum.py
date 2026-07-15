"""VacuumScheduler — cron-style SQLite VACUUM, only when in-flight=0."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable
from datetime import UTC, datetime

from phantom.instances.context import InstanceContext

logger = logging.getLogger(__name__)


def _parse_cron(spec: str) -> tuple[int | None, int | None, int | None, int | None, int | None]:
    """Parse a minimal cron string ``m h dom mon dow`` (each ``*`` or int).

    Returns:
        Tuple of five optional integers. ``None`` means ``*`` (wildcard).
    """
    parts = spec.split()
    if len(parts) != 5:
        raise ValueError(f"Cron spec must have 5 fields, got {len(parts)}: {spec!r}")
    return tuple(None if p == "*" else int(p) for p in parts)  # type: ignore[return-value]


def _matches_cron(spec: str, now: datetime) -> bool:
    """True if ``now`` matches ``spec`` (minute granularity)."""
    minute, hour, dom, month, dow = _parse_cron(spec)
    if minute is not None and now.minute != minute:
        return False
    if hour is not None and now.hour != hour:
        return False
    if dom is not None and now.day != dom:
        return False
    if month is not None and now.month != month:
        return False
    if dow is not None:
        # Python: Monday=0..Sunday=6. Cron: Sunday=0..Saturday=6.
        cron_dow = (now.weekday() + 1) % 7
        if cron_dow != dow:
            return False
    return True


class VacuumScheduler:
    """Periodic VACUUM scheduler, gated on in-flight=0."""

    def __init__(
        self,
        *,
        instance: InstanceContext,
        cron_spec: str,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        """Construct the scheduler.

        Args:
            instance: The instance whose persistent store gets VACUUMed.
            cron_spec: Cron-style time expression.
            clock: Injectable clock for tests.
        """
        self._instance = instance
        self._cron = cron_spec
        self._clock = clock or (lambda: datetime.now(tz=UTC))
        self._last_run_minute: tuple[int, int, int, int, int] | None = None

    async def run(self, stop_event: asyncio.Event) -> None:
        """Tick every ~30s; fire VACUUM when the cron matches and saturation=0."""
        while not stop_event.is_set():
            await self._tick(self._clock())
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=30)
            except TimeoutError:
                continue

    async def _tick(self, now: datetime) -> None:
        """One scheduling decision at ``now``; fires at most one VACUUM.

        The complete gate in one place: a minute-slot not already fired
        (same-minute dedup), a cron match, and ``saturation.in_flight == 0``
        (the flash-wear invariant: never VACUUM under load). :meth:`run` is
        loop coordination around this method and nothing else, so a test can
        drive real ticks at injected times without copying any decision
        logic.
        """
        slot = (now.year, now.month, now.day, now.hour, now.minute)
        if (
            slot != self._last_run_minute
            and _matches_cron(self._cron, now)
            and self._instance.saturation.in_flight == 0
        ):
            self._last_run_minute = slot
            await self._vacuum()

    async def _vacuum(self) -> None:
        """Run VACUUM on the persistent store via the Protocol method."""
        logger.info("Running SQLite VACUUM on persistent store")
        await self._instance.store.vacuum()
