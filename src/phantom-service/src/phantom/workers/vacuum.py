"""VacuumScheduler - cron-style SQLite VACUUM, only when in-flight=0."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable
from datetime import UTC, datetime

from phantom.instances.context import InstanceContext

logger = logging.getLogger(__name__)

# How often the scheduler wakes to compare the clock against ``vacuum_cron``.
# The cron expression's finest granularity is one minute, so any period
# comfortably below 60 s samples every matching minute exactly once; 30 s is
# the same value ``disk_pressure`` uses for its probe and keeps shutdown
# latency at most one period. NOT a knob: nothing about a deployment changes
# the right sampling rate for a one-minute grid, and ``vacuum_cron`` answers a
# different question (WHEN a VACUUM fires, not how often the loop checks).
_POLL_INTERVAL_SECONDS = 30


# Inclusive (low, high) bound per cron field, in ``m h dom mon dow`` order.
# Ranges are validated because an out-of-range field is not a harmless typo: it
# matches no instant, so the VACUUM silently never fires and the operator has
# no signal at all.
_CRON_FIELD_BOUNDS: tuple[tuple[int, int], ...] = ((0, 59), (0, 23), (1, 31), (1, 12), (0, 6))
_CRON_FIELD_NAMES: tuple[str, ...] = ("minute", "hour", "day-of-month", "month", "day-of-week")


class CronSpecError(ValueError):
    """A ``vacuum_cron`` expression this scheduler cannot honour."""


def _parse_field(raw: str, index: int) -> frozenset[int] | None:
    """Parse one cron field into the set of values it matches.

    Supports ``*`` (any), a bare integer, and the ``*/N`` step form. The step
    form is supported because it is the single most common cron idiom and
    rejecting it was not a harmless limitation: the parse error escaped the
    scheduler's first tick and crash-looped the process.

    Args:
        raw: The field text.
        index: Position in ``m h dom mon dow``, used for bounds and messages.

    Returns:
        ``None`` for a wildcard, otherwise the frozen set of matching values.

    Raises:
        CronSpecError: The field is malformed or out of range for its position.
    """
    low, high = _CRON_FIELD_BOUNDS[index]
    name = _CRON_FIELD_NAMES[index]
    if raw == "*":
        return None
    if raw.startswith("*/"):
        step_text = raw[2:]
        if not step_text.isdigit() or int(step_text) < 1:
            raise CronSpecError(f"{name} step must be a positive integer, got {raw!r}")
        step = int(step_text)
        return frozenset(range(low, high + 1, step))
    if not (raw.isdigit() or (raw.startswith("-") and raw[1:].isdigit())):
        raise CronSpecError(f"{name} must be '*', an integer, or '*/N', got {raw!r}")
    value = int(raw)
    if not low <= value <= high:
        raise CronSpecError(f"{name} must be between {low} and {high}, got {value}")
    return frozenset({value})


def _parse_cron(spec: str) -> tuple[frozenset[int] | None, ...]:
    """Parse a cron string ``m h dom mon dow`` into per-field match sets.

    Args:
        spec: The expression, five whitespace-separated fields.

    Returns:
        Five entries, each ``None`` for a wildcard or a set of matching values.

    Raises:
        CronSpecError: Wrong field count, or any field malformed or out of range.
    """
    parts = spec.split()
    if len(parts) != len(_CRON_FIELD_BOUNDS):
        raise CronSpecError(
            f"Cron spec must have {len(_CRON_FIELD_BOUNDS)} fields, got {len(parts)}: {spec!r}"
        )
    return tuple(_parse_field(part, i) for i, part in enumerate(parts))


def _matches_parsed(parsed: tuple[frozenset[int] | None, ...], now: datetime) -> bool:
    """True if ``now`` matches an ALREADY-PARSED spec (minute granularity).

    Takes the parsed form rather than the text so the expression is parsed once
    at construction instead of on every tick. That is what makes a malformed
    spec a startup decision rather than a per-tick exception.
    """
    minute, hour, dom, month, dow = parsed
    if minute is not None and now.minute not in minute:
        return False
    if hour is not None and now.hour not in hour:
        return False
    if dom is not None and now.day not in dom:
        return False
    if month is not None and now.month not in month:
        return False
    if dow is not None:
        # Python: Monday=0..Sunday=6. Cron: Sunday=0..Saturday=6.
        cron_dow = (now.weekday() + 1) % 7
        if cron_dow not in dow:
            return False
    return True


def _matches_cron(spec: str, now: datetime) -> bool:
    """True if ``now`` matches ``spec``, parsing the text each call.

    A convenience over :func:`_parse_cron` plus :func:`_matches_parsed`, kept
    for callers that hold only the text. The scheduler itself does NOT use it:
    it parses once at construction, which is what turns a malformed expression
    into a startup decision instead of a per-tick exception.

    Raises:
        CronSpecError: ``spec`` is malformed or out of range.
    """
    return _matches_parsed(_parse_cron(spec), now)


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
        # Parse ONCE, here, rather than on every tick. A malformed expression
        # used to raise out of the first `_tick`, which runs before any sleep
        # and has no error handling, so it cancelled every sibling in the
        # composition root's TaskGroup, reached the fatal-worker bridge, and the
        # orchestrator restarted into the identical crash within milliseconds.
        # The service never stayed up long enough to serve ingress or to accept
        # the admin reload that would have fixed the config, and every restart
        # re-quarantined the RAM-resident rows. Degrading to inert instead
        # honours ADR-025: never refuse to boot on recoverable config.
        try:
            self._parsed: tuple[frozenset[int] | None, ...] | None = _parse_cron(cron_spec)
        except CronSpecError:
            self._parsed = None
            logger.error(
                "vacuum_cron %r is not a valid expression; VACUUM is DISABLED for "
                "instance %s until the config is corrected. Supported field forms "
                "are '*', an integer in range, and '*/N'.",
                cron_spec,
                instance.cfg.id,
            )

    @property
    def enabled(self) -> bool:
        """False when the configured expression could not be parsed."""
        return self._parsed is not None

    async def run(self, stop_event: asyncio.Event) -> None:
        """Tick every ``_POLL_INTERVAL_SECONDS``; fire VACUUM when the cron matches.

        The VACUUM fires only on a matching cron minute with saturation=0. A
        scheduler whose expression did not parse is inert: it waits for the
        stop event and schedules nothing, so a config typo costs the VACUUM and
        nothing else.
        """
        if not self.enabled:
            await stop_event.wait()
            return
        while not stop_event.is_set():
            await self._tick(self._clock())
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=_POLL_INTERVAL_SECONDS)
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
        if self._parsed is None:
            return
        slot = (now.year, now.month, now.day, now.hour, now.minute)
        if (
            slot != self._last_run_minute
            and _matches_parsed(self._parsed, now)
            and self._instance.saturation.in_flight == 0
        ):
            self._last_run_minute = slot
            await self._vacuum()

    async def _vacuum(self) -> None:
        """Run VACUUM on the persistent store via the Protocol method."""
        logger.info("Running SQLite VACUUM on persistent store")
        await self._instance.store.vacuum()
