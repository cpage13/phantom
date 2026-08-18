"""DiskPressureProbe — periodic disk-usage sampler for the saturation gate.

The gate's ``max_disk_bytes`` was a documented YAML knob that the code
never consulted. Without it, a misconfigured producer would happily fill its
SD card via the file body store until ``os.fsync`` raised ENOSPC mid-write,
the sender's persist_now raised, and RAM filled until OOM-kill.

This probe runs out of band so the gate's synchronous ``admit`` path
stays I/O-free. Each tick:

1. Re-reads ``saturation.max_disk_bytes`` and logs any enable/disable
   transition. The cap is hot-reloadable (ADR-013), so it is read per
   tick and NEVER decided once at loop entry (F13).
2. While the cap is positive, reads ``FileBodyStore.total_bytes`` (an
   ``os.walk``-based sum).
3. Calls :meth:`SaturationGate.set_disk_usage_bytes` with the result.
4. Sleeps :attr:`poll_interval_seconds` until the next tick.

While the cap is ``0`` the walk is skipped, because the observation has
exactly one consumer and that consumer short-circuits on
``max_disk_bytes > 0`` before it looks at the observation at all. The
last observation is left in place rather than zeroed: zeroing would
replace a stale truth with a fresh lie, and the next tick after a
re-enable overwrites it anyway.

When the cached observation crosses the gate's ``max_disk_bytes`` cap,
the gate's next admit returns
:class:`~phantom.workers.saturation.AdmissionRefusedDiskPressure`, which
the ingress translates into a 503 with code ``disk_pressure`` and a
``Retry-After`` header.
"""

from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from phantom.instances.context import InstanceContext

logger = logging.getLogger(__name__)

# 30 s default: the disk_usage_bytes observation feeds the gate's admit
# decision and nothing else. (An earlier version of this comment also
# claimed it feeds operator-visible /v1/admin/status; that was already
# false, since both admin surfaces compute disk usage from a live
# total_bytes() walk. Corrected with F13, whose skip-the-walk-under-a-
# zero-cap decision rests on the admit path being the sole consumer.)
# Staler than 30 s risks tripping max_disk_bytes only after the disk is
# already full, tighter than 5 s wastes CPU on `os.walk` for no
# observable benefit. Fixed constant: operators tune the cap
# (max_disk_bytes) and live with this probe cadence. The cadence also
# bounds how long a reloaded cap waits for its first observation, since
# the transition is detected and sampled in the SAME tick.
DEFAULT_PROBE_INTERVAL_SECONDS = 30.0


class DiskPressureProbe:
    """Periodic coroutine that refreshes the saturation gate's disk-usage view."""

    def __init__(
        self,
        *,
        instance: InstanceContext,
        poll_interval_seconds: float = DEFAULT_PROBE_INTERVAL_SECONDS,
    ) -> None:
        """Construct the probe.

        Args:
            instance: The instance whose file body store to sample and
                whose saturation gate to update.
            poll_interval_seconds: How often to re-sample.
        """
        self._instance = instance
        self._poll_interval = poll_interval_seconds

    async def run(self, stop_event: asyncio.Event) -> None:
        """Main loop: sample disk usage and update the gate until stopped.

        The cap is re-read on EVERY tick, never decided at entry.
        ``saturation.max_disk_bytes`` is hot-reloadable (ADR-013) and
        ``apply_reload`` pushes it into the gate; a probe that returned at
        boot because the cap was 0 made a later reload unenforceable forever,
        since the gate's disk-usage observation has no other writer (F13).
        """
        cap = self._instance.saturation.max_disk_bytes
        sampling = cap > 0
        logger.info(
            "DiskPressureProbe started for instance %s (sampling=%s, max_disk_bytes=%d)",
            self._instance.cfg.id,
            sampling,
            cap,
        )
        while not stop_event.is_set():
            # One int read per tick, deliberately outside the gate's lock:
            # the same argument set_disk_usage_bytes documents applies, a
            # single int read is atomic under the GIL, and the worst case
            # is one tick decided against a cap that changed microseconds
            # ago. The next tick corrects it.
            cap = self._instance.saturation.max_disk_bytes
            if (cap > 0) != sampling:
                sampling = cap > 0
                logger.info(
                    "DiskPressureProbe %s for instance %s (max_disk_bytes=%d)",
                    "resumed sampling" if sampling else "paused sampling",
                    self._instance.cfg.id,
                    cap,
                )
            if sampling:
                try:
                    await self._probe_once()
                except Exception:
                    # Broad except: a transient probe failure must not kill
                    # the loop; the gate keeps the previous observation.
                    logger.exception("DiskPressureProbe tick failed")
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=self._poll_interval)
            except TimeoutError:
                continue

    async def _probe_once(self) -> None:
        """One sample + one update."""
        bytes_used = await self._instance.file_body_store.total_bytes()
        self._instance.saturation.set_disk_usage_bytes(bytes_used)
