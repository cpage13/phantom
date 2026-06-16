"""DiskPressureProbe — periodic disk-usage sampler for the saturation gate.

The gate's ``max_disk_bytes`` was a documented YAML knob that the code
never consulted. Without it, a misconfigured producer would happily fill its
SD card via the file body store until ``os.fsync`` raised ENOSPC mid-write,
the sender's persist_now raised, and RAM filled until OOM-kill.

This probe runs out of band so the gate's synchronous ``admit`` path
stays I/O-free. Each tick:

1. Reads ``FileBodyStore.total_bytes`` (an ``os.walk``-based sum).
2. Calls :meth:`SaturationGate.set_disk_usage_bytes` with the result.
3. Sleeps :attr:`poll_interval_seconds` until the next tick.

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

# 30 s default: the disk_usage_bytes observation feeds operator-visible
# /v1/admin/status (Item §6.1) and the admit decision; staler than 30 s
# risks tripping max_disk_bytes only after the disk is already full,
# tighter than 5 s wastes CPU on `os.walk` for no observable benefit.
# Fixed constant — operators tune the cap (max_disk_bytes) and live with
# this probe cadence.
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
        """Main loop — sample disk usage and update the gate until stopped."""
        # If max_disk_bytes is unset, the probe is a no-op — exit early.
        if self._instance.saturation.max_disk_bytes <= 0:
            logger.info(
                "DiskPressureProbe disabled (max_disk_bytes=0 for instance %s)",
                self._instance.cfg.id,
            )
            return
        while not stop_event.is_set():
            try:
                await self._probe_once()
            except Exception:
                # Broad except — a transient probe failure must not kill
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
