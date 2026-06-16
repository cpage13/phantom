"""Holds the live settings snapshot per instance, under an asyncio.Lock.

The composition root constructs one :class:`SettingsHolder` per app
lifespan; the hot-reload SIGHUP handler swaps the snapshots atomically.
Worker coroutines call :meth:`SettingsHolder.snapshot_for` to read the
live snapshot.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field

from phantom.instances.snapshot import InstanceSettingsSnapshot


@dataclass
class SettingsHolder:
    """Mutable holder for the per-instance settings snapshots.

    Reads are lock-free (read of a dict reference is atomic under the
    GIL). Writes (hot reload) acquire the lock and replace the dict
    reference atomically.
    """

    _snapshots: dict[str, InstanceSettingsSnapshot] = field(default_factory=dict)
    _lock: asyncio.Lock = field(default_factory=asyncio.Lock)

    def snapshot_for(self, instance_id: str) -> InstanceSettingsSnapshot:
        """Return the current snapshot for ``instance_id``.

        Args:
            instance_id: The :attr:`InstanceCfg.id` of the instance whose
                snapshot to return.

        Returns:
            The frozen :class:`InstanceSettingsSnapshot` currently in
            effect for ``instance_id``.

        Raises:
            KeyError: If the instance is unknown to the holder.
        """
        return self._snapshots[instance_id]

    async def replace(self, snapshots: dict[str, InstanceSettingsSnapshot]) -> None:
        """Atomically replace every snapshot.

        Called by the hot-reload handler after a fresh ``Settings`` has
        been loaded and validated. The replacement is a single
        dict-reference swap under the lock; readers between writes see
        either the old or the new state, never half-applied.

        Args:
            snapshots: The complete per-instance snapshot map to install.
                Keys are :attr:`InstanceCfg.id`; values are the freshly
                built :class:`InstanceSettingsSnapshot` instances.
        """
        async with self._lock:
            self._snapshots = dict(snapshots)


__all__ = ["SettingsHolder"]
