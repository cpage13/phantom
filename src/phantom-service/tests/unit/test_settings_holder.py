"""Unit tests for phantom.instances.settings_holder.SettingsHolder."""

from __future__ import annotations

import asyncio

import pytest
from phantom.instances.settings_holder import SettingsHolder

from .conftest import make_snapshot


@pytest.mark.asyncio
async def test_holder_returns_current_snapshot() -> None:
    """``snapshot_for(id)`` returns the snapshot installed via ``replace``."""
    holder = SettingsHolder()
    snapshot = make_snapshot()
    await holder.replace({"primary": snapshot})
    assert holder.snapshot_for("primary") is snapshot


@pytest.mark.asyncio
async def test_holder_unknown_instance_raises() -> None:
    """``snapshot_for`` on an unknown id raises ``KeyError``."""
    holder = SettingsHolder()
    with pytest.raises(KeyError):
        holder.snapshot_for("nope")


@pytest.mark.asyncio
async def test_holder_replace_is_atomic() -> None:
    """Concurrent readers see either the pre-state or post-state, never a mix.

    Spawns N read tasks that race a single writer. The writer toggles the
    snapshot reference between two known identities; the readers gather
    identities across many iterations. Every observation must be one of
    the two known snapshots — never a half-applied dict (missing the
    instance key entirely, or returning an obsolete partial map).
    """
    holder = SettingsHolder()
    snap_a = make_snapshot()
    snap_b = make_snapshot()
    await holder.replace({"primary": snap_a})

    reader_iterations = 200
    observations: list[object] = []

    async def reader() -> None:
        for _ in range(reader_iterations):
            observations.append(holder.snapshot_for("primary"))
            await asyncio.sleep(0)

    async def writer() -> None:
        for i in range(20):
            await holder.replace({"primary": snap_b if i % 2 == 0 else snap_a})
            await asyncio.sleep(0)

    await asyncio.gather(*(reader() for _ in range(4)), writer())

    valid_identities = {id(snap_a), id(snap_b)}
    bad = [obs for obs in observations if id(obs) not in valid_identities]
    assert not bad, f"Observed {len(bad)} half-applied snapshots: {bad[:3]!r}"
