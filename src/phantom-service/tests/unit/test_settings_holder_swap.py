"""Behavioral smoke test for SettingsHolder concurrent reads vs replace.

A reader polls ``snapshot_for(id)`` many times while a writer fires
``replace`` mid-poll. Every observation must be one of the two known
snapshots — never a half-applied dict missing the instance key or
returning an obsolete partial map. The existing
``test_settings_holder.py::test_holder_replace_is_atomic`` covers the
same property; this file is a higher-iteration version so a future
regression in the atomicity contract trips a dedicated test.
"""

from __future__ import annotations

import asyncio

import pytest
from phantom.instances.settings_holder import SettingsHolder

from .conftest import make_snapshot


@pytest.mark.asyncio
async def test_holder_swap_visible_to_reader_on_next_call() -> None:
    """Reader sees either A or B on every call, never a half-applied map.

    Constructs a holder with snapshot A; spawns a reader that polls
    ``snapshot_for(id)`` 100 times with ``asyncio.sleep(0)`` between
    calls so the writer's coroutine gets a chance to run between
    reads. The writer fires ``replace({id: snap_B})`` mid-poll.

    Assertion: every observation's identity is in ``{id(A), id(B)}``;
    a half-applied state (KeyError on the instance id, or a returned
    object that isn't either known snapshot) would be a bug.
    """
    holder = SettingsHolder()
    snap_a = make_snapshot()
    snap_b = make_snapshot()
    await holder.replace({"primary": snap_a})

    observations: list[int] = []
    iterations = 100

    async def reader() -> None:
        for _ in range(iterations):
            observations.append(id(holder.snapshot_for("primary")))
            await asyncio.sleep(0)

    async def writer() -> None:
        # One mid-poll swap; the property under test is "between any
        # two reads either A or B is visible, never both".
        await asyncio.sleep(0)
        await holder.replace({"primary": snap_b})

    await asyncio.gather(reader(), writer())

    valid = {id(snap_a), id(snap_b)}
    bad = [obs for obs in observations if obs not in valid]
    assert not bad, f"Observed {len(bad)} half-applied snapshots"
    # Sanity — both pre- and post-swap snapshots SHOULD have been observed.
    assert id(snap_a) in observations
    assert id(snap_b) in observations
