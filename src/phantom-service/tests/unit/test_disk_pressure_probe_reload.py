"""F13: the disk-pressure probe re-reads its cap on every tick.

``DiskPressureProbe.run`` used to decide its whole future at loop entry:
``max_disk_bytes <= 0`` logged once and returned. The probe is spawned
exactly once per instance in the lifespan, and a reload pushes new caps
into the gate through ``update_caps`` without touching the probe, which
by then is a coroutine that has already returned. So after a ``0`` to
positive reload of ``saturation.max_disk_bytes``, the gate's
``_disk_usage_bytes`` stayed at its initial ``0`` forever, the admit
check compared ``0 + declared`` against the cap, and nothing ever
tripped: the disk filled until ``os.fsync`` raised ENOSPC mid-write,
while ADR-013 listed the knob as reloadable and the knob-matrix test
passed because the gate really had received the cap.

These tests drive the real loop with a short interval and a stub file
body store, and cover both directions plus the operator-facing logging.
The reverse leg (positive to ``0``) is benign at the gate, which is why
the probe pauses the walk rather than zeroing the observation: nothing
reads it while the cap is disabled, and zeroing would replace a stale
truth with a fresh lie.
"""

from __future__ import annotations

import asyncio
from typing import Any
from unittest.mock import MagicMock

import pytest
from phantom.config.settings import SaturationCfg
from phantom.workers.disk_pressure import DiskPressureProbe
from phantom.workers.saturation import (
    AdmissionGranted,
    AdmissionRefusedDiskPressure,
    SaturationGate,
)

pytestmark = pytest.mark.asyncio

# Probe cadence for these tests. Production's is a fixed 30 s module
# constant; the probe takes it as a constructor argument, so no
# production value has to move for the loop to be observable here.
_TICK_SECONDS = 0.01

# Upper bound on any "wait for the loop to do the thing" poll. Every
# assertion polls a condition under this deadline rather than sleeping a
# fixed total, so a passing run is fast and a failing one cannot hang.
_WAIT_BUDGET_SECONDS = 2.0

# What the stub file body store reports as the on-disk total.
_STUB_DISK_BYTES = 4096

# The declared size of the admission each test attempts.
_DECLARED_BYTES = 1024

# The cap a reload raises max_disk_bytes to. Note
# _STUB_DISK_BYTES + _DECLARED_BYTES >= _POSITIVE_CAP, which is what
# makes the post-fix refusal deterministic.
_POSITIVE_CAP = 2048

# Permissive row and byte caps. The disk arm is checked FIRST in admit,
# so post-fix the refusal is attributable to disk whatever these are;
# they are permissive because the PRE-FIX assertion of test 1 and the
# whole of test 3 require the admission to be GRANTED, and a tight row
# or byte cap would refuse both for the wrong reason.
_MAX_IN_FLIGHT = 100
_MAX_IN_FLIGHT_BYTES = 1_073_741_824

# Tick budgets for the one assertion that must observe an ABSENCE of
# work: a settle window for a tick already in flight when the cap
# changed, then the quiet window the call count must not move across.
_SETTLE_TICKS = 3
_QUIET_TICKS = 10

# Ticks to let the probe task reach its own first cap read before a test
# reloads that cap. ``asyncio.create_task`` only SCHEDULES the coroutine,
# and ``SaturationGate.update_caps`` takes an uncontended lock without
# yielding, so a reload issued immediately after the spawn lands before
# the probe has looked at anything: the pre-fix entry check would then
# see the NEW cap and the witness would pass on the broken tree.
_START_TICKS = 3


def _saturation_cfg(*, max_disk_bytes: int) -> SaturationCfg:
    """Build a :class:`SaturationCfg` with every probe-fillable field set.

    ``SaturationCfg`` defaults all five numeric caps to ``None`` (the
    fill is ``Settings._resolve_defaults``' job, not the model's), and
    ``SaturationGate.update_caps`` opens with five bare asserts. A
    partial cfg therefore raises a message-less ``AssertionError``
    identically on the pre-fix and post-fix trees, which would turn the
    M2 witness into a crash rather than a behavioural failure.
    """
    return SaturationCfg(
        max_in_flight=_MAX_IN_FLIGHT,
        max_in_flight_bytes=_MAX_IN_FLIGHT_BYTES,
        max_disk_bytes=max_disk_bytes,
        large_body_threshold_bytes=0,
        max_large_in_flight=0,
    )


class _CountingBodyStore:
    """A file body store stub that reports a fixed total and counts calls."""

    def __init__(self) -> None:
        """Start with no observed calls."""
        self.calls = 0

    async def total_bytes(self) -> int:
        """Report the module's stub total and record the walk."""
        self.calls += 1
        return _STUB_DISK_BYTES


def _build_probe(
    *, max_disk_bytes: int
) -> tuple[DiskPressureProbe, SaturationGate, _CountingBodyStore]:
    """Assemble a probe over a real gate and a counting store stub.

    The probe needs exactly three things from its instance:
    ``saturation``, ``file_body_store.total_bytes()`` and ``cfg.id``.
    Everything else is a ``MagicMock``.
    """
    gate = SaturationGate(
        max_in_flight=_MAX_IN_FLIGHT,
        max_in_flight_bytes=_MAX_IN_FLIGHT_BYTES,
        max_disk_bytes=max_disk_bytes,
    )
    store = _CountingBodyStore()
    instance = MagicMock()
    instance.saturation = gate
    instance.file_body_store = store
    instance.cfg.id = "inst-a"
    probe = DiskPressureProbe(instance=instance, poll_interval_seconds=_TICK_SECONDS)
    return probe, gate, store


async def _await_condition(predicate: Any, message: str) -> None:
    """Poll ``predicate`` under the module's deadline, then fail loudly."""
    loop = asyncio.get_running_loop()
    deadline = loop.time() + _WAIT_BUDGET_SECONDS
    while loop.time() < deadline:
        if predicate():
            return
        await asyncio.sleep(_TICK_SECONDS)
    raise AssertionError(message)


async def test_probe_enforces_a_cap_reloaded_up_from_zero() -> None:
    """The F13 defect itself: a cap raised from zero must be enforced.

    Objective: boot with ``max_disk_bytes=0``, start the probe, then push
    a positive cap exactly as ``apply_reload`` does. Success is the gate
    carrying a real observation within the budget AND refusing an
    admission that would cross the new cap.

    Pre-fix ``run`` returned at loop entry, so the observation never left
    ``0`` and an admission that should have been refused was granted. The
    poll is deadline-bounded, so the pre-fix run fails rather than hangs.
    """
    probe, gate, _store = _build_probe(max_disk_bytes=0)
    stop = asyncio.Event()
    task = asyncio.create_task(probe.run(stop))
    try:
        await asyncio.sleep(_TICK_SECONDS * _START_TICKS)
        await gate.update_caps(_saturation_cfg(max_disk_bytes=_POSITIVE_CAP))
        await _await_condition(
            lambda: gate.disk_usage_bytes == _STUB_DISK_BYTES,
            "the probe never sampled after the cap was reloaded up from zero; "
            f"gate.disk_usage_bytes is still {gate.disk_usage_bytes}",
        )
        assert isinstance(await gate.admit(_DECLARED_BYTES), AdmissionRefusedDiskPressure)
    finally:
        stop.set()
        await task


async def test_probe_stops_sampling_when_the_cap_is_reloaded_to_zero() -> None:
    """The reverse leg, and proof the loop survives it.

    Objective: a cap reloaded down to ``0`` disables the class, so the
    probe must stop walking the body tree, which has no reader while the
    cap is off. Success is the stub's call count going flat while the
    task stays alive. Pre-fix the walk continued every tick, because
    nothing re-read the cap after entry.
    """
    probe, gate, store = _build_probe(max_disk_bytes=_POSITIVE_CAP)
    stop = asyncio.Event()
    task = asyncio.create_task(probe.run(stop))
    try:
        await _await_condition(
            lambda: store.calls > 0,
            "the probe never sampled under a positive boot cap",
        )
        await gate.update_caps(_saturation_cfg(max_disk_bytes=0))
        # A "nothing happens from here on" assertion cannot be polled for,
        # so it waits: a short settle for any tick already in flight, then
        # a window of several ticks over which the count must not move.
        await asyncio.sleep(_TICK_SECONDS * _SETTLE_TICKS)
        settled = store.calls
        await asyncio.sleep(_TICK_SECONDS * _QUIET_TICKS)
        assert store.calls == settled, (
            "the probe kept walking the body tree after the cap was reloaded "
            f"to zero: {store.calls - settled} extra walk(s) in "
            f"{_QUIET_TICKS} tick budgets"
        )
        assert not task.done(), "the probe loop must survive a disabled cap"
    finally:
        stop.set()
        await task


async def test_zero_cap_admits_despite_a_stale_observation() -> None:
    """A stale observation under a zero cap is harmless at the gate.

    Objective: pin the reason § 3.2.2 leaves the last observation in
    place instead of zeroing it. ``admit`` guards on
    ``self._max_disk_bytes > 0`` before it looks at the observation at
    all, so a large stale reading cannot refuse anything once the cap is
    off. Success is ``AdmissionGranted``.
    """
    probe, gate, _store = _build_probe(max_disk_bytes=_POSITIVE_CAP)
    stop = asyncio.Event()
    task = asyncio.create_task(probe.run(stop))
    try:
        await _await_condition(
            lambda: gate.disk_usage_bytes == _STUB_DISK_BYTES,
            "the probe never installed the observation this test needs to go stale",
        )
        await gate.update_caps(_saturation_cfg(max_disk_bytes=0))
        assert isinstance(await gate.admit(_DECLARED_BYTES), AdmissionGranted)
        assert gate.disk_usage_bytes == _STUB_DISK_BYTES, (
            "the observation must be left alone rather than zeroed: zeroing "
            "would replace a stale truth with a fresh lie"
        )
    finally:
        stop.set()
        await task


async def test_probe_logs_each_transition_once_and_not_every_tick(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """The operator-facing half: one record per transition, never per tick.

    Objective: a probe that logged its state every 30 seconds would be
    noise, and one that never logged would leave the operator guessing
    whether a reload took. Success is exactly one "started" record and
    exactly one "resumed sampling" record across many ticks on both
    sides of the transition.
    """
    probe, gate, store = _build_probe(max_disk_bytes=0)
    stop = asyncio.Event()
    with caplog.at_level("INFO", logger="phantom.workers.disk_pressure"):
        task = asyncio.create_task(probe.run(stop))
        try:
            await asyncio.sleep(_TICK_SECONDS * 5)
            await gate.update_caps(_saturation_cfg(max_disk_bytes=_POSITIVE_CAP))
            await _await_condition(
                lambda: store.calls > 1,
                "the probe never sampled repeatedly after being resumed",
            )
        finally:
            stop.set()
            await task

    messages = [record.getMessage() for record in caplog.records]
    assert len([m for m in messages if "DiskPressureProbe started" in m]) == 1
    assert len([m for m in messages if "resumed sampling" in m]) == 1
