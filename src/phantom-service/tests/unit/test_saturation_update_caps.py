"""Unit tests for SaturationGate.update_caps (plan §11.5).

Hot reload calls ``update_caps`` after the snapshot swap so a cap
change takes effect on the next admit decision, without resetting the
in-flight counters.
"""

from __future__ import annotations

import pytest
from phantom.config.settings import SaturationCfg
from phantom.workers.saturation import (
    AdmissionGranted,
    AdmissionRefusedSaturation,
    SaturationGate,
)


def _saturation_cfg(
    *,
    max_in_flight: int,
    max_in_flight_bytes: int = 10_000,
    max_disk_bytes: int = 100_000,
    large_body_threshold_bytes: int = 0,
    max_large_in_flight: int = 0,
) -> SaturationCfg:
    """Build a :class:`SaturationCfg` with every probe-fillable field set.

    The validator's non-None invariant for the gate constructor must
    hold here too; the helper makes the test-side intent explicit.
    """
    return SaturationCfg(
        max_in_flight=max_in_flight,
        max_in_flight_bytes=max_in_flight_bytes,
        max_disk_bytes=max_disk_bytes,
        large_body_threshold_bytes=large_body_threshold_bytes,
        max_large_in_flight=max_large_in_flight,
    )


@pytest.mark.asyncio
async def test_update_caps_changes_admit_threshold() -> None:
    """Tightening + relaxing the cap takes effect on the next admit.

    Sequence:
      1. Construct a gate with ``max_in_flight=1``.
      2. Admit one row (success).
      3. Admit a second row (refused on row-count cap).
      4. Call ``update_caps`` with ``max_in_flight=10``.
      5. Admit a second row (now succeeds).
    """
    gate = SaturationGate(
        max_in_flight=1,
        max_in_flight_bytes=10_000,
        max_disk_bytes=100_000,
    )
    r1 = await gate.admit(100)
    assert isinstance(r1, AdmissionGranted)
    r2 = await gate.admit(100)
    assert isinstance(r2, AdmissionRefusedSaturation)
    # Cap update — in_flight stays at 1, new admits use the new ceiling.
    await gate.update_caps(_saturation_cfg(max_in_flight=10))
    assert gate.in_flight == 1
    r3 = await gate.admit(100)
    assert isinstance(r3, AdmissionGranted)


@pytest.mark.asyncio
async def test_update_caps_preserves_in_flight_counters() -> None:
    """Cap update never resets the in-flight counters.

    After two admits the gate is at row=2 and bytes=300. ``update_caps``
    must leave those exactly alone — operators expect cap changes to
    apply forward, not retroactively cancel in-progress rows.
    """
    gate = SaturationGate(
        max_in_flight=10,
        max_in_flight_bytes=10_000,
        max_disk_bytes=100_000,
    )
    await gate.admit(100)
    await gate.admit(200)
    assert gate.in_flight == 2
    assert gate.in_flight_bytes == 300
    await gate.update_caps(_saturation_cfg(max_in_flight=20, max_in_flight_bytes=20_000))
    assert gate.in_flight == 2
    assert gate.in_flight_bytes == 300
    assert gate.max_in_flight == 20
    assert gate.max_in_flight_bytes == 20_000
