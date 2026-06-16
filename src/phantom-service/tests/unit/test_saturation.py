"""Unit tests for phantom.workers.saturation."""

from __future__ import annotations

import pytest
from phantom.workers.saturation import (
    AdmissionGranted,
    AdmissionRefusedDiskPressure,
    AdmissionRefusedSaturation,
    SaturationGate,
)


@pytest.mark.asyncio
async def test_admit_release() -> None:
    """Admit/release round-trip the counters."""
    gate = SaturationGate(max_in_flight=2, max_in_flight_bytes=1000, max_disk_bytes=10000)
    assert isinstance(await gate.admit(100), AdmissionGranted)
    assert gate.in_flight == 1
    assert isinstance(await gate.admit(100), AdmissionGranted)
    # Third would exceed max_in_flight.
    assert isinstance(await gate.admit(100), AdmissionRefusedSaturation)
    await gate.release(100)
    assert gate.in_flight == 1


@pytest.mark.asyncio
async def test_bytes_cap() -> None:
    """Admit refuses when bytes cap would be exceeded."""
    gate = SaturationGate(max_in_flight=100, max_in_flight_bytes=500, max_disk_bytes=100000)
    assert isinstance(await gate.admit(400), AdmissionGranted)
    assert isinstance(await gate.admit(200), AdmissionRefusedSaturation)
    assert gate.in_flight == 1
    assert gate.in_flight_bytes == 400


@pytest.mark.asyncio
async def test_saturated_property() -> None:
    """Saturated reflects cap-hit state."""
    gate = SaturationGate(max_in_flight=1, max_in_flight_bytes=10000, max_disk_bytes=100000)
    assert gate.saturated is False
    await gate.admit(100)
    assert gate.saturated is True


# ---------------------------------------------------------------------------
# §2.3 disk-pressure + typed AdmissionResult classification (plan §13.4).
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_admit_returns_granted_on_success() -> None:
    """Successful admit returns :class:`AdmissionGranted`."""
    gate = SaturationGate(max_in_flight=10, max_in_flight_bytes=10000, max_disk_bytes=100000)
    result = await gate.admit(100)
    assert isinstance(result, AdmissionGranted)


@pytest.mark.asyncio
async def test_admit_returns_saturation_refused_on_row_cap() -> None:
    """Row-count cap exceeded -> :class:`AdmissionRefusedSaturation`."""
    gate = SaturationGate(max_in_flight=1, max_in_flight_bytes=10000, max_disk_bytes=100000)
    await gate.admit(100)
    result = await gate.admit(100)
    assert isinstance(result, AdmissionRefusedSaturation)


@pytest.mark.asyncio
async def test_admit_returns_saturation_on_bytes_cap() -> None:
    """Bytes cap exceeded -> :class:`AdmissionRefusedSaturation`."""
    gate = SaturationGate(max_in_flight=100, max_in_flight_bytes=500, max_disk_bytes=100000)
    await gate.admit(400)
    result = await gate.admit(200)
    assert isinstance(result, AdmissionRefusedSaturation)


@pytest.mark.asyncio
async def test_admit_returns_disk_pressure_on_disk_cap() -> None:
    """Disk usage at cap -> :class:`AdmissionRefusedDiskPressure` (§2.3)."""
    gate = SaturationGate(max_in_flight=100, max_in_flight_bytes=10000, max_disk_bytes=1000)
    gate.set_disk_usage_bytes(1000)  # at cap
    result = await gate.admit(100)
    assert isinstance(result, AdmissionRefusedDiskPressure)


@pytest.mark.asyncio
async def test_disk_pressure_takes_precedence_over_saturation() -> None:
    """When both disk and bytes/count caps would refuse, disk_pressure wins."""
    gate = SaturationGate(max_in_flight=1, max_in_flight_bytes=10, max_disk_bytes=1000)
    gate.set_disk_usage_bytes(1000)
    # Even though bytes cap would also reject, disk is checked first so the
    # operator sees the load-bearing cause.
    result = await gate.admit(100)
    assert isinstance(result, AdmissionRefusedDiskPressure)


@pytest.mark.asyncio
async def test_set_disk_usage_bytes_floors_at_zero() -> None:
    """Negative disk usage observations clamp to zero."""
    gate = SaturationGate(max_in_flight=10, max_in_flight_bytes=10000, max_disk_bytes=1000)
    gate.set_disk_usage_bytes(-50)
    assert gate.disk_usage_bytes == 0


@pytest.mark.asyncio
async def test_max_disk_bytes_zero_disables_disk_check() -> None:
    """``max_disk_bytes=0`` means 'check disabled'; admits never refuse on disk."""
    gate = SaturationGate(max_in_flight=10, max_in_flight_bytes=10000, max_disk_bytes=0)
    gate.set_disk_usage_bytes(1_000_000_000_000)
    result = await gate.admit(100)
    assert isinstance(result, AdmissionGranted)


# ---------------------------------------------------------------------------
# §1.3 size-aware accounting: large-body class.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_large_body_class_admits_up_to_max_large() -> None:
    """Bodies at/above threshold are admitted until ``max_large_in_flight``."""
    gate = SaturationGate(
        max_in_flight=100,
        max_in_flight_bytes=10_000_000_000,
        max_disk_bytes=10_000_000_000,
        large_body_threshold_bytes=1_000_000,  # 1 MB
        max_large_in_flight=2,
    )
    # Two large bodies admitted.
    r1 = await gate.admit(5_000_000)
    r2 = await gate.admit(5_000_000)
    assert isinstance(r1, AdmissionGranted)
    assert isinstance(r2, AdmissionGranted)
    assert gate.large_in_flight == 2
    # Third large body refused.
    r3 = await gate.admit(5_000_000)
    assert isinstance(r3, AdmissionRefusedSaturation)
    # Small bodies still admitted alongside.
    r4 = await gate.admit(100)
    assert isinstance(r4, AdmissionGranted)
    assert gate.large_in_flight == 2


@pytest.mark.asyncio
async def test_release_decrements_large_counter() -> None:
    """Releasing a large body brings the counter back down (§1.3)."""
    gate = SaturationGate(
        max_in_flight=10,
        max_in_flight_bytes=10_000_000_000,
        max_disk_bytes=10_000_000_000,
        large_body_threshold_bytes=1_000_000,
        max_large_in_flight=1,
    )
    await gate.admit(5_000_000)
    assert gate.large_in_flight == 1
    # Another large body refused while the first is in flight.
    result = await gate.admit(5_000_000)
    assert isinstance(result, AdmissionRefusedSaturation)
    # After release, room for another large body opens.
    await gate.release(5_000_000)
    assert gate.large_in_flight == 0
    result = await gate.admit(5_000_000)
    assert isinstance(result, AdmissionGranted)


@pytest.mark.asyncio
async def test_large_class_disabled_when_threshold_zero() -> None:
    """``large_body_threshold_bytes=0`` disables the class entirely."""
    gate = SaturationGate(
        max_in_flight=100,
        max_in_flight_bytes=10_000_000_000,
        max_disk_bytes=10_000_000_000,
        large_body_threshold_bytes=0,
        max_large_in_flight=0,
    )
    # Even huge bodies don't increment the large counter.
    await gate.admit(5_000_000_000)  # 5 GB
    assert gate.large_in_flight == 0


@pytest.mark.asyncio
async def test_small_body_below_threshold_unaffected_by_large_cap() -> None:
    """Bodies below the threshold are not counted in the large class."""
    gate = SaturationGate(
        max_in_flight=100,
        max_in_flight_bytes=10_000_000_000,
        max_disk_bytes=10_000_000_000,
        large_body_threshold_bytes=1_000_000,
        max_large_in_flight=0,  # zero would refuse ALL large bodies
    )
    # A small body (under threshold) is fine even with max_large_in_flight=0.
    result = await gate.admit(500_000)
    assert isinstance(result, AdmissionGranted)
    # A large body is rejected by the large-class cap.
    result = await gate.admit(2_000_000)
    assert isinstance(result, AdmissionRefusedSaturation)
