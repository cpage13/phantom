"""Regression test for aggressor finding A-1 (adopted Round 2).

Asserts that a SaturationGate constructed with ``max_in_flight_bytes=0``
refuses every admission, including zero-byte bodies. Round 1 found the
bytes cap admitted 0-byte bodies under a 0 cap (``0 + 0 > 0`` is False),
inconsistent with the row cap where ``max_in_flight=0`` refuses all.

The SaturationGate is constructible standalone (no E2E stack), so this
lives as a unit test under ``src/phantom-service/tests/unit/``.

Defender Round 2 resolution: ``max_in_flight_bytes=0`` is special-cased
to refuse every admission (matching ``max_in_flight=0``'s zero-semantics
via ``0 + 1 > 0``). Positive caps keep ``>`` semantics so a body that
exactly fills the cap is still admitted. See
``phantom.workers.saturation.SaturationGate.admit``.
"""

from __future__ import annotations

import pytest
from phantom.workers.saturation import (
    AdmissionGranted,
    AdmissionRefusedSaturation,
    SaturationGate,
)


@pytest.mark.asyncio
async def test_zero_bytes_cap_refuses_every_admission() -> None:
    """``max_in_flight_bytes=0`` refuses zero-byte AND non-zero admissions.

    Tighten the gate's check so an operator who pins the bytes cap to
    zero gets the intuitive behavior — every admission refused.
    """
    gate = SaturationGate(
        max_in_flight=100,
        max_in_flight_bytes=0,
        max_disk_bytes=0,
        large_body_threshold_bytes=0,
        max_large_in_flight=0,
    )
    zero_byte = await gate.admit(declared_bytes=0)
    one_byte = await gate.admit(declared_bytes=1)
    big = await gate.admit(declared_bytes=1_048_576)

    # Adversary expectation: ALL refused.
    assert isinstance(zero_byte, AdmissionRefusedSaturation), (
        "0-byte body admitted; max_in_flight_bytes=0 should refuse everything"
    )
    assert isinstance(one_byte, AdmissionRefusedSaturation)
    assert isinstance(big, AdmissionRefusedSaturation)


@pytest.mark.asyncio
async def test_one_byte_cap_refuses_zero_byte_admission() -> None:
    """``max_in_flight_bytes=1`` still refuses 0-byte admissions when row cap is 0.

    Belt-and-suspenders: a 0-byte body should NEVER be admitted when
    the bytes-cap is at its lowest meaningful setting and the row cap is
    full.
    """
    gate = SaturationGate(
        max_in_flight=1,
        max_in_flight_bytes=1,
        max_disk_bytes=0,
        large_body_threshold_bytes=0,
        max_large_in_flight=0,
    )
    # Fill the single row slot.
    first = await gate.admit(declared_bytes=1)
    assert isinstance(first, AdmissionGranted)
    # Now any further admission (zero or not) should be refused.
    zero_byte = await gate.admit(declared_bytes=0)
    assert isinstance(zero_byte, AdmissionRefusedSaturation)
