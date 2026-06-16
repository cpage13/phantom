"""The large-body ledger must stay exact across a threshold reload (R9-6).

``saturation.*`` is hot-reloadable BY DESIGN - the ADR-031 push
exception, applied via ``SaturationGate.update_caps``. The gate's
large-body class, however, classifies at BOTH ends against the CURRENT
threshold: ``admit`` counts a body as large against the threshold at
admit time, and ``release`` recomputes ``_is_large(actual_bytes)``
against whatever the threshold is at release time
(``workers/saturation.py``). A reload that changes
``large_body_threshold_bytes`` while bodies are in flight therefore
desynchronizes the class ledger:

* Threshold raised (or the class disabled with 0): a body admitted as
  large is no longer "large" at release, so ``_large_in_flight`` never
  decrements - the counter sticks at N forever and the class cap
  refuses fresh large admissions while NOTHING large is in flight.
* Threshold lowered: a body admitted as small releases as "large",
  decrementing the counter held by some OTHER actually-large row - the
  class over-admits past ``max_large_in_flight``.

``update_caps`` documents that in-flight counters are NOT reset because
"they reflect actual current state" - but the threshold change silently
invalidates the basis the large counter was accumulated under, in both
directions. Invariant #16's ledger-balance discipline (exactly one
charge, exactly one matching release) holds for the row and byte
counters because release amounts equal admit amounts; the large class
is the one counter whose CLASSIFICATION is recomputed rather than
remembered. The knob-matrix contract test cannot see this: its push-row
cases assert the caps landed on the gate, not that in-flight
classification survives the transition.

MED severity, the R8-4 availability class: a routine reload of a
documented-hot knob permanently shrinks (or over-opens) the large-class
capacity until restart; the gauge surfaces nothing because row/byte
totals stay correct.

The test drives the REAL gate alone: admit one large body, push a
raised threshold through the REAL ``update_caps``, release the same
bytes, and require the class counter to return to zero and a fresh
under-threshold admission to be granted. Falsifiability proven both
ways in scratch: today the counter sticks at 1 and the fresh admission
is refused; a gate that remembers each admission's classification
returns to zero and grants it.
"""

from __future__ import annotations

import pytest
from phantom.config.settings import SaturationCfg
from phantom.workers.saturation import AdmissionGranted, SaturationGate

pytestmark = pytest.mark.asyncio

# Boot-time large-class threshold and the one body in flight across the
# reload: 150 bytes is large under the boot threshold of 100.
_BOOT_THRESHOLD_BYTES: int = 100
_IN_FLIGHT_BODY_BYTES: int = 150

# The reloaded threshold: the in-flight body is NOT large under it.
_RELOADED_THRESHOLD_BYTES: int = 400

# A fresh body that IS large under the reloaded threshold (450 >= 400).
# The class is empty in truth at that point, so the gate must grant it.
_FRESH_LARGE_BODY_BYTES: int = 450

# One concurrent large body allowed: the smallest cap that makes a
# stuck counter immediately visible as a refusal.
_MAX_LARGE_IN_FLIGHT: int = 1

# Generous row/byte/disk caps so only the large-class cap can refuse.
_GATE_ROW_CAP: int = 10
_GATE_BYTE_CAP: int = 10_000_000
_GATE_DISK_CAP: int = 10_000_000

_R9_6_REASON: str = (
    "R9-6: SaturationGate.release reclassifies actual_bytes against the "
    "CURRENT large_body_threshold_bytes instead of the classification the "
    "admission was charged under, so a hot reload of the threshold (the "
    "sanctioned ADR-031 push) strands or over-frees _large_in_flight: after "
    "raising the threshold past an in-flight large body, the class counter "
    "sticks at 1 forever and fresh large admissions are refused while "
    "nothing large is in flight - invariant #16 broken for the large class, "
    "invisible to the knob-matrix test which observes only the pushed caps"
)


async def test_large_class_ledger_survives_a_threshold_reload() -> None:
    """Raising the threshold mid-flight must not strand the large counter.

    Attack: admit one body that is large under the boot threshold (the
    class is now full at its cap of one), push a raised threshold
    through the REAL ``update_caps`` (the documented hot-reload path),
    then release the same body. Release reclassifies against the NEW
    threshold, judges the body small, and skips the decrement - the
    class reads full forever. A fresh body that is large under the new
    threshold must be granted (the class is truly empty); today it is
    refused.
    """
    gate = SaturationGate(
        max_in_flight=_GATE_ROW_CAP,
        max_in_flight_bytes=_GATE_BYTE_CAP,
        max_disk_bytes=_GATE_DISK_CAP,
        large_body_threshold_bytes=_BOOT_THRESHOLD_BYTES,
        max_large_in_flight=_MAX_LARGE_IN_FLIGHT,
    )
    admitted = await gate.admit(_IN_FLIGHT_BODY_BYTES)
    assert isinstance(admitted, AdmissionGranted), admitted
    assert gate.large_in_flight == _MAX_LARGE_IN_FLIGHT, (
        "precondition: the body was charged to the large class under the boot threshold"
    )

    await gate.update_caps(
        SaturationCfg(
            max_in_flight=_GATE_ROW_CAP,
            max_in_flight_bytes=_GATE_BYTE_CAP,
            max_disk_bytes=_GATE_DISK_CAP,
            large_body_threshold_bytes=_RELOADED_THRESHOLD_BYTES,
            max_large_in_flight=_MAX_LARGE_IN_FLIGHT,
        )
    )
    await gate.release(_IN_FLIGHT_BODY_BYTES)

    assert gate.in_flight == 0 and gate.in_flight_bytes == 0, (
        "precondition: the row and byte ledgers balanced (release amounts equal admit amounts)"
    )
    assert gate.large_in_flight == 0, (
        "the large-class counter is stranded at "
        f"{gate.large_in_flight} after the threshold reload: release "
        "reclassified the in-flight body against the NEW threshold and "
        "skipped the decrement, so the class cap is permanently consumed by "
        "a row that no longer exists"
    )
    fresh = await gate.admit(_FRESH_LARGE_BODY_BYTES)
    assert isinstance(fresh, AdmissionGranted), (
        "a fresh large body must be granted while nothing large is in "
        f"flight; the gate refused it ({fresh.__class__.__name__}) off the "
        "stranded class counter - large-class capacity is gone until restart"
    )
