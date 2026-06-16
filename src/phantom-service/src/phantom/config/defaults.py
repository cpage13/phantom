"""Smart default values derived from MachineFacts.

The Settings validator calls compute_defaults(probe_machine(data_dir))
once at startup. None-valued Settings fields are filled from the
result; operator-supplied YAML values are preserved.
"""

from __future__ import annotations

from dataclasses import dataclass

from phantom.config.probe import MachineFacts

# Ceilings keep the auto-resolved values reasonable on very large
# hardware (a server with 1 TiB of RAM shouldn't auto-resolve to a
# 256 GiB max_in_flight_bytes — operators on big iron tune explicitly).
_MAX_MEMORY_CEILING_BYTES = 8 * 1024 * 1024 * 1024  # 8 GiB
_WORKER_COUNT_CEILING = 8

# Large-body class boundary. Bodies at or above this size count as
# "large" and are admitted under the separate ``max_large_in_flight``
# concurrent cap.
_LARGE_BODY_THRESHOLD_BYTES = 16 * 1024 * 1024  # 16 MiB

# Concurrent large-body cap. Four concurrent ≥16 MiB bodies keeps RAM
# spikes bounded on mid-range hardware (~64 MiB working set above the
# steady-state in-flight floor).
_MAX_LARGE_IN_FLIGHT = 4

# Lower bound on the worker pool size. Even a 1-CPU container gets two
# workers so a single stuck attempt doesn't starve the rest.
_WORKER_COUNT_FLOOR = 2

# Lower bound on the in-flight row cap. SQLite + Python state for a
# row with no body is on the order of a few KiB; this floor prevents
# tiny RAM machines from auto-resolving below the level needed to
# absorb a burst.
_MAX_IN_FLIGHT_FLOOR = 64

# Upper bound on the in-flight row cap. Above this, the SQLite write
# lock starts contending on per-tick claim_due iterations regardless
# of RAM headroom.
_MAX_IN_FLIGHT_CEILING = 1000

# RAM fraction reserved for Phantom. A quarter of total RAM is a
# defensive default — leaves three quarters for the OS, other
# co-tenant processes, codec working memory, and asyncio task frames.
_RAM_FRACTION_NUMERATOR = 1
_RAM_FRACTION_DENOMINATOR = 4

# Fraction of free disk usable by Phantom. 80% leaves headroom for
# OS / log / SD-card-wear buffer.
_DISK_FRACTION_NUMERATOR = 4
_DISK_FRACTION_DENOMINATOR = 5

# In-flight bytes are half of the RAM Phantom claims; the other half
# is working memory (codecs, dict overhead, asyncio task frames).
_IN_FLIGHT_BYTES_FRACTION_NUMERATOR = 1
_IN_FLIGHT_BYTES_FRACTION_DENOMINATOR = 2

# Per-row RAM-cost denominator for the in-flight row cap. The cap
# assumes ~4 MiB of working state per row; on a 1 GiB Phantom RAM
# share that yields ~256 in-flight rows before the byte cap binds.
_PER_ROW_RAM_COST_BYTES = 4 * 1024 * 1024

# Size-aware persist threshold fraction of Phantom's RAM share.
# Bodies at or above 1/8 of the RAM share skip the memory tier
# entirely (size-aware persist on receipt).
_PERSIST_THRESHOLD_FRACTION_NUMERATOR = 1
_PERSIST_THRESHOLD_FRACTION_DENOMINATOR = 8


@dataclass(frozen=True)
class ResolvedDefaults:
    """All settings values the probe can derive at startup.

    Each field is a value the operator may override in YAML. The
    Settings validator consults this when a field is None.
    """

    # Saturation
    max_in_flight: int
    max_in_flight_bytes: int
    max_disk_bytes: int
    large_body_threshold_bytes: int
    max_large_in_flight: int

    # Storage — size-aware persist trigger threshold.
    body_size_threshold_bytes: int

    # Workers
    worker_count: int


def compute_defaults(facts: MachineFacts) -> ResolvedDefaults:
    """Compute smart defaults from machine facts.

    Args:
        facts: The probe output.

    Returns:
        Resolved defaults. Every value is a non-zero positive int.
    """
    # Saturation: 25% of RAM up to 8 GiB. The fraction is bounded so
    # we don't take more than the operator can afford; the ceiling
    # prevents auto-resolution on big iron from over-allocating.
    max_memory_bytes = min(
        (facts.total_ram_bytes * _RAM_FRACTION_NUMERATOR) // _RAM_FRACTION_DENOMINATOR,
        _MAX_MEMORY_CEILING_BYTES,
    )

    # Disk: use 80% of free disk. Leaves headroom for OS + logs.
    max_disk_bytes = (
        facts.free_disk_bytes * _DISK_FRACTION_NUMERATOR
    ) // _DISK_FRACTION_DENOMINATOR

    # In-flight row count: scale with RAM but cap at the ceiling. A row
    # with no body still costs SQLite + Python state.
    max_in_flight = min(
        _MAX_IN_FLIGHT_CEILING,
        max(_MAX_IN_FLIGHT_FLOOR, max_memory_bytes // _PER_ROW_RAM_COST_BYTES),
    )

    # In-flight bytes: half of max_memory_bytes; the other half is
    # working RAM (codecs, dict overhead, asyncio task frames).
    max_in_flight_bytes = (
        max_memory_bytes * _IN_FLIGHT_BYTES_FRACTION_NUMERATOR
    ) // _IN_FLIGHT_BYTES_FRACTION_DENOMINATOR

    # Large-body class: bodies ≥ 16 MiB count as large. Cap concurrent
    # large bodies so a burst doesn't OOM-kill on mid-range hardware.
    large_body_threshold_bytes = _LARGE_BODY_THRESHOLD_BYTES
    max_large_in_flight = _MAX_LARGE_IN_FLIGHT

    # Size-aware persist trigger: bodies at or above this size skip
    # the RAM tier entirely (persist on receipt). Set at one-eighth
    # of max_memory_bytes so an unusual body can't dominate; floored
    # at the large-body threshold so a tiny RAM share never silently
    # disables the threshold.
    body_size_threshold_bytes = max(
        _LARGE_BODY_THRESHOLD_BYTES,
        (max_memory_bytes * _PERSIST_THRESHOLD_FRACTION_NUMERATOR)
        // _PERSIST_THRESHOLD_FRACTION_DENOMINATOR,
    )

    # Worker count: one per CPU, capped at the ceiling and floored at 2.
    worker_count = min(_WORKER_COUNT_CEILING, max(_WORKER_COUNT_FLOOR, facts.cpu_count))

    return ResolvedDefaults(
        max_in_flight=max_in_flight,
        max_in_flight_bytes=max_in_flight_bytes,
        max_disk_bytes=max_disk_bytes,
        large_body_threshold_bytes=large_body_threshold_bytes,
        max_large_in_flight=max_large_in_flight,
        body_size_threshold_bytes=body_size_threshold_bytes,
        worker_count=worker_count,
    )
