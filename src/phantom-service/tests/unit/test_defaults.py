"""Unit tests for phantom.config.defaults."""

from __future__ import annotations

from phantom.config.defaults import ResolvedDefaults, compute_defaults
from phantom.config.probe import MachineFacts


def _facts(*, ram_gib: int, disk_gib: int, cpus: int) -> MachineFacts:
    """Helper — build MachineFacts from GiB-scale inputs."""
    gib = 1024 * 1024 * 1024
    return MachineFacts(
        total_ram_bytes=ram_gib * gib,
        free_disk_bytes=disk_gib * gib,
        cpu_count=cpus,
    )


def test_compute_defaults_caps_max_memory() -> None:
    """A host with 64 GiB total RAM resolves to max_memory_bytes <= 8 GiB ceiling.

    The fraction is 25% of total = 16 GiB; the ceiling clips to 8 GiB.
    max_in_flight_bytes is half of max_memory_bytes, so 4 GiB exactly.
    """
    defaults = compute_defaults(_facts(ram_gib=64, disk_gib=100, cpus=8))
    eight_gib = 8 * 1024 * 1024 * 1024
    assert defaults.max_in_flight_bytes == eight_gib // 2


def test_compute_defaults_scales_with_disk() -> None:
    """A host with 100 GiB free resolves max_disk_bytes ≈ 80 GiB (80%)."""
    defaults = compute_defaults(_facts(ram_gib=8, disk_gib=100, cpus=4))
    # 80% of 100 GiB = 80 GiB exactly (the fraction is 4/5).
    expected = (100 * 1024 * 1024 * 1024 * 4) // 5
    assert defaults.max_disk_bytes == expected


def test_compute_defaults_worker_count_floor_2() -> None:
    """cpu_count=1 floors worker_count to 2."""
    defaults = compute_defaults(_facts(ram_gib=8, disk_gib=10, cpus=1))
    assert defaults.worker_count == 2


def test_compute_defaults_worker_count_ceiling_8() -> None:
    """cpu_count=32 caps worker_count at 8."""
    defaults = compute_defaults(_facts(ram_gib=8, disk_gib=10, cpus=32))
    assert defaults.worker_count == 8


def test_compute_defaults_returns_resolved_defaults() -> None:
    """compute_defaults returns the ResolvedDefaults dataclass."""
    defaults = compute_defaults(_facts(ram_gib=8, disk_gib=100, cpus=4))
    assert isinstance(defaults, ResolvedDefaults)
    # Every field is a positive int.
    assert defaults.max_in_flight > 0
    assert defaults.max_in_flight_bytes > 0
    assert defaults.max_disk_bytes > 0
    assert defaults.large_body_threshold_bytes > 0
    assert defaults.max_large_in_flight > 0
    assert defaults.body_size_threshold_bytes > 0
    assert defaults.worker_count > 0


def test_compute_defaults_in_flight_count_bounded() -> None:
    """max_in_flight floors at 64 and caps at 1000."""
    # 256 GiB total = 64 GiB max_memory, but capped at 8 GiB ceiling.
    # 8 GiB / 4 MiB = 2048 — clipped to 1000.
    high = compute_defaults(_facts(ram_gib=256, disk_gib=100, cpus=4))
    assert high.max_in_flight == 1000
    # 1 GiB total = 256 MiB max_memory. 256 MiB / 4 MiB = 64 — at the floor.
    low = compute_defaults(_facts(ram_gib=1, disk_gib=10, cpus=2))
    assert low.max_in_flight == 64
