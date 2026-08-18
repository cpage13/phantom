"""System probe - read machine facts at startup for smart defaults.

The probe runs once during Settings.from_yaml. The MachineFacts it
produces feed compute_defaults (config/defaults.py); the validator on
Settings fills None-valued fields from those defaults. Operator-
supplied YAML values always win.
"""

from __future__ import annotations

import os
from dataclasses import dataclass

import psutil  # type: ignore[import-untyped]


@dataclass(frozen=True)
class MachineFacts:
    """Snapshot of machine resources at startup.

    Attributes:
        total_ram_bytes: ``psutil.virtual_memory().total`` at probe time.
        free_disk_bytes: ``shutil.disk_usage(data_dir).free`` at probe time.
        cpu_count: ``os.cpu_count() or 1``.
    """

    total_ram_bytes: int
    free_disk_bytes: int
    cpu_count: int


def probe_machine(data_dir: str) -> MachineFacts:
    """Read machine facts for the data_dir's filesystem.

    Args:
        data_dir: The Phantom data directory path. The probe reads disk
            usage for this path's filesystem. If the path does not yet
            exist, the probe walks parents until it finds an existing
            directory (the parent's filesystem is what ``data_dir``
            will live on after ``mkdir`` runs in the composition root).

    Returns:
        A frozen MachineFacts. Reads are atomic from psutil; shutil's
        disk_usage is a single syscall.
    """
    import shutil
    from pathlib import Path

    probe_path = Path(data_dir)
    while not probe_path.exists() and probe_path != probe_path.parent:
        probe_path = probe_path.parent

    return MachineFacts(
        total_ram_bytes=psutil.virtual_memory().total,
        free_disk_bytes=shutil.disk_usage(str(probe_path)).free,
        cpu_count=os.cpu_count() or 1,
    )
