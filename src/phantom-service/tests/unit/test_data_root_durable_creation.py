"""Q10: the data root's own directory entry is fsynced at first boot.

F10 made every directory level BELOW the store root durable and deliberately
stopped at the boundary, because the body store's contract begins at its own
root. That left the data root's entry in ITS parent unfsynced by anyone, so a
power cut between the boot mkdir and the first fsync beneath it could leave a
brand-new instance booting into a directory that is not there.

The observable is a power cut and the repo has no power-cut harness, so this
inherits F10's own witness shape: assert the fsync CALLS.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from phantom.app import _ensure_data_root_writable

from phantom import app as app_module


def test_first_boot_fsyncs_the_data_root_into_its_parent(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Creating an absent data root fsyncs every level it created, parent first.

    Objective: ``_ensure_data_root_writable`` creates the directory DURABLY,
    so the entry survives a power cut on the very first boot of a new
    instance. Success is that the recorder saw an fsync of the deepest
    existing ancestor (which now holds the new entry) and of each level below
    it, and that the directory exists afterwards.

    The data root is TWO levels below an existing directory here, because
    ``parents=True`` may create several: ``<storage.data_dir>`` itself can be
    absent on a first boot, and a fix that fsynced only the leaf's parent
    would leave the level above it unlinked. Pre-fix the recorder is empty,
    which is the red. The second half asserts the reverse for an existing
    directory, so the fix cannot become a per-boot fsync.
    """
    boundary = tmp_path
    data_root = boundary / "phantom-data" / "instance-a"
    synced: list[Path] = []

    real_sync = app_module._makedirs_durable.__globals__["_sync_directory"]

    def _recording_sync(path: Path) -> None:
        synced.append(path)
        real_sync(path)

    monkeypatch.setitem(
        app_module._makedirs_durable.__globals__, "_sync_directory", _recording_sync
    )

    _ensure_data_root_writable(data_root)

    assert data_root.is_dir()
    assert boundary in synced, "the parent that now holds the new entry was not fsynced"
    assert data_root.parent in synced, "the intermediate level's own entry was not fsynced"

    # And every boot AFTER the first does no fsync at all, which is the
    # helper's own "leaf existed, nothing was created" arm: the durable
    # creation is a first-boot cost, not a per-boot one.
    synced.clear()
    _ensure_data_root_writable(data_root)
    assert synced == []
