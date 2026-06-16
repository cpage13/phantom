"""Unit tests for phantom.config.probe."""

from __future__ import annotations

from pathlib import Path

import pytest
from phantom.config.probe import MachineFacts, probe_machine


def test_probe_returns_machine_facts(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """probe_machine assembles MachineFacts from psutil + shutil + os.cpu_count."""
    import shutil
    from types import SimpleNamespace

    import phantom.config.probe as probe_module

    monkeypatch.setattr(
        probe_module.psutil,
        "virtual_memory",
        lambda: SimpleNamespace(total=8 * 1024 * 1024 * 1024),
    )
    monkeypatch.setattr(
        shutil,
        "disk_usage",
        lambda path: SimpleNamespace(total=0, used=0, free=200 * 1024 * 1024 * 1024),
    )
    monkeypatch.setattr(probe_module.os, "cpu_count", lambda: 6)

    facts = probe_machine(str(tmp_path))
    assert isinstance(facts, MachineFacts)
    assert facts.total_ram_bytes == 8 * 1024 * 1024 * 1024
    assert facts.free_disk_bytes == 200 * 1024 * 1024 * 1024
    assert facts.cpu_count == 6


def test_probe_handles_none_cpu_count(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When os.cpu_count() returns None, MachineFacts.cpu_count is 1."""
    import shutil
    from types import SimpleNamespace

    import phantom.config.probe as probe_module

    monkeypatch.setattr(
        probe_module.psutil,
        "virtual_memory",
        lambda: SimpleNamespace(total=1024 * 1024 * 1024),
    )
    monkeypatch.setattr(
        shutil,
        "disk_usage",
        lambda path: SimpleNamespace(total=0, used=0, free=10 * 1024 * 1024 * 1024),
    )
    monkeypatch.setattr(probe_module.os, "cpu_count", lambda: None)

    facts = probe_machine(str(tmp_path))
    assert facts.cpu_count == 1
