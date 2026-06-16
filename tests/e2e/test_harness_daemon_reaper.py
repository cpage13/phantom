"""Task 0.2 acceptance: the session daemon reaper leaves no leaked phantom.

The prior cycle's subprocess harness leaked orphaned ``python -m phantom``
daemons across test sessions whenever a test failed (or returned) before its
own teardown ran. The fix is a session-scoped registry in
``tests/e2e/_harness/subprocess_harness.py``: every spawn site registers its
``Popen`` at spawn time and the ``tests/e2e/conftest.py`` session finalizer
calls ``DAEMON_REGISTRY.reap_all()`` after the last test.

This test proves the reap path end to end with a REAL daemon: it spawns
``python -m phantom`` through the harness, deliberately performs NO teardown
of its own (the leak scenario), then drives the exact call the session
finalizer makes and asserts via a registry-based scan plus an OS-level
signal-0 probe that the daemon is genuinely gone and the reap was both
recorded and logged. The daemon needs no live upstream: phantom is a
buffering proxy and boots healthy with the route targets unreachable.

Falsifier: drop the ``DAEMON_REGISTRY.register(...)`` call from
``PhantomSubprocess.start()`` (or make ``reap_all`` a no-op) and this test
goes RED at the registry scan / liveness assertions.
"""

from __future__ import annotations

import logging
import os
from pathlib import Path

import pytest

from tests.e2e._harness.subprocess_harness import (
    DAEMON_REGISTRY,
    PhantomSubprocess,
    allocate_port,
    write_phantom_config,
)

pytestmark = [pytest.mark.asyncio, pytest.mark.e2e]


async def test_skipped_teardown_daemon_is_reaped(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A daemon whose test skips teardown does not survive the reaper.

    Mirrors the leak scenario exactly: spawn, never call ``terminate()``,
    then run the session finalizer's reap call and scan the registry.
    """
    port = allocate_port()
    config_path = write_phantom_config(data_dir=tmp_path / "phantom-data", bind_port=port)
    daemon = PhantomSubprocess.make(config_path, port)
    await daemon.start()

    pid = daemon.pid
    assert pid is not None, "daemon must report a pid after start()"

    # The spawn registered itself: the registry's live scan sees the pid.
    assert pid in {live_pid for live_pid, _ in DAEMON_REGISTRY.live()}, (
        "spawned daemon must be tracked by the session registry at spawn time"
    )

    # Deliberately NO daemon.terminate() / sigkill(): this is the leak.
    with caplog.at_level(logging.WARNING, logger="e2e.subprocess_harness"):
        reaped = DAEMON_REGISTRY.reap_all()

    # The reap is recorded and attributable.
    assert pid in {r.pid for r in reaped}, "reap_all must report the leaked daemon it killed"
    assert f"pid={pid}" in caplog.text, "the reaper must log what it reaped"

    # Registry-based scan: nothing tracked is left alive.
    assert DAEMON_REGISTRY.live() == [], "no tracked daemon may survive the reap"

    # OS-level proof: signal 0 delivery fails because the process is gone
    # (reap_all already wait()ed the child, so the pid cannot be a zombie).
    with pytest.raises(ProcessLookupError):
        os.kill(pid, 0)

    # Idempotent: a second sweep (the real session finalizer running later
    # in this same session) finds nothing left to do.
    assert DAEMON_REGISTRY.reap_all() == [], "a second reap sweep must be a no-op"
