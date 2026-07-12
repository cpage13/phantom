"""Narrow sender claim-boundary exception classification tests.

The real-process E2Es prove unknown pre- and post-claim faults crash and
recover. These small tests pin the one exception that deliberately remains
recoverable at the polling boundary: classified SQLite lock contention.
"""

from __future__ import annotations

import asyncio
import sqlite3
from unittest.mock import MagicMock

import pytest
from phantom.instances.context import InstanceContext
from phantom.storage.interface import UploadStore
from phantom.workers.sender import Sender


async def test_transient_claim_lock_is_retried() -> None:
    """A classified SQLite lock retries instead of killing supervision."""
    stop_event = asyncio.Event()
    calls = 0

    async def _claim_due(*_args: object, **_kwargs: object) -> list[object]:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise sqlite3.OperationalError("database is locked")
        stop_event.set()
        return []

    store = MagicMock(spec=UploadStore)
    store.claim_due.side_effect = _claim_due
    instance = MagicMock(spec=InstanceContext)
    instance.store = store
    sender = Sender(instance=instance, worker_count=1, poll_interval_ms=1)

    await sender._worker_loop(0, stop_event)

    assert calls == 2


async def test_non_transient_claim_operational_error_escapes() -> None:
    """A schema-class OperationalError is fatal rather than infinitely retried."""
    store = MagicMock(spec=UploadStore)
    store.claim_due.side_effect = sqlite3.OperationalError("no such table: uploads")
    instance = MagicMock(spec=InstanceContext)
    instance.store = store
    sender = Sender(instance=instance, worker_count=1, poll_interval_ms=1)

    with pytest.raises(sqlite3.OperationalError, match="no such table"):
        await sender._worker_loop(0, asyncio.Event())
