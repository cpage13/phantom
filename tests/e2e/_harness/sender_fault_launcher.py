"""Test-only Phantom launcher for deterministic sender fault supervision E2Es.

The launcher patches one concrete sender boundary inside the child process,
performs a bounded loopback reached/release/ack handshake with the parent, and
then raises an unknown exception. It invokes the real ``phantom.__main__`` entry point
after installing the patch, so configuration, uvicorn, application lifespan,
TaskGroups, storage, recovery, and delivery are all production code.

Nothing in this module is imported by or exposed from the product packages.
"""

from __future__ import annotations

import asyncio
import os
import sys
from datetime import datetime

from phantom.models.upload import UploadRow
from phantom.storage.interface import UploadStore
from phantom.storage.sqlite_store import SqliteUploadStore
from phantom.workers.sender import Sender

from tests.e2e._harness.sender_fault_ipc import (
    FAULT_MESSAGE_PREFIX,
    FAULT_PHASE_ENV,
    IPC_ENDPOINT_ENV,
    SenderFaultPhase,
    SenderFaultReached,
    run_child_handshake,
)


class InjectedUnknownSenderError(RuntimeError):
    """Unique unknown exception used to distinguish the E2E crash path."""


def _endpoint() -> str:
    """Return the required bounded loopback IPC endpoint."""
    value = os.environ.get(IPC_ENDPOINT_ENV)
    if value is None:
        raise RuntimeError(f"missing required child environment variable {IPC_ENDPOINT_ENV}")
    return value


def _install_pre_claim_fault() -> None:
    """Patch the first ``claim_due`` call to fault before durable claim."""
    original_claim_due = SqliteUploadStore.claim_due
    fired = False

    async def _claim_due(store: SqliteUploadStore, now: datetime, limit: int) -> list[UploadRow]:
        nonlocal fired
        if fired:
            return await original_claim_due(store, now, limit)
        fired = True
        await asyncio.to_thread(
            run_child_handshake,
            _endpoint(),
            SenderFaultReached(phase=SenderFaultPhase.PRE_CLAIM, chain_id=None),
        )
        raise InjectedUnknownSenderError(f"{FAULT_MESSAGE_PREFIX}:pre_claim")

    SqliteUploadStore.claim_due = _claim_due


def _install_post_claim_fault() -> None:
    """Patch the first row drive to fault after ``claim_due`` commits."""
    original_drive_one = Sender._drive_one
    fired = False

    async def _drive_one(sender: Sender, store: UploadStore, row: UploadRow) -> None:
        nonlocal fired
        if fired:
            await original_drive_one(sender, store, row)
            return
        fired = True
        await asyncio.to_thread(
            run_child_handshake,
            _endpoint(),
            SenderFaultReached(phase=SenderFaultPhase.POST_CLAIM, chain_id=row.chain_id),
        )
        raise InjectedUnknownSenderError(f"{FAULT_MESSAGE_PREFIX}:post_claim")

    Sender._drive_one = _drive_one


def main() -> int:
    """Install the selected child-only patch and invoke the production CLI."""
    phase = SenderFaultPhase(os.environ.get(FAULT_PHASE_ENV, SenderFaultPhase.CONTROL.value))
    if phase is SenderFaultPhase.PRE_CLAIM:
        _install_pre_claim_fault()
    elif phase is SenderFaultPhase.POST_CLAIM:
        _install_post_claim_fault()

    from phantom.__main__ import main as phantom_main

    return phantom_main()


if __name__ == "__main__":
    sys.exit(main())
