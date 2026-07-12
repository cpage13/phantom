"""Bounded peer-loss falsifiers for the sender fault IPC harness."""

from __future__ import annotations

import asyncio

import pytest

from tests.e2e._harness.sender_fault_ipc import (
    SenderFaultIpcServer,
    SenderFaultPeerLostError,
    SenderFaultPhase,
    SenderFaultReached,
    run_child_handshake,
)

pytestmark = [pytest.mark.asyncio, pytest.mark.e2e]


async def test_child_raises_when_parent_closes_before_release() -> None:
    """Child read observes EOF within deadline instead of retaining a thread."""
    ipc = await SenderFaultIpcServer.start(timeout_seconds=1.0)
    child = asyncio.create_task(
        asyncio.to_thread(
            run_child_handshake,
            ipc.endpoint,
            SenderFaultReached(phase=SenderFaultPhase.PRE_CLAIM, chain_id=None),
            timeout_seconds=1.0,
        )
    )
    try:
        reached = await ipc.wait_reached()
        assert reached.phase is SenderFaultPhase.PRE_CLAIM
        await ipc.close()
        with pytest.raises(SenderFaultPeerLostError):
            await asyncio.wait_for(child, timeout=1.0)
    finally:
        if not child.done():
            child.cancel()
        await ipc.close()


async def test_parent_release_raises_when_child_closes_after_reached() -> None:
    """Parent acknowledgement wait observes peer loss instead of hanging cleanup."""
    ipc = await SenderFaultIpcServer.start(timeout_seconds=1.0)
    reader, writer = await asyncio.open_connection("127.0.0.1", int(ipc.endpoint.rsplit(":", 1)[1]))
    del reader
    try:
        writer.write(b"REACHED post_claim 00000000-0000-0000-0000-000000000001\n")
        await writer.drain()
        reached = await ipc.wait_reached()
        assert reached.phase is SenderFaultPhase.POST_CLAIM
        writer.close()
        await writer.wait_closed()
        with pytest.raises(SenderFaultPeerLostError):
            await ipc.release()
    finally:
        await ipc.close()
