"""Bounded loopback IPC for deterministic sender fault reached/release control."""

from __future__ import annotations

import asyncio
import socket
import time
from contextlib import suppress
from dataclasses import dataclass
from enum import StrEnum
from uuid import UUID

HOST = "127.0.0.1"
IPC_ENDPOINT_ENV = "E2E_SENDER_IPC_ENDPOINT"
FAULT_PHASE_ENV = "E2E_SENDER_FAULT_PHASE"
FAULT_MESSAGE_PREFIX = "phantom-e2e-unknown-sender-fault"
DEFAULT_IPC_TIMEOUT_SECONDS = 10.0
_MAX_MESSAGE_BYTES = 256


class SenderFaultPhase(StrEnum):
    """Supported child-launcher modes."""

    PRE_CLAIM = "pre_claim"
    POST_CLAIM = "post_claim"
    CONTROL = "control"


class SenderFaultIpcError(RuntimeError):
    """Base failure for a bounded sender-fault IPC operation."""


class SenderFaultPeerLostError(SenderFaultIpcError):
    """The IPC peer closed before completing reached/release."""


@dataclass(frozen=True)
class SenderFaultReached:
    """Typed child notification that the selected fault boundary was reached."""

    phase: SenderFaultPhase
    chain_id: UUID | None


def _remaining(deadline: float) -> float:
    """Return positive time remaining on a child monotonic deadline."""
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise TimeoutError("sender fault IPC deadline expired")
    return remaining


def _recv_line(peer: socket.socket, deadline: float) -> bytes:
    """Receive one bounded newline-terminated message from ``peer``."""
    message = bytearray()
    while not message.endswith(b"\n"):
        peer.settimeout(_remaining(deadline))
        chunk = peer.recv(1)
        if not chunk:
            raise SenderFaultPeerLostError("sender fault IPC peer closed before release")
        message.extend(chunk)
        if len(message) > _MAX_MESSAGE_BYTES:
            raise SenderFaultIpcError("sender fault IPC message exceeded size limit")
    return bytes(message)


def run_child_handshake(
    endpoint: str,
    reached: SenderFaultReached,
    *,
    timeout_seconds: float = DEFAULT_IPC_TIMEOUT_SECONDS,
) -> None:
    """Connect, report reached, wait for release, and acknowledge it."""
    host, separator, port_text = endpoint.rpartition(":")
    if not separator or host != HOST:
        raise SenderFaultIpcError("sender fault IPC endpoint must be loopback host:port")
    try:
        port = int(port_text)
    except ValueError as exc:
        raise SenderFaultIpcError("sender fault IPC endpoint has an invalid port") from exc

    chain_id = "-" if reached.chain_id is None else str(reached.chain_id)
    payload = f"REACHED {reached.phase.value} {chain_id}\n".encode()
    deadline = time.monotonic() + timeout_seconds
    try:
        with socket.create_connection((host, port), timeout=_remaining(deadline)) as peer:
            peer.settimeout(_remaining(deadline))
            peer.sendall(payload)
            release = _recv_line(peer, deadline)
            if release != b"RELEASE\n":
                raise SenderFaultIpcError("sender fault IPC received an invalid release")
            peer.settimeout(_remaining(deadline))
            peer.sendall(b"RELEASED\n")
    except (OSError, TimeoutError) as exc:
        if isinstance(exc, SenderFaultIpcError):
            raise
        raise SenderFaultIpcError("sender fault IPC child handshake failed") from exc


class SenderFaultIpcServer:
    """One-child bounded loopback server for the reached/release protocol."""

    def __init__(
        self,
        server: asyncio.AbstractServer,
        connection: asyncio.Future[tuple[asyncio.StreamReader, asyncio.StreamWriter]],
        port: int,
        timeout_seconds: float,
    ) -> None:
        """Bind server internals; construct with :meth:`start`."""
        self._server = server
        self._connection = connection
        self._port = port
        self._timeout_seconds = timeout_seconds
        self._peer: tuple[asyncio.StreamReader, asyncio.StreamWriter] | None = None
        self._closed = False

    @classmethod
    async def start(
        cls, *, timeout_seconds: float = DEFAULT_IPC_TIMEOUT_SECONDS
    ) -> SenderFaultIpcServer:
        """Bind an ephemeral loopback listener within the startup deadline."""
        loop = asyncio.get_running_loop()
        connection: asyncio.Future[tuple[asyncio.StreamReader, asyncio.StreamWriter]] = (
            loop.create_future()
        )

        async def _accept(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
            """Accept exactly one child and close any unexpected extra peer."""
            if connection.done():
                writer.close()
                with suppress(ConnectionError, TimeoutError):
                    await asyncio.wait_for(writer.wait_closed(), timeout=timeout_seconds)
                return
            connection.set_result((reader, writer))

        server = await asyncio.wait_for(
            asyncio.start_server(_accept, HOST, 0), timeout=timeout_seconds
        )
        sockets = server.sockets
        if not sockets:
            server.close()
            await asyncio.wait_for(server.wait_closed(), timeout=timeout_seconds)
            raise SenderFaultIpcError("sender fault IPC listener has no socket")
        port = int(sockets[0].getsockname()[1])
        return cls(server, connection, port, timeout_seconds)

    @property
    def endpoint(self) -> str:
        """Return the child-safe loopback ``host:port`` endpoint."""
        return f"{HOST}:{self._port}"

    async def wait_reached(self) -> SenderFaultReached:
        """Wait within deadline for one child and its reached message."""
        try:
            self._peer = await asyncio.wait_for(
                asyncio.shield(self._connection), timeout=self._timeout_seconds
            )
            reader, _writer = self._peer
            line = await asyncio.wait_for(reader.readline(), timeout=self._timeout_seconds)
        except TimeoutError as exc:
            raise SenderFaultIpcError("sender fault IPC timed out waiting for reached") from exc
        if not line:
            raise SenderFaultPeerLostError("sender fault IPC child closed before reached")
        if len(line) > _MAX_MESSAGE_BYTES:
            raise SenderFaultIpcError("sender fault IPC reached message exceeded size limit")
        parts = line.decode().strip().split()
        if len(parts) != 3 or parts[0] != "REACHED":
            raise SenderFaultIpcError("sender fault IPC received an invalid reached message")
        try:
            phase = SenderFaultPhase(parts[1])
            chain_id = None if parts[2] == "-" else UUID(parts[2])
        except ValueError as exc:
            raise SenderFaultIpcError("sender fault IPC reached fields are invalid") from exc
        return SenderFaultReached(phase=phase, chain_id=chain_id)

    async def release(self) -> None:
        """Release the reached child and require its bounded acknowledgement."""
        if self._peer is None:
            raise SenderFaultIpcError("sender fault IPC release called before reached")
        reader, writer = self._peer
        try:
            writer.write(b"RELEASE\n")
            await asyncio.wait_for(writer.drain(), timeout=self._timeout_seconds)
            acknowledgement = await asyncio.wait_for(
                reader.readline(), timeout=self._timeout_seconds
            )
        except (ConnectionError, TimeoutError) as exc:
            raise SenderFaultPeerLostError(
                "sender fault IPC child disappeared before release acknowledgement"
            ) from exc
        if not acknowledgement:
            raise SenderFaultPeerLostError(
                "sender fault IPC child closed before release acknowledgement"
            )
        if acknowledgement != b"RELEASED\n":
            raise SenderFaultIpcError("sender fault IPC received an invalid acknowledgement")

    async def close(self) -> None:
        """Bound listener and peer cleanup independently of protocol success."""
        if self._closed:
            return
        self._closed = True
        self._server.close()
        peer = self._peer
        if peer is None and self._connection.done() and not self._connection.cancelled():
            peer = self._connection.result()
        if peer is not None:
            _reader, writer = peer
            writer.close()
            with suppress(ConnectionError, TimeoutError):
                await asyncio.wait_for(writer.wait_closed(), timeout=self._timeout_seconds)
        await asyncio.wait_for(self._server.wait_closed(), timeout=self._timeout_seconds)
