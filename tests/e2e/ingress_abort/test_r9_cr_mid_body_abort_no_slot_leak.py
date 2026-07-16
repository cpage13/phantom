"""Mid-body client abort leaks no slot, orphans no body (R9-CR1).

Aggressor R9, vector CR (connection reset). GREEN regression PIN: a clean
pass is the GOOD result; pinning it catches a future refactor that breaks the
ingress ordering invariant.

THE PROPERTY (R9-CR1, holds): a client declares a large ``Content-Length``
(under the ``max_buffered_bytes`` cap so the precheck passes and the request
enters the body-read path), sends only a fraction of the body, then drops the
TCP connection while Phantom is mid-receive (Starlette ``request.stream()``).
Phantom buffers the WHOLE body into RAM BEFORE it calls ``gate.admit`` or
``body_store.put`` -- so the abort lands BEFORE any saturation slot is claimed
or any body file is staged. Correct behavior: the disconnect is cleanly aborted,
``in_flight`` returns to baseline (no leaked slot), no ``.tmp`` body orphan
remains, no bogus committed row is admitted for the partial upload, and a fresh
clean upload still admits afterward.

A future change that claimed the slot at header time (before the full read)
would leak a slot per abort -- this pin would then go RED on the in_flight /
clean-upload assertions.

DETERMINISM: POLL the live gate ``in_flight`` + on-disk census + ``.tmp`` glob
until they settle to baseline (never a fixed wall-clock sleep that races the
disconnect-cleanup path).
"""

from __future__ import annotations

import asyncio
import contextlib
import time
from pathlib import Path
from uuid import uuid4

import httpx
import pytest

from tests.e2e._harness.subprocess_harness import (
    HOST,
    PhantomSubprocess,
    allocate_port,
    boot_emulator,
    count_rows_by_state,
    fake_security_token,
    instance_dir,
    submit_one,
    write_phantom_config,
)

pytestmark = [pytest.mark.conformance, pytest.mark.asyncio, pytest.mark.e2e]

# Declared Content-Length: well under the 2 GiB max_buffered_bytes cap, so the
# H2 precheck passes and the request enters the body-read path.
_DECLARED_LEN = 4 * 1024 * 1024
# Bytes actually sent before the socket is slammed shut (a tiny fraction).
_SENT_LEN = 64 * 1024
_BODY_BYTES = 4 * 1024
# Generous so a *hang* (slow timeout) is observable as a slot that never returns
# to baseline within the window, rather than a flaky race.
_SETTLE_TIMEOUT_SECONDS = 45.0
_SETTLE_POLL_SECONDS = 0.25


def _overrides() -> dict:
    """Hybrid (default) mode, generous caps so the gate never refuses."""
    return {
        "storage": {"body_store": {"mode": "hybrid"}},
        "saturation": {"max_in_flight": 64, "max_in_flight_bytes": 256 * 1024 * 1024},
        "retry": {"worker_count": 2, "poll_interval_ms": 50},
        "retention": {"reaper_interval_seconds": 3600},
    }


async def _read_in_flight(phantom_url: str) -> int:
    """Sum per-instance live gate ``in_flight`` from GET /v1/admin/status."""
    async with httpx.AsyncClient(timeout=5.0) as client:
        r = await client.get(f"{phantom_url}/v1/admin/status")
        r.raise_for_status()
        data = r.json()
    return sum(int(inst["in_flight"]) for inst in data.get("instances", []))


async def _abort_mid_body(bind_port: int) -> None:
    """Send valid headers + ``Content-Length``, then a partial body, then close."""
    _reader, writer = await asyncio.open_connection(HOST, bind_port)
    request_head = (
        f"POST /v1/send HTTP/1.1\r\n"
        f"Host: {HOST}:{bind_port}\r\n"
        f"Content-Type: application/json\r\n"
        f"Content-Length: {_DECLARED_LEN}\r\n"
        f"X-Phantom-Uid: 00000000-0000-0000-0000-000000000001\r\n"
        f"Connection: close\r\n"
        f"\r\n"
    ).encode("ascii")
    writer.write(request_head)
    # Partial body -- a tiny fraction of the declared length, valid JSON start.
    writer.write(b'{"steps":[' + b" " * (_SENT_LEN - 10))
    await writer.drain()
    # Slam the connection shut mid-body: no terminating bytes, no FIN-after-all-N.
    writer.close()
    with contextlib.suppress(ConnectionResetError, BrokenPipeError, OSError):
        await writer.wait_closed()


def _tmp_files(data_dir: Path) -> list[Path]:
    """Any staged files under the instance body_store ``.tmp/`` dir."""
    tmp_dir = instance_dir(data_dir) / "body_store" / ".tmp"
    if not tmp_dir.exists():
        return []
    return [p for p in tmp_dir.iterdir() if p.is_file()]


async def _await_settled(
    phantom_url: str,
    data_dir: Path,
    *,
    baseline_in_flight: int,
    baseline_rows: int,
) -> tuple[bool, str]:
    """Poll until in_flight + committed rows + ``.tmp`` glob return to baseline."""
    deadline = time.monotonic() + _SETTLE_TIMEOUT_SECONDS
    last = "no observation"
    while time.monotonic() < deadline:
        in_flight = await _read_in_flight(phantom_url)
        census = await count_rows_by_state(data_dir)
        total_rows = sum(census.values())
        tmp = _tmp_files(data_dir)
        last = (
            f"in_flight={in_flight} (baseline {baseline_in_flight}), "
            f"rows={total_rows} (baseline {baseline_rows}) census={census}, "
            f"tmp_files={[p.name for p in tmp]}"
        )
        if in_flight <= baseline_in_flight and total_rows <= baseline_rows and not tmp:
            return True, last
        await asyncio.sleep(_SETTLE_POLL_SECONDS)  # pre-commit-allow: sleep
    return False, last


async def test_mid_body_abort_leaks_no_slot_no_orphan(tmp_path: Path) -> None:
    """A single mid-body abort cleanly aborts: baseline restored, service serves.

    Pins R9-CR1: the body is buffered before ``gate.admit`` / ``body_store.put``,
    so a disconnect mid-receive takes no slot and stages no body. A future
    claim-the-slot-first refactor would leak a slot and turn this RED.
    """
    from phantom_client import PhantomClient

    emu = await boot_emulator()
    data_dir = tmp_path / "phantom-data"
    data_dir.mkdir()
    port = allocate_port()
    cfg = write_phantom_config(data_dir=data_dir, bind_port=port, config_overrides=_overrides())
    p = PhantomSubprocess.make(cfg, port)
    try:
        await p.start()
        bearer = fake_security_token(emu)

        # Baseline: a clean upload first (DB + schema + a known row count), then
        # snapshot in_flight (0 once it commits/queues).
        async with PhantomClient(p.url) as c:
            await submit_one(
                c,
                emulator_url=emu.url,
                bearer=bearer,
                body=b"x" * _BODY_BYTES,
                chain_id=uuid4(),
                file_prefix="cr1-warmup",
            )
        # in_flight is read from GET /v1/admin/status on the single listener
        # (p.url).
        baseline_in_flight = await _read_in_flight(p.url)
        baseline_rows = sum((await count_rows_by_state(data_dir)).values())

        # The abort.
        await _abort_mid_body(p.bind_port)

        # The process must not have died from the client disconnect.
        assert p._proc is not None and p._proc.poll() is None, (  # type: ignore[attr-defined]
            "phantom subprocess EXITED after a mid-body client abort. "
            f"Log tail:\n{p._read_log_tail(60)}"  # type: ignore[attr-defined]
        )

        settled, detail = await _await_settled(
            p.url,
            data_dir,
            baseline_in_flight=baseline_in_flight,
            baseline_rows=baseline_rows,
        )
        assert settled, (
            "after a mid-body abort the system did not return to baseline within "
            f"{_SETTLE_TIMEOUT_SECONDS:.0f}s -- {detail}. A leaked saturation slot "
            "(in_flight stuck above baseline = slow capacity DoS), an orphaned .tmp "
            "body, or a bogus committed row for the aborted upload. Log tail:\n"
            f"{p._read_log_tail(40)}"  # type: ignore[attr-defined]
        )

        # The service still serves a fresh clean upload (no wedge / slot exhaustion).
        async with PhantomClient(p.url) as c:
            await submit_one(
                c,
                emulator_url=emu.url,
                bearer=bearer,
                body=b"y" * _BODY_BYTES,
                chain_id=uuid4(),
                file_prefix="cr1-after",
            )
    finally:
        p.terminate()
        await emu.stop()
