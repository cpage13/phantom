"""A burst of concurrent mid-body aborts exhausts no slot pool (R9-CR3).

Aggressor R9, vector CR (connection reset). GREEN regression PIN: the
headline connection-reset scenario. A clean pass is the GOOD result.

THE PROPERTY (R9-CR3, holds): many clients each open a connection, send valid
headers + a partial body, and ABORT mid-upload -- all overlapping. To make any
per-abort slot leak maximally OBSERVABLE the gate is deliberately CONSTRAINED to
``max_in_flight = ABORT_BURST // 2``: if even ONE aborted upload leaked a slot,
a burst of ``ABORT_BURST`` aborts would drive ``in_flight`` to (and past) the
cap and the post-burst clean upload would be REFUSED with a saturation error. A
clean pass under a tight cap is therefore strong evidence the receive-path abort
takes no slot at all (it lands before ``gate.admit``, which only runs after the
full body is buffered).

A future change that reserved capacity at header time (before the full read)
would leak a slot per abort and turn this RED on the post-burst clean-upload
assertion.

DETERMINISM: POLL the live gate ``in_flight`` + on-disk census + ``.tmp`` glob
until they settle to baseline (never a fixed wall-clock sleep that races the
disconnect-cleanup path), THEN assert a fresh clean upload still admits.
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

pytestmark = [pytest.mark.asyncio, pytest.mark.e2e]

_ABORT_BURST = 40
_DECLARED_LEN = 2 * 1024 * 1024
_SENT_LEN = 32 * 1024
_BODY_BYTES = 4 * 1024
# A leak DoS would be observable as in_flight stuck above baseline; budget is
# generous so even a slow timeout-based cleanup would be seen settling.
_SETTLE_TIMEOUT_SECONDS = 60.0
_SETTLE_POLL_SECONDS = 0.25


def _overrides(max_in_flight: int) -> dict:
    """Hybrid mode; gate TIGHTLY capped so any per-abort slot leak refuses later."""
    return {
        "storage": {"body_store": {"mode": "hybrid"}},
        "saturation": {
            "max_in_flight": max_in_flight,
            "max_in_flight_bytes": 256 * 1024 * 1024,
        },
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


async def _abort_one(bind_port: int) -> None:
    """One partial-then-abort upload over a raw socket."""
    try:
        _reader, writer = await asyncio.open_connection(HOST, bind_port)
    except OSError:
        # A connect failure for one aborter is acceptable under a burst.
        return
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
    writer.write(b'{"steps":[' + b" " * (_SENT_LEN - 10))
    with contextlib.suppress(ConnectionResetError, BrokenPipeError, OSError):
        await writer.drain()
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
            f"tmp_files={len(tmp)}"
        )
        if in_flight <= baseline_in_flight and total_rows <= baseline_rows and not tmp:
            return True, last
        await asyncio.sleep(_SETTLE_POLL_SECONDS)  # pre-commit-allow: sleep
    return False, last


async def test_abort_burst_leaks_no_slots_pool_intact(tmp_path: Path) -> None:
    """A burst of concurrent aborts against a tight cap leaves the slot pool intact.

    Pins R9-CR3: the receive-path abort lands before ``gate.admit``, so aborts
    take no saturation slot and cannot exhaust the pool. A future
    reserve-at-header-time refactor would leak a slot per abort and turn this RED
    on the post-burst clean-upload assertion.
    """
    from phantom_client import PhantomClient

    max_in_flight = max(2, _ABORT_BURST // 2)
    emu = await boot_emulator()
    data_dir = tmp_path / "phantom-data"
    data_dir.mkdir()
    port = allocate_port()
    cfg = write_phantom_config(
        data_dir=data_dir, bind_port=port, config_overrides=_overrides(max_in_flight)
    )
    p = PhantomSubprocess.make(cfg, port)
    try:
        await p.start()
        bearer = fake_security_token(emu)

        # in_flight is read from GET /v1/admin/status on the single listener
        # (p.url).
        baseline_in_flight = await _read_in_flight(p.url)
        baseline_rows = sum((await count_rows_by_state(data_dir)).values())

        # Fire the concurrent abort burst.
        await asyncio.gather(*(_abort_one(p.bind_port) for _ in range(_ABORT_BURST)))

        # The process must not have died under the burst.
        assert p._proc is not None and p._proc.poll() is None, (  # type: ignore[attr-defined]
            "phantom subprocess EXITED after a concurrent mid-body abort burst. "
            f"Log tail:\n{p._read_log_tail(60)}"  # type: ignore[attr-defined]
        )

        settled, detail = await _await_settled(
            p.url,
            data_dir,
            baseline_in_flight=baseline_in_flight,
            baseline_rows=baseline_rows,
        )
        assert settled, (
            f"after a burst of {_ABORT_BURST} aborts the system did not return to "
            f"baseline within {_SETTLE_TIMEOUT_SECONDS:.0f}s -- {detail}. The saturation "
            "slot count is stuck above baseline (per-abort slot leak = slow DoS toward "
            "refuse-all), and/or .tmp orphans / bogus committed rows accumulated. "
            f"Log tail:\n{p._read_log_tail(40)}"  # type: ignore[attr-defined]
        )

        # The decisive check: a fresh clean upload must admit. If the burst leaked
        # slots into the tight cap, this is refused with a saturation error.
        async with PhantomClient(p.url) as c:
            await submit_one(
                c,
                emulator_url=emu.url,
                bearer=bearer,
                body=b"y" * _BODY_BYTES,
                chain_id=uuid4(),
                file_prefix="cr3-after",
            )
    finally:
        p.terminate()
        await emu.stop()
