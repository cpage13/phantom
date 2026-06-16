"""Adopted EXPLORATORY regression test for aggressor Round-5 finding V5.

Pins the three retry-cadence properties that HOLD on current code (A
budget→terminal, B backoff-held, D crash-survival-of-schedule). The
thundering-herd risk (V5-C — ``fixed_intervals`` had no jitter) is FIXED in R6
by adding an optional ``jitter`` fraction to ``FixedIntervalsStrategy`` (parity
with ``exponential_backoff``); the de-correlation behavior is asserted
deterministically at the unit level in
``src/phantom-service/tests/unit/test_strategies.py``
(``test_fixed_intervals_jitter_within_bounds_and_decorrelates``). A wall-clock
recovery-spread assertion across a subprocess would be flaky, so it is NOT
asserted here (per the aggressor's own guidance). The remaining global-outbound
rate bound is a larger design change documented in findings.md, not in scope.

* ``test_retry_budget_reaches_terminal`` — exponential_backoff max_attempts=4,
  upstream always-503: the row reaches a terminal state (``stored``), it does
  NOT retry forever.
* ``test_backoff_interval_is_held_not_hammered`` — fixed_intervals [2,…],
  always-503: the attempt count over a window respects the interval (the sender
  does not hammer at the poll rate).
* ``test_retry_schedule_survives_sigkill`` — a backlog with future
  next_attempt_at is SIGKILLed and restarted (all_disk → no recovery quarantine);
  recovery PRESERVES next_attempt_at + attempts (no post-restart "retry now"
  storm).

SUBPROCESS tests (real sender + executor + real upstream) — they reuse the
killable-subprocess harness at ``tests/e2e/_harness/subprocess_harness.py``.
"""

from __future__ import annotations

import asyncio
import secrets
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4

import pytest

from tests.e2e._harness.subprocess_harness import (
    PhantomSubprocess,
    allocate_port,
    boot_emulator,
    fake_security_token,
    open_store_readonly,
    submit_one,
    write_phantom_config,
)

pytestmark = [pytest.mark.asyncio, pytest.mark.e2e]

_TERMINAL = {"stored", "failed", "cancelled", "succeeded", "corrupted", "auth_expired"}


async def _poll(
    client: Any, chain_id: UUID, *, want_state: str | None = None, timeout: float = 40.0
) -> Any:
    deadline = asyncio.get_event_loop().time() + timeout
    last = None
    while asyncio.get_event_loop().time() < deadline:
        try:
            d = await client.get_upload(chain_id)
            last = d
            if want_state is None or d.state == want_state:
                return d
        except Exception:
            pass
        await asyncio.sleep(0.2)  # pre-commit-allow: sleep — status poll interval
    return last


async def test_retry_budget_reaches_terminal(tmp_path: Path) -> None:
    """exp backoff max_attempts=4 + always-503 → terminal 'stored' (not forever)."""
    emu = await boot_emulator()
    port = allocate_port()
    cfg = write_phantom_config(
        data_dir=tmp_path / "d",
        bind_port=port,
        config_overrides={
            "storage": {"body_store": {"mode": "hybrid"}},
            "retry": {
                "worker_count": 2,
                "poll_interval_ms": 50,
                "default_strategy": {
                    "type": "exponential_backoff",
                    "base_seconds": 0.2,
                    "factor": 2.0,
                    "cap_seconds": 2.0,
                    "jitter": 0.1,
                    "max_attempts": 4,
                },
            },
        },
    )
    phantom = PhantomSubprocess.make(cfg, port)
    try:
        from phantom_client import PhantomClient
        from phantom_emulator.failure.injection import FailurePolicy, FailureScope

        await phantom.start()
        bearer = fake_security_token(emu)
        emu.inject_failure(FailurePolicy(scope=FailureScope.GLOBAL, error_rate_5xx=1.0))  # type: ignore[call-arg]
        cid = uuid4()
        # Intake and the poll (get_upload, an admin route) both ride the
        # single listener, so one client covers both.
        async with PhantomClient(phantom.url) as client:
            await submit_one(
                client,
                emulator_url=emu.url,
                bearer=bearer,
                body=secrets.token_bytes(512),
                chain_id=cid,
                file_prefix="v5a",
            )
            d = await _poll(client, cid, want_state="stored", timeout=30.0)
        assert d is not None and d.state in _TERMINAL, (
            f"row never reached terminal - retries forever (state={getattr(d, 'state', None)})"
        )
        assert d.state == "stored", f"expected budget-exhausted 'stored', got {d.state}"
    finally:
        phantom.terminate()
        await emu.stop()


async def test_backoff_interval_is_held_not_hammered(tmp_path: Path) -> None:
    """fixed_intervals [2,…] + always-503 → attempts respect the interval, no hammering."""
    emu = await boot_emulator()
    port = allocate_port()
    interval = 2
    cfg = write_phantom_config(
        data_dir=tmp_path / "d",
        bind_port=port,
        config_overrides={
            "storage": {"body_store": {"mode": "hybrid"}},
            "retry": {
                "worker_count": 2,
                "poll_interval_ms": 50,
                "default_strategy": {
                    "type": "fixed_intervals",
                    "intervals_seconds": [interval] * 8,
                },
            },
        },
    )
    phantom = PhantomSubprocess.make(cfg, port)
    try:
        from phantom_client import PhantomClient
        from phantom_emulator.failure.injection import FailurePolicy, FailureScope

        await phantom.start()
        bearer = fake_security_token(emu)
        emu.inject_failure(FailurePolicy(scope=FailureScope.GLOBAL, error_rate_5xx=1.0))  # type: ignore[call-arg]
        cid = uuid4()
        # Intake and get_upload (an admin route) both ride the single
        # listener, so one client covers both.
        async with PhantomClient(phantom.url) as client:
            await submit_one(
                client,
                emulator_url=emu.url,
                bearer=bearer,
                body=secrets.token_bytes(512),
                chain_id=cid,
                file_prefix="v5b",
            )
            window = 9.0
            # Cadence measurement: count retries over a fixed wall-clock window
            # to prove the backoff interval is HELD (not hammered). The window
            # IS the measurement - there is no terminal event to wait on (the
            # upstream is permanently 503).
            await asyncio.sleep(window)  # pre-commit-allow: sleep
            d = await client.get_upload(cid)
            attempts = d.attempts
        ceiling = int(window / interval) + 3
        assert attempts <= ceiling, (
            f"sender HAMMERS — {attempts} attempts in {window:.0f}s exceeds the {interval}s "
            f"backoff (ceiling {ceiling})"
        )
        assert attempts >= 2, f"expected multiple retries in {window:.0f}s, got {attempts}"
    finally:
        phantom.terminate()
        await emu.stop()


async def test_retry_schedule_survives_sigkill(tmp_path: Path) -> None:
    """A backlog with future next_attempt_at survives SIGKILL — no post-restart storm."""
    emu = await boot_emulator()
    data_dir = tmp_path / "d"
    port = allocate_port()
    overrides = {
        "storage": {"body_store": {"mode": "all_disk"}},  # no recovery quarantine
        "saturation": {"max_in_flight": 200, "max_in_flight_bytes": 50_000_000},
        "retry": {
            "worker_count": 4,
            "poll_interval_ms": 50,
            "default_strategy": {
                "type": "fixed_intervals",
                "intervals_seconds": [30, 30, 30, 30],
            },
        },
    }
    cfg = write_phantom_config(data_dir=data_dir, bind_port=port, config_overrides=overrides)
    p1 = PhantomSubprocess.make(cfg, port)
    p2: PhantomSubprocess | None = None
    try:
        from phantom_client import PhantomClient
        from phantom_emulator.failure.injection import FailurePolicy, FailureScope

        await p1.start()
        bearer = fake_security_token(emu)
        emu.inject_failure(FailurePolicy(scope=FailureScope.GLOBAL, error_rate_5xx=1.0))  # type: ignore[call-arg]
        cids = [uuid4() for _ in range(30)]

        async def _one(c: UUID) -> None:
            try:
                async with PhantomClient(p1.url) as client:
                    await submit_one(
                        client,
                        emulator_url=emu.url,
                        bearer=bearer,
                        body=secrets.token_bytes(512),
                        chain_id=c,
                        file_prefix="v5d",
                    )
            except Exception:
                pass

        await asyncio.gather(*(_one(c) for c in cids), return_exceptions=True)
        # Let each row fail once so it gets a future next_attempt_at (≈ now+30s)
        # we can SIGKILL across and verify survives.
        await asyncio.sleep(3.0)  # pre-commit-allow: sleep

        before: dict[str, tuple[str | None, int]] = {}
        store = await open_store_readonly(data_dir)
        try:
            async for row in store.iter_rows():
                before[str(row.chain_id)] = (
                    row.next_attempt_at.isoformat() if row.next_attempt_at else None,
                    row.attempts,
                )
        finally:
            await store.stop()
        scheduled = sum(1 for (na, _) in before.values() if na is not None)
        assert scheduled > 0, (
            "premise: rows should have a future next_attempt_at after failing once"
        )

        p1.sigkill()

        port2 = allocate_port()
        cfg2 = write_phantom_config(data_dir=data_dir, bind_port=port2, config_overrides=overrides)
        p2 = PhantomSubprocess.make(cfg2, port2)
        await p2.start()

        after: dict[str, tuple[str | None, int]] = {}
        store2 = await open_store_readonly(data_dir)
        try:
            async for row in store2.iter_rows():
                after[str(row.chain_id)] = (
                    row.next_attempt_at.isoformat() if row.next_attempt_at else None,
                    row.attempts,
                )
        finally:
            await store2.stop()

        reset = 0
        for cid, (na_b, att_b) in before.items():
            na_a, att_a = after.get(cid, (None, -1))
            # A row's schedule was RESET if its future next_attempt_at was
            # cleared, OR its attempt counter went backwards.
            if (na_b is not None and na_a is None) or att_a < att_b:
                reset += 1
        assert reset == 0, (
            f"{reset}/{scheduled} rows had their retry schedule RESET by recovery "
            "→ post-restart storm risk"
        )
    finally:
        p1.terminate()
        if p2 is not None:
            p2.terminate()
        await emu.stop()
