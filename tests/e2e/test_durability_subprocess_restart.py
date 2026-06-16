"""Deployment durability: real-process boot + container-replacement survival (§ 5.D).

Plan § 5.2 Part 5.D, the deployment-durability bundle. The Docker E2E mode is a stub
(``_boot_docker`` raises ``NotImplementedError``), so per F-11 a literal
container-replacement-on-a-named-volume test is approximated by the subprocess harness's
fresh-process-on-the-same-``data_dir`` restart - which is exactly the durability contract:
the bytes live on the volume, not in the process.

* :func:`test_subprocess_boots_with_regenerated_config_and_answers_ready_and_health`
  - the deployment shape: ``python -m phantom -c <config>`` boots from a written config
    and answers ``GET /v1/readyz`` AND ``GET /v1/healthz`` (the admin endpoints
    the deploy compose exposes on its admin port). Both report a healthy, ready,
    non-degraded service.

* :func:`test_buffer_survives_container_replacement_on_same_data_dir`
  - write a buffer (upstream down so the rows stay buffered), GRACEFULLY stop the process
    (a clean container replacement, NOT a crash - the SIGKILL crash path is covered by the
    crash-recovery suite), boot a FRESH process on the SAME ``data_dir``, and confirm the
    buffered rows survive the swap and DELIVER once the upstream recovers. ``all_disk`` +
    persist-immediately pins the bytes to the volume so survival is deterministic.

* :func:`test_stale_config_fails_subprocess_boot_loudly`
  - the deployment-level half of the § 5.C config-swap regression: a deployed image started
    with a STALE config (a key removed in Phase 1) must fail the boot LOUDLY - the process
    exits non-zero and never answers health, so the harness sees a hard failure rather than a
    silently half-configured service. (§ 5.C's ``test_chaos_config_swap_fail_fast`` pins the
    in-process ``load_settings`` validator; this pins the real ``python -m phantom`` boot.)

Public e2e-light lane (§ 5.0): generic ``submit`` shapes.
"""

from __future__ import annotations

import asyncio
import secrets
import time
from pathlib import Path
from uuid import UUID, uuid4

import pytest
from phantom_client import PhantomClient
from phantom_emulator.failure.injection import FailurePolicy, FailureScope

from tests.e2e._harness.subprocess_harness import (
    EmulatorHandle,
    PhantomSubprocess,
    allocate_port,
    boot_emulator,
    count_rows_by_state,
    fake_security_token,
    submit_one,
    write_phantom_config,
)

pytestmark = pytest.mark.e2e

_BODY_BYTES = 2 * 1024
_PRECONDITION_TIMEOUT_SECONDS = 60.0
_PRECONDITION_POLL_SECONDS = 0.2
_DELIVERY_TIMEOUT_SECONDS = 60.0

# Block the upstream so an admitted body stays buffered (retrying) across the
# container replacement; lifting it lets the surviving rows deliver.
_BLOCK_UPSTREAM = FailurePolicy(scope=FailureScope.GLOBAL, error_rate_5xx=1.0)  # type: ignore[call-arg]

# Many short retry intervals so a blocked row stays retryable (does not exhaust to the
# terminal ``stored`` state) for the whole window; once the upstream recovers the next
# attempt delivers it. all_disk + a 1-byte persist threshold pins the body to the volume.
_DURABLE_ALL_DISK: dict[str, object] = {
    "retry": {
        "worker_count": 4,
        "poll_interval_ms": 50,
        "default_strategy": {"type": "fixed_intervals", "intervals_seconds": [1] * 600},
    },
    "retention": {"reaper_interval_seconds": 3600},
    "storage": {
        "body_store": {"mode": "all_disk"},
        "persist_trigger": {"body_size_threshold_bytes": 1},
    },
}

# A field removed in Phase 1 (subsumed by body_store.mode); a deployed YAML still carrying
# it is the canonical stale-deployment shape. Mirrors § 5.C's in-process regression key.
_STALE_REMOVED_KEY_OVERRIDE: dict[str, object] = {"storage": {"default_tier": "ram"}}


async def _await_buffered(data_dir: Path, *, minimum: int = 1) -> int:
    """Poll the on-disk census until at least ``minimum`` rows are buffered."""
    deadline = time.monotonic() + _PRECONDITION_TIMEOUT_SECONDS
    total = 0
    while time.monotonic() < deadline:
        total = sum((await count_rows_by_state(data_dir)).values())
        if total >= minimum:
            return total
        await asyncio.sleep(_PRECONDITION_POLL_SECONDS)  # pre-commit-allow: sleep
    raise AssertionError(
        f"precondition not met: only {total} rows buffered after "
        f"{_PRECONDITION_TIMEOUT_SECONDS}s (need >= {minimum})"
    )


async def _await_delivery(emu: EmulatorHandle, *, minimum: int = 1) -> None:
    """Poll the emulator until it has accepted at least ``minimum`` bodies."""
    deadline = time.monotonic() + _DELIVERY_TIMEOUT_SECONDS
    while time.monotonic() < deadline:
        if len(emu.received()) >= minimum:
            return
        await asyncio.sleep(_PRECONDITION_POLL_SECONDS)  # pre-commit-allow: sleep
    raise AssertionError(
        f"upstream never received >= {minimum} bodies within {_DELIVERY_TIMEOUT_SECONDS}s "
        f"(got {len(emu.received())})"
    )


async def _submit_blocked(proc: PhantomSubprocess, emu: EmulatorHandle, chain_id: UUID) -> None:
    """Submit one chain whose upstream is blocked, so the row buffers on the volume."""
    bearer = fake_security_token(emu)
    async with PhantomClient(proc.url) as client:
        await submit_one(
            client,
            emulator_url=emu.url,
            bearer=bearer,
            body=secrets.token_bytes(_BODY_BYTES),
            chain_id=chain_id,
            file_prefix="durability",
        )


async def test_subprocess_boots_with_regenerated_config_and_answers_ready_and_health(
    tmp_path: Path,
) -> None:
    """The deployed image boots from a written config and answers /ready + /health.

    The deployment contract: ``python -m phantom -c <config>`` comes up and the admin
    endpoints the compose exposes (``/v1/readyz``, ``/v1/healthz``) report a
    healthy, ready, non-degraded service.

    Falsifier: break the boot so the admin router never binds -> ``.start()`` times out on
    health -> RED.
    """
    emu = await boot_emulator()
    data_dir = tmp_path / "phantom-data"
    data_dir.mkdir()
    port = allocate_port()
    cfg = write_phantom_config(data_dir=data_dir, bind_port=port)
    proc = PhantomSubprocess.make(cfg, port)
    try:
        await proc.start()  # raises if /v1/healthz never answers
        async with PhantomClient(proc.url) as client:
            ready = await client.get_ready()
            assert ready.ready is True, f"service not ready after boot: {ready!r}"
            health = await client.get_health()
            assert health.status == "ok", f"health not ok: {health!r}"
            assert health.storage == "ok", (
                f"a freshly-booted writable deployment must report storage ok: {health!r}"
            )
    finally:
        proc.terminate()
        await emu.stop()


async def test_buffer_survives_container_replacement_on_same_data_dir(tmp_path: Path) -> None:
    """A buffered upload survives a clean process replacement on the same data_dir.

    Approximates "replace the container on the same named volume" (F-11): write a buffer
    (upstream down), GRACEFULLY stop the process, boot a FRESH process on the SAME data_dir,
    and confirm the buffered row survives AND delivers once the upstream recovers.

    Falsifier: lose the on-disk row/body across the restart -> the post-restart census is
    empty or the upstream never receives the body -> RED.
    """
    emu = await boot_emulator()
    data_dir = tmp_path / "phantom-data"
    data_dir.mkdir()
    procs: list[PhantomSubprocess] = []
    try:
        emu.inject_failure(_BLOCK_UPSTREAM)

        # 1. Boot, buffer one row with the upstream down (the body lands on the volume).
        port1 = allocate_port()
        cfg1 = write_phantom_config(
            data_dir=data_dir, bind_port=port1, config_overrides=_DURABLE_ALL_DISK
        )
        p1 = PhantomSubprocess.make(cfg1, port1)
        await p1.start()
        procs.append(p1)
        chain_id = uuid4()
        await _submit_blocked(p1, emu, chain_id)
        buffered = await _await_buffered(data_dir, minimum=1)

        # 2. Clean container replacement: graceful stop, then a FRESH process on the SAME
        #    data_dir (a new port, like a replacement container getting a new IP).
        p1.terminate()
        port2 = allocate_port()
        cfg2 = write_phantom_config(
            data_dir=data_dir, bind_port=port2, config_overrides=_DURABLE_ALL_DISK
        )
        p2 = PhantomSubprocess.make(cfg2, port2)
        await p2.start()
        procs.append(p2)

        # 3. The buffer survived the swap.
        survived = sum((await count_rows_by_state(data_dir)).values())
        assert survived >= 1, (
            f"container replacement lost the buffer: {buffered} buffered before, "
            f"{survived} after the same-data_dir restart"
        )

        # 4. And it delivers once the upstream recovers - the replacement is fully live.
        emu.clear_failures()
        await _await_delivery(emu, minimum=1)
        async with PhantomClient(p2.url) as client:
            health = await client.get_health()
            assert health.status == "ok"
    finally:
        for proc in procs:
            proc.terminate()
        await emu.stop()


async def test_stale_config_fails_subprocess_boot_loudly(tmp_path: Path) -> None:
    """A deployed image started with a stale config fails the boot LOUDLY (process exits).

    The deployment-level half of the § 5.C config-swap regression: a stale key
    (``storage.default_tier``, removed in Phase 1) must stop ``python -m phantom`` at boot
    rather than booting a half-configured service. The subprocess exits non-zero and never
    answers health, so ``.start()`` raises - the harness sees a hard, visible failure.

    Falsifier: relax the settings models to ``extra="ignore"`` -> the stale key is dropped
    and the process boots healthy -> this RED (no raise).
    """
    emu = await boot_emulator()
    data_dir = tmp_path / "phantom-data"
    data_dir.mkdir()
    port = allocate_port()
    cfg = write_phantom_config(
        data_dir=data_dir, bind_port=port, config_overrides=_STALE_REMOVED_KEY_OVERRIDE
    )
    proc = PhantomSubprocess.make(cfg, port)
    try:
        with pytest.raises(RuntimeError) as excinfo:
            await proc.start()
        # The harness raises RuntimeError when the process exits early (loud, not silent).
        msg = str(excinfo.value).lower()
        assert "exited early" in msg or "did not become healthy" in msg, (
            f"unexpected boot-failure message: {excinfo.value!r}"
        )
    finally:
        proc.terminate()
        await emu.stop()
