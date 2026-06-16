"""all_ram RAM ceiling refuses cleanly, never OOMs (R9-PM-2).

Aggressor R9, per-mode set. GREEN regression PIN: a clean pass is the GOOD
result.

THE PROPERTY (R9-PM-2, holds): hybrid relieves RAM pressure by evicting bodies
to disk; all_ram has NO disk fallback. The ONLY admitted-byte bound in all_ram
is the saturation ``max_in_flight_bytes`` gate (admission -> 503
``saturation_cap``). When admitted-but-undelivered bodies fill RAM to that bound,
admission MUST refuse GRACEFULLY:

* refuse with a 503-class saturation refusal -- NOT a naked 5xx, NOT an OOM, NOT
  a process crash, NOT unbounded RAM growth;
* ``ram_body_store_bytes`` never materially exceeds the bound (the gate holds the
  line, within a one-in-flight-body tolerance);
* once pressure clears (upstream recovers, bodies drain out of RAM), a fresh
  upload is admitted again (no refuse-forever wedge).

A regression that bypassed the bytes gate (or made the refusal a naked 5xx)
would turn this RED.

DETERMINISM: the upstream is PAUSED (503) so admitted bodies never drain and the
bytes cap actually binds; the burst's own responses are the saturation signal;
recovery POLLS the RAM gauge back below one body before the final clean submit
(never a wall-clock race). Bodies are INCOMPRESSIBLE (random) so the
stored-size-accounting gate actually binds under the base zstd compression.
"""

from __future__ import annotations

import asyncio
import contextlib
import secrets
import time
from pathlib import Path
from uuid import uuid4

import httpx
import pytest

from tests.e2e._harness.subprocess_harness import (
    PhantomSubprocess,
    allocate_port,
    boot_emulator,
    fake_security_token,
    submit_one,
    write_phantom_config,
)

pytestmark = [pytest.mark.asyncio, pytest.mark.e2e]

_BODY_BYTES = 64 * 1024
# The operative RAM ceiling in all_ram == the saturation bytes cap (~3 bodies).
_MAX_IN_FLIGHT_BYTES = 3 * _BODY_BYTES  # 192 KiB
_RAM_CEILING_BYTES = 2 * _BODY_BYTES  # 128 KiB (observational; unenforced in all_ram)
_BURST = 12
# Generous row cap so the BYTES cap is the binding constraint.
_MAX_IN_FLIGHT_ROWS = _BURST * 8
# The gate admits the body that pushes in_flight TO the cap (the `>` check), so a
# single in-flight body's worth of overshoot is acceptable; beyond that is
# unbounded growth.
_RAM_TOLERANCE_BYTES = _MAX_IN_FLIGHT_BYTES + _BODY_BYTES + 4096
_RECOVERY_TIMEOUT_SECONDS = 60.0
_RECOVERY_POLL_SECONDS = 0.5


def _overrides() -> dict:
    """all_ram, TIGHT saturation bytes cap, generous row cap + retry."""
    return {
        "storage": {
            "body_store": {
                "mode": "all_ram",
                "ram_ceiling_bytes": _RAM_CEILING_BYTES,
            }
        },
        "saturation": {
            "max_in_flight": _MAX_IN_FLIGHT_ROWS,
            "max_in_flight_bytes": _MAX_IN_FLIGHT_BYTES,
        },
        "retry": {"worker_count": 4, "poll_interval_ms": 50},
        "retention": {"reaper_interval_seconds": 3600},
    }


async def _ram_bytes(admin_url: str) -> int:
    """Read ``ram_body_store_bytes`` from the ram_pressure observability route."""
    async with httpx.AsyncClient(timeout=5.0) as c:
        r = await c.get(f"{admin_url}/v1/admin/observability/ram_pressure")
        r.raise_for_status()
        return int(r.json()["ram_body_store_bytes"])


def _extract_status(exc: BaseException) -> int | None:
    """Best-effort HTTP status extraction from a client exception."""
    resp = getattr(exc, "response", None)
    code = getattr(resp, "status_code", None)
    if isinstance(code, int):
        return code
    for token in str(exc).replace(",", " ").split():
        if token.isdigit() and len(token) == 3 and token[0] in "45":
            return int(token)
    return None


async def _submit_classified(phantom_url: str, emulator_url: str, bearer: str) -> str:
    """Submit one chain; return ``'ok'`` | ``'http_<code>'`` | ``'exc:<type>'``."""
    from phantom_client import PhantomClient

    try:
        async with PhantomClient(phantom_url) as c:
            await submit_one(
                c,
                emulator_url=emulator_url,
                bearer=bearer,
                body=secrets.token_bytes(_BODY_BYTES),  # incompressible: gate binds
                chain_id=uuid4(),
                file_prefix="pm2",
            )
        return "ok"
    except Exception as exc:
        status = _extract_status(exc)
        return f"http_{status}" if status is not None else f"exc:{type(exc).__name__}"


async def test_allram_ram_ceiling_clean(tmp_path: Path) -> None:
    """all_ram RAM ceiling: clean 503 saturation_cap, RAM bounded, recovers.

    Pins R9-PM-2: with no disk fallback, the saturation bytes gate is the RAM
    bound. A regression bypassing the gate (unbounded RAM / OOM) or returning a
    naked 5xx instead of a clean 503 would turn this RED.
    """
    emu = await boot_emulator()
    data_dir = tmp_path / "phantom-data"
    data_dir.mkdir()
    port = allocate_port()
    cfg = write_phantom_config(data_dir=data_dir, bind_port=port, config_overrides=_overrides())
    p = PhantomSubprocess.make(cfg, port)
    # the RAM-pressure observability read is an admin endpoint on the single
    # listener (p.url).
    admin_url = p.url
    try:
        await p.start()
        bearer = fake_security_token(emu)

        # PAUSE the upstream (503) so admitted bodies NEVER drain out of RAM --
        # RAM pressure builds and STAYS, forcing the bytes cap to bind.
        emu.pause()

        # Fire a burst ~4x the bytes cap; classify each response.
        labels = await asyncio.gather(
            *(_submit_classified(p.url, emu.url, bearer) for _ in range(_BURST))
        )
        refused_503 = sum(1 for lbl in labels if lbl == "http_503")

        # The process must NOT have died (OOM / crash) -- a clean refusal instead.
        assert p._proc is not None and p._proc.poll() is None, (  # type: ignore[attr-defined]
            "phantom subprocess EXITED during the RAM-saturating burst -- an all_ram "
            f"OOM/crash instead of a clean refusal. Log tail:\n{p._read_log_tail(60)}"  # type: ignore[attr-defined]
        )

        # Every refusal must be a clean 503 -- no naked 5xx, no bare client error.
        naked_5xx = [lbl for lbl in labels if lbl.startswith("http_5") and lbl != "http_503"]
        bare_exc = [lbl for lbl in labels if lbl.startswith("exc:")]
        assert not naked_5xx and not bare_exc, (
            "a RAM-ceiling refusal in all_ram was a NON-503 5xx / bare error "
            f"(5xx={naked_5xx} exc={bare_exc}) -- expected a clean 503 saturation_cap. "
            f"Log tail:\n{p._read_log_tail(40)}"  # type: ignore[attr-defined]
        )

        # The tight cap MUST actually bind under a 4x burst against a paused
        # upstream -- otherwise the scenario is vacuous (and RAM would be unbounded).
        assert refused_503 > 0, (
            f"no 503s under a {_BURST}-deep burst against a paused upstream with a "
            f"{_MAX_IN_FLIGHT_BYTES}-byte cap -- the bytes cap did not bind (labels={labels})."
        )

        # RAM at peak MUST NOT materially exceed the bound (gate holds the line).
        peak_ram = await _ram_bytes(admin_url)
        assert peak_ram <= _RAM_TOLERANCE_BYTES, (
            f"ram_body_store_bytes={peak_ram} exceeded the saturation bytes cap "
            f"({_MAX_IN_FLIGHT_BYTES}) beyond a one-body tolerance ({_RAM_TOLERANCE_BYTES}) "
            "-- the all_ram RAM bound was NOT enforced; RAM grew unbounded with no disk "
            "fallback (OOM risk on a Pi)."
        )

        # Recovery: resume upstream, poll RAM down below one body (admitted set
        # cleared), then a fresh upload must cleanly admit.
        emu.resume()
        drain_target = _BODY_BYTES
        deadline = time.monotonic() + _RECOVERY_TIMEOUT_SECONDS
        drained = False
        while time.monotonic() < deadline:
            if await _ram_bytes(admin_url) < drain_target:
                drained = True
                break
            await asyncio.sleep(_RECOVERY_POLL_SECONDS)  # pre-commit-allow: sleep
        assert drained, (
            f"RAM did not drain below one body ({drain_target}) after upstream recovery "
            f"(stuck at {await _ram_bytes(admin_url)}) -- admitted bodies never left RAM."
        )

        recov_label = await _submit_classified(p.url, emu.url, bearer)
        assert recov_label == "ok", (
            f"after RAM pressure cleared, a fresh upload did NOT cleanly admit "
            f"(got {recov_label}) -- the gate wedged refuse-forever."
        )
    finally:
        with contextlib.suppress(Exception):
            emu.resume()
        p.terminate()
        await emu.stop()
