"""The InvariantAuditor's cadence must follow a hot reload (R9-1).

ADR-031 decision 1 makes live-snapshot read the canonical distribution
mechanism: a worker MUST NOT cache a snapshot-derived value across loop
iterations, and a constructor MUST NOT take a config value a snapshot
can carry. ``invariant_audit_period_seconds`` is a ``BodyStoreCfg``
field, and the full ``BodyStoreCfg`` IS projected into every
``InstanceSettingsSnapshot`` - so after a reload the live snapshot
carries the operator's new cadence. But the InvariantAuditor violates
both halves of decision 1: its constructor takes the BOOT ``Settings``
object (``workers/invariant_audit.py``, ``self._settings``), and
``run()`` hoists the period read out of the loop entirely (one read
before the first iteration). The reloaded value never reaches the
running auditor; the knob is a silent config no-op until restart -
the same consumer-side cache class as R5-2 / R6-2 / T1's janitor.

The knob-matrix contract test cannot catch this: the knob is missing
from the ADR-013 table altogether (it is neither a live-read row nor a
member of the enumerated restart-required set, which invariant 18 says
must cover everything), so no matrix row observes it - exactly the
blind spot ADR-031 was written to close. The sibling cadences all read
live: the reaper interval, the janitor sweep (T1, fixed today), the
RAM-pressure poll. The auditor is the one worker cadence a reload
cannot retune.

Harm: an operator chasing suspected corruption tightens the audit
cadence via reload (the documented operational path for every other
cadence) and silently keeps the boot cadence - detection latency stays
at the old period while the reload reported success. LOW severity
(diagnostic latency only, no data path), but a structural violation of
invariant 18 created today.

The test boots real ``Settings`` from a probe-reliant YAML exactly as
the knob-matrix producer does, reloads the cadence from one hour down to one
second through the REAL ``apply_reload`` BEFORE the auditor's loop
starts (so even the first armed wait should see the new value under a
live-read implementation), then requires a second sweep within a
bounded deadline. The auditor's own ``invariant_audit_runs_total``
counter is the observation surface. Falsifiability proven both ways in
scratch: the real auditor never re-sweeps inside the deadline (the boot
hour is pinned); a T1-style live-read variant re-sweeps in about one
second.
"""

from __future__ import annotations

import asyncio
import contextlib
import copy
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest
import yaml
from phantom.config.settings import Settings
from phantom.instances.settings_holder import SettingsHolder
from phantom.instances.snapshot import _build_snapshot
from phantom.observability.metrics import MetricsRegistry
from phantom.runtime.reload import apply_reload
from phantom.storage import FileBodyStore, RamBodyStore, SqliteUploadStore
from phantom.storage.hybrid_body_store import HybridBodyStore
from phantom.workers.invariant_audit import InvariantAuditor

from .conftest import track_started

pytestmark = pytest.mark.asyncio

# The single instance id used by the producer.
_INSTANCE_ID = "inst-a"

# Boot cadence: one hour, so a pinned auditor demonstrably never
# re-sweeps inside the test deadline.
_BOOT_PERIOD_SECONDS: int = 3600

# Reloaded cadence: one second, the operator's tightened value.
_RELOADED_PERIOD_SECONDS: int = 1

# A second sweep under the reloaded one-second cadence lands at about
# one second; five seconds is generous headroom for a loaded host while
# staying far below the boot hour.
_SECOND_SWEEP_DEADLINE_SECONDS: float = 5.0

# Counter-poll interval while waiting for the second sweep.
_SWEEP_POLL_SECONDS: float = 0.05

# Budget for the run loop to drain after stop_event fires.
_STOP_DRAIN_SECONDS: float = 2.0

_R9_1_REASON: str = (
    "R9-1: InvariantAuditor pins its cadence at construction (boot Settings) "
    "and hoists the period read out of its run loop, so a reloaded "
    "invariant_audit_period_seconds never reaches the running auditor even "
    "though the live snapshot carries it (BodyStoreCfg is snapshot-projected); "
    "the knob is also missing from the ADR-013 table so the knob-matrix test "
    "cannot see it - a violation of ADR-031 decision 1 and invariant 18"
)


def _base_yaml_payload(data_dir: Path) -> dict[str, Any]:
    """A probe-reliant one-instance config (the smart-defaults posture)."""
    return {
        "storage": {
            "data_dir": str(data_dir),
            "body_store": {"invariant_audit_period_seconds": _BOOT_PERIOD_SECONDS},
        },
        "instances": [
            {
                "id": _INSTANCE_ID,
                "host_prefixes": ["files.example.com"],
                "data_dir": _INSTANCE_ID,
                "routes": [
                    {
                        "name": "files",
                        "hosts": ["files.example.com"],
                        "auth_mode": "phantom_bearer",
                    }
                ],
            }
        ],
    }


async def test_reloaded_audit_cadence_reaches_the_running_auditor(tmp_path: Path) -> None:
    """A reload tightening the audit cadence must retune the running loop.

    Attack: boot with a one-hour cadence, reload to one second through
    the REAL ``apply_reload`` (the live snapshot now carries one
    second), start the REAL auditor, and require its second sweep
    within five seconds. The pinned implementation reads the boot hour
    once before the loop and never sweeps again inside the deadline.
    """
    raw = _base_yaml_payload(tmp_path / "data")
    settings_path = tmp_path / "phantom.yaml"
    settings_path.write_text(yaml.safe_dump(raw))
    boot = Settings.reload_from_yaml(settings_path)
    cfg = boot.instances[0]
    holder = SettingsHolder({cfg.id: _build_snapshot(boot, cfg)})

    store = track_started(SqliteUploadStore(str(tmp_path / "uploads.db")))
    await store.start()
    ram = track_started(RamBodyStore())
    await ram.start()
    fbs = track_started(FileBodyStore(tmp_path / "bodies"))
    await fbs.start()
    body_store = track_started(HybridBodyStore(ram=ram, disk=fbs))
    await body_store.start()

    registry = MetricsRegistry()
    # R9-1 fix: the auditor takes the live-snapshot thunk (the boot
    # Settings capture and the loop-hoisted period read are gone).
    auditor = InvariantAuditor(
        store=store,
        body_store=body_store,
        current_settings=lambda: holder.snapshot_for(_INSTANCE_ID),
        metrics_registry=registry,
    )

    # The operator tightens the cadence and reloads BEFORE the loop
    # starts, so even the first armed wait should use the new value
    # under a live-read implementation (no armed-timer ambiguity).
    new_raw = copy.deepcopy(raw)
    new_raw["storage"]["body_store"]["invariant_audit_period_seconds"] = _RELOADED_PERIOD_SECONDS
    settings_path.write_text(yaml.safe_dump(new_raw))
    reload_ctx = MagicMock()
    reload_ctx.cfg = cfg
    reload_ctx.token_cache = MagicMock()
    reload_ctx.saturation = MagicMock(update_caps=AsyncMock())
    await apply_reload(holder, settings_path, [reload_ctx])
    live_period = holder.snapshot_for(_INSTANCE_ID).body_store.invariant_audit_period_seconds
    assert live_period == _RELOADED_PERIOD_SECONDS, (
        "precondition: the live snapshot must carry the reloaded cadence "
        f"(got {live_period}); the distribution layer delivered the knob"
    )

    stop_event = asyncio.Event()
    task = asyncio.create_task(auditor.run(stop_event))
    runs = registry.counters["invariant_audit_runs_total"]
    second_sweep_seen = False
    try:
        with contextlib.suppress(TimeoutError):
            async with asyncio.timeout(_SECOND_SWEEP_DEADLINE_SECONDS):
                while True:
                    if sum(runs.snapshot().values()) >= 2:
                        second_sweep_seen = True
                        break
                    await asyncio.sleep(_SWEEP_POLL_SECONDS)
    finally:
        stop_event.set()
        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(task, timeout=_STOP_DRAIN_SECONDS)

    assert second_sweep_seen, (
        "the reloaded one-second audit cadence never reached the running "
        "auditor: no second sweep within "
        f"{_SECOND_SWEEP_DEADLINE_SECONDS} s, so the loop is still pacing on "
        "the construction-pinned boot hour - the reload was a silent no-op "
        "on this knob"
    )
