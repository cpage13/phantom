"""A topology-change reload must not crash the instances it keeps running (R8-1).

ADR-013 places the instance list in the restart-required column and
states the runtime contract for a reload whose YAML differs from the
running set: "The reload handler logs a warning when the new YAML's
instance list differs from the running set; the operator restarts to
apply." ``apply_reload``'s own removed-instance branch logs "keeping
previous config" - the documented posture is warn-and-keep-running.

Before the R8-1 fix the implementation broke that promise one line earlier:
``holder.replace(snapshots)`` installs a snapshot map keyed ONLY by the
NEW YAML's instance ids, so a running instance the operator removed (or
renamed, or typo'd) loses its :class:`SettingsHolder` entry the moment
the swap lands. Its :class:`InstanceContext` is then warn-skipped and
keeps its old ``cfg``, but every per-tick live-snapshot read -
``current_settings()`` is bound to ``holder.snapshot_for(cfg.id)`` -
now raises ``KeyError``:

* ``RamPressureWatcher.run`` reads the poll cadence OUTSIDE its tick
  try/except (``ram_pressure.py``), so the KeyError escapes ``run()``,
  cascades out of the composition root's ``asyncio.TaskGroup``, and
  crashes the WHOLE process - every healthy instance included.
* The reaper's interval read (``reaper.py`` ``_current_interval_seconds``)
  escapes the same way when the removed instance is first in its list.
* The sender, admission, and the admin observability routes for the
  removed instance hit the same KeyError on their next read.

Why it matters: removing one instance from the YAML and hitting reload
is a routine operator action that ADR-013 explicitly documents as safe
(warn + keep running until a restart). Instead the reload reports
success, then within one watcher poll interval (default 1.0 s) the
process dies, losing the in-flight RAM bodies of EVERY instance - the
recovery sweep quarantines those rows to ``corrupted`` on restart, so
accepted uploads are lost. That violates the north star ("never lose an
accepted upload") on a documented-safe admin action.

Both tests drive the REAL ``apply_reload`` / ``SettingsHolder`` /
``RamPressureWatcher`` off real boot artifacts, mirroring the R7-1
harness. They pin the operator-observable ADR-013 contract, not a
mechanism: after a reload whose YAML dropped a running instance, that
instance's live-snapshot reads keep working (carried forward, or the
reload is refused with the documented clean-reject pair) and its
workers survive their next tick.
"""

from __future__ import annotations

import asyncio
import contextlib
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock
from uuid import UUID

import yaml
from phantom.config.settings import Settings
from phantom.instances.context import InstanceContext
from phantom.instances.settings_holder import SettingsHolder
from phantom.instances.snapshot import InstanceSettingsSnapshot, _build_snapshot
from phantom.models.upload import UploadRow
from phantom.runtime.reload import apply_reload
from phantom.strategies import build_retry_strategy
from phantom.workers.ram_pressure import RamPressureWatcher
from phantom.workers.saturation import SaturationGate
from pydantic import ValidationError

# Upper bound on one watcher loop iteration in the fixed posture. The
# loop's only wait is ``wait_for(stop_event.wait(), poll_interval)`` and
# the stop event is set before the wait starts, so the real duration is
# milliseconds; the bound only converts a hypothetical hang into a loud
# test failure instead of a suite stall.
_WATCHER_ITERATION_TIMEOUT_SECONDS: float = 10.0

# The two instance ids of the boot topology. inst-b is the one the
# operator removes from the YAML before reloading.
_KEPT_INSTANCE_ID: str = "inst-a"
_REMOVED_INSTANCE_ID: str = "inst-b"

# R8-1 (fixed): apply_reload now carries forward the previous snapshot
# for any live instance the new YAML omits, so per-tick reads keep
# working and workers survive (warn-and-keep-running per ADR-013).


def _two_instance_yaml_payload(data_dir: Path, instance_ids: tuple[str, ...]) -> dict[str, Any]:
    """A config whose instance list is exactly ``instance_ids``.

    Every probe-fillable knob is left to the machine probe (the
    documented smart-defaults posture), keeping the harness identical to
    the R7-1 repro and independent of this machine's capacity facts.
    """
    return {
        "storage": {"data_dir": str(data_dir)},
        "instances": [
            {
                "id": instance_id,
                "host_prefixes": [f"{instance_id}.example.com"],
                "data_dir": instance_id,
                "routes": [
                    {
                        "name": "files",
                        "hosts": [f"{instance_id}.example.com"],
                        "auth_mode": "phantom_bearer",
                    }
                ],
            }
            for instance_id in instance_ids
        ],
    }


def _boot_context(boot: Settings, holder: SettingsHolder, index: int) -> InstanceContext:
    """Assemble the minimal real InstanceContext ``apply_reload`` touches.

    Mirrors the R7-1 harness: the saturation gate is REAL and built from
    the probe-resolved boot values exactly as the composition root
    builds it; storage components are inert mocks (``apply_reload``
    never touches them). ``current_settings`` is the production binding,
    ``holder.snapshot_for(cfg.id)``.
    """
    cfg = boot.instances[index]
    assert boot.saturation.max_in_flight is not None
    assert boot.saturation.max_in_flight_bytes is not None
    assert boot.saturation.max_disk_bytes is not None
    return InstanceContext(
        cfg=cfg,
        store=MagicMock(),
        ram_body_store=MagicMock(),
        file_body_store=MagicMock(),
        body_store=MagicMock(),
        persist_controller=None,
        token_cache=MagicMock(),
        minter=None,
        retry_strategy=build_retry_strategy(boot.retry.default_strategy),
        upstream_client=MagicMock(),
        executor=MagicMock(),
        saturation=SaturationGate(
            max_in_flight=boot.saturation.max_in_flight,
            max_in_flight_bytes=boot.saturation.max_in_flight_bytes,
            max_disk_bytes=boot.saturation.max_disk_bytes,
        ),
        codec_factory=MagicMock(),
        current_settings=lambda: holder.snapshot_for(cfg.id),
    )


def _boot_two_instances(tmp_path: Path) -> tuple[Settings, Path]:
    """Write the two-instance YAML and boot Settings from it.

    The boot load runs the REAL machine probe (``skip_probe`` defaults
    to ``False``), so every capacity knob resolves to a non-None
    machine-derived value - the state a real process boots into.
    """
    settings_path = tmp_path / "phantom.yaml"
    settings_path.write_text(
        yaml.safe_dump(
            _two_instance_yaml_payload(
                tmp_path / "data",
                (_KEPT_INSTANCE_ID, _REMOVED_INSTANCE_ID),
            )
        )
    )
    boot = Settings.reload_from_yaml(settings_path)
    assert [cfg.id for cfg in boot.instances] == [
        _KEPT_INSTANCE_ID,
        _REMOVED_INSTANCE_ID,
    ], "precondition: both instances boot"
    return boot, settings_path


class _EmptyCandidateStore:
    """Upload store with no RAM-resident migration candidates."""

    async def list_oldest_ram_bodies(self, limit: int) -> list[UUID]:
        """Return no candidates (the crash leg needs none)."""
        del limit
        return []

    async def get(self, chain_id: UUID) -> UploadRow | None:
        """No rows exist in this store."""
        del chain_id
        return None


class _StopSignallingRamStore:
    """RAM store whose ``total_bytes`` read sets the loop's stop event.

    In the fixed posture the watcher's first tick reads the RAM total,
    finds it under the ceiling, returns, reads the poll cadence, and
    then sees the stop event already set - so ``run()`` completes after
    exactly one iteration with no timing dependence. In the broken
    posture the per-tick ceiling read raises ``KeyError`` BEFORE this
    hook runs, and the cadence read outside the tick try/except raises
    the same ``KeyError`` out of ``run()``.
    """

    def __init__(self, stop_event: asyncio.Event) -> None:
        self._stop_event = stop_event

    async def total_bytes(self) -> int:
        """Signal the loop to stop after this tick, report an idle store."""
        self._stop_event.set()
        return 0


class _LiveHolderInstance:
    """Instance surface for the watcher, reading the LIVE holder per tick.

    ``current_settings`` goes through the real :class:`SettingsHolder`
    exactly as the composition root binds it, so the watcher sees what
    the real reload installed (or removed).
    """

    def __init__(
        self,
        *,
        holder: SettingsHolder,
        instance_id: str,
        ram_body_store: _StopSignallingRamStore,
        store: _EmptyCandidateStore,
    ) -> None:
        self._holder = holder
        self._instance_id = instance_id
        self.ram_body_store = ram_body_store
        self.store = store

    def current_settings(self) -> InstanceSettingsSnapshot:
        """Return the live snapshot the worker reads per tick."""
        return self._holder.snapshot_for(self._instance_id)


async def test_reload_dropping_an_instance_keeps_its_snapshot_readable(
    tmp_path: Path,
) -> None:
    """A removed-from-YAML instance's live-snapshot reads must keep working.

    Attack: boot two instances, remove inst-b from the YAML (the
    routine operator action ADR-013 documents as warn-and-keep-running),
    then drive the REAL ``apply_reload``. The documented clean-reject
    pair (``yaml.YAMLError`` / ``ValidationError``) is tolerated ONLY
    because the boot snapshot must then still be live. Afterwards
    inst-b's ``current_settings()`` - the binding every worker tick and
    admin observability read goes through - must still return a usable
    snapshot per the "keeping previous config" contract.

    Today ``holder.replace`` keyed the map by the new YAML's ids only,
    so the read raises ``KeyError`` and every consumer of the kept-
    running instance is broken until restart.
    """
    boot, settings_path = _boot_two_instances(tmp_path)
    holder = SettingsHolder({cfg.id: _build_snapshot(boot, cfg) for cfg in boot.instances})
    ctx_kept = _boot_context(boot, holder, 0)
    ctx_removed = _boot_context(boot, holder, 1)

    settings_path.write_text(
        yaml.safe_dump(_two_instance_yaml_payload(tmp_path / "data", (_KEPT_INSTANCE_ID,)))
    )
    with contextlib.suppress(yaml.YAMLError, ValidationError):
        await apply_reload(holder, settings_path, [ctx_kept, ctx_removed])

    live = ctx_removed.current_settings()
    assert live.body_store.ram_ceiling_bytes is not None, (
        "the removed-but-still-running instance lost its live snapshot: "
        "ADR-013 promises the reload warns and keeps it on its previous "
        "config until the operator restarts"
    )


async def test_reload_dropping_an_instance_does_not_crash_its_workers(
    tmp_path: Path,
) -> None:
    """The removed instance's RamPressureWatcher must survive its next tick.

    Attack: same topology-change reload as above, then run ONE iteration
    of the REAL :class:`RamPressureWatcher` loop for the removed
    instance (the loop's cadence read sits OUTSIDE the per-tick
    try/except, so this is the first worker to die in production - and
    an unhandled worker exception cascades out of the composition
    root's TaskGroup, crashing the process and losing every instance's
    in-flight RAM bodies). The loop must complete its iteration without
    raising; today the ``KeyError`` from the holder escapes ``run()``.
    """
    boot, settings_path = _boot_two_instances(tmp_path)
    holder = SettingsHolder({cfg.id: _build_snapshot(boot, cfg) for cfg in boot.instances})
    ctx_kept = _boot_context(boot, holder, 0)
    ctx_removed = _boot_context(boot, holder, 1)

    settings_path.write_text(
        yaml.safe_dump(_two_instance_yaml_payload(tmp_path / "data", (_KEPT_INSTANCE_ID,)))
    )
    with contextlib.suppress(yaml.YAMLError, ValidationError):
        await apply_reload(holder, settings_path, [ctx_kept, ctx_removed])

    stop_event = asyncio.Event()
    watcher_instance = _LiveHolderInstance(
        holder=holder,
        instance_id=_REMOVED_INSTANCE_ID,
        ram_body_store=_StopSignallingRamStore(stop_event),
        store=_EmptyCandidateStore(),
    )
    watcher = RamPressureWatcher(
        instance=watcher_instance,  # type: ignore[arg-type]  # duck-typed minimal surface
        persist_controller=MagicMock(),
    )

    # One full loop iteration: tick, cadence read, stop. KeyError out of
    # run() is the process-crash leg; the wait_for bound only turns a
    # hypothetical hang into a loud failure.
    await asyncio.wait_for(
        watcher.run(stop_event),
        timeout=_WATCHER_ITERATION_TIMEOUT_SECONDS,
    )
