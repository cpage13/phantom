"""A restart-required mode change must not ride the snapshot into live reads (R9-2).

ADR-013 marks ``body_store.mode`` restart-required, and ADR-031
decision 3 puts it in the enumerated restart-required set: the mode
selects which BodyStore classes are wired and which workers spawn at
the composition root, none of which a reload can change. The reload
path has guards for the OTHER restart-required drifts - topology gets
warn-and-carry-forward (R8-1), ad_mint gets a restart-required WARNING -
but the mode has NO guard at all: ``_build_snapshot`` projects the full
``BodyStoreCfg`` (mode included) into the new snapshot, and
``apply_reload`` swaps it live without warning or refusal.

Two per-request consumers read the mode from the live snapshot, so the
swapped value half-applies while the wiring stays the boot mode
(``routes/admission.py``):

* the mode-aware initial ``body_location`` - boot hybrid + reload
  all_disk makes every new admission a ``body_location='file'`` row
  whose bytes the boot-wired HybridBodyStore put in RAM. The row's
  location column lies: invariant #1 is violated at insert; the
  migration triggers (linger, RAM pressure) key on
  ``body_location='ram'`` so the bytes are invisible to ceiling
  enforcement and can never be persisted; on the next restart the
  recovery sweep quarantines the row to ``corrupted`` because the
  claimed disk files do not exist - an ACCEPTED upload lost on a
  restart that correctly-labeled hybrid rows would have survived via
  linger migration. The mirror direction (boot all_disk + reload
  hybrid) births ``'ram'`` rows whose bytes are durably on disk, which
  recovery then quarantines for the opposite lie.
* the hybrid-only immediate-persist trigger - boot hybrid + reload
  all_ram kills ``_maybe_enqueue_immediate_persist``'s
  ``mode == "hybrid"`` arm while the PersistController is still wired
  and running, so the ``body_size_threshold_bytes`` knob silently stops
  applying (large bodies wait out the full linger on a deployment that
  explicitly configured immediate persistence).

The reload reports success and lists the instance as reloaded; nothing
warns. MED severity: the operator action is off the documented path
(the table says restart), but the system neither refuses nor stays
consistent - it silently enters a torn state with a north-star harm
armed for the next restart.

Both tests boot real ``Settings`` from a probe-reliant YAML (the
knob-matrix producer posture) and run the REAL ``apply_reload``. The fix
posture is the R8-1 family's: carry the boot mode forward (warn), or
refuse the reload cleanly - the assertions hold under either (the
reload attempt is wrapped in the documented failure set so a clean
refusal also passes). Falsifiability proven both ways in scratch: today
the snapshot carries the new mode and the immediate-persist decision
dies; a carried-forward-mode snapshot keeps both truthful.
"""

from __future__ import annotations

import contextlib
import copy
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, MagicMock
from uuid import UUID, uuid4

import pytest
import yaml
from phantom.config.settings import Settings
from phantom.instances.settings_holder import SettingsHolder
from phantom.instances.snapshot import _build_snapshot
from phantom.routes.admission import _maybe_enqueue_immediate_persist
from phantom.runtime.reload import RELOAD_FAILURE_ERRORS, apply_reload

pytestmark = pytest.mark.asyncio

# The single instance id used by the producer.
_INSTANCE_ID = "inst-a"

# The boot deployment mode (the production default) and the two reload
# targets that tear the per-request reads away from the boot wiring.
_BOOT_MODE = "hybrid"
_RELOADED_MODE_DISK = "all_disk"
_RELOADED_MODE_RAM = "all_ram"

_R9_2_REASON: str = (
    "R9-2: apply_reload projects a changed body_store.mode (restart-required, "
    "ADR-013/ADR-031) into the live snapshots with no warn or refusal, so "
    "admission's per-request mode reads half-apply against the boot-wired "
    "stores: new rows are born with a body_location that lies about where "
    "their bytes are (quarantined as corrupted on the next restart), and the "
    "hybrid-only immediate-persist trigger silently dies"
)


def _base_yaml_payload(data_dir: Path) -> dict[str, Any]:
    """A probe-reliant one-instance config booting the default hybrid mode."""
    return {
        "storage": {"data_dir": str(data_dir)},
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


def _boot_holder(tmp_path: Path) -> tuple[SettingsHolder, Path, dict[str, Any], Any]:
    """Boot real Settings exactly as production does; return the reload surface."""
    raw = _base_yaml_payload(tmp_path / "data")
    settings_path = tmp_path / "phantom.yaml"
    settings_path.write_text(yaml.safe_dump(raw))
    boot = Settings.reload_from_yaml(settings_path)
    cfg = boot.instances[0]
    holder = SettingsHolder({cfg.id: _build_snapshot(boot, cfg)})
    return holder, settings_path, raw, cfg


async def _reload_with_mode(
    holder: SettingsHolder,
    settings_path: Path,
    raw: dict[str, Any],
    cfg: Any,
    *,
    mode: str,
) -> None:
    """Run the REAL apply_reload against a YAML that pins a new mode.

    The attempt is wrapped in the documented reload failure set so a
    fixed implementation that refuses the mode change cleanly (instead
    of carrying the boot mode forward) also satisfies the assertions.
    """
    new_raw = copy.deepcopy(raw)
    new_raw.setdefault("storage", {})["body_store"] = {"mode": mode}
    settings_path.write_text(yaml.safe_dump(new_raw))
    reload_ctx = MagicMock()
    reload_ctx.cfg = cfg
    reload_ctx.token_cache = MagicMock()
    reload_ctx.saturation = MagicMock(update_caps=AsyncMock())
    with contextlib.suppress(*RELOAD_FAILURE_ERRORS):
        await apply_reload(holder, settings_path, [reload_ctx])


class _RecordingPersistController:
    """Minimal controller double recording immediate-persist enqueues."""

    def __init__(self) -> None:
        self.enqueued: list[UUID] = []

    async def enqueue(self, chain_id: UUID) -> None:
        """Record the enqueue the admission trigger fired."""
        self.enqueued.append(chain_id)


async def test_mode_change_does_not_reach_the_live_snapshot(tmp_path: Path) -> None:
    """A reload that flips the mode must not change the live snapshot's mode.

    Attack: boot hybrid (stores wired, PersistController spawned),
    reload a YAML pinning all_disk. The composition cannot follow, so
    the live snapshot - which admission reads per request to choose the
    initial ``body_location`` - must keep reporting the boot mode (or
    the reload must refuse). Today the new mode swaps straight in and
    every subsequent admission writes rows whose location column lies
    about where the boot-wired HybridBodyStore actually put the bytes.
    """
    holder, settings_path, raw, cfg = _boot_holder(tmp_path)
    assert holder.snapshot_for(_INSTANCE_ID).body_store.mode == _BOOT_MODE, (
        "precondition: the producer boots the default hybrid mode"
    )

    await _reload_with_mode(holder, settings_path, raw, cfg, mode=_RELOADED_MODE_DISK)

    live_mode = holder.snapshot_for(_INSTANCE_ID).body_store.mode
    assert live_mode == _BOOT_MODE, (
        f"the restart-required mode change leaked into the live snapshot "
        f"(now {live_mode!r}): admission's next per-request read births "
        "body_location='file' rows whose bytes the boot-wired hybrid store "
        "put in RAM - invariant #1 broken at insert, the bytes invisible to "
        "linger and RAM-pressure migration, and the rows quarantined as "
        "corrupted on the next restart"
    )


async def test_immediate_persist_trigger_survives_a_mode_change_reload(
    tmp_path: Path,
) -> None:
    """The size-threshold persist trigger must keep firing after the reload.

    Attack: boot hybrid (controller wired and running), reload a YAML
    pinning all_ram. The immediate-persist trigger reads the mode from
    the live snapshot per request and is gated on ``mode == "hybrid"``,
    so the leaked mode kills the ``body_size_threshold_bytes`` knob
    while the controller still runs - large bodies silently wait out
    the full linger on a deployment that configured immediate
    persistence. Under the boot truth (or a refused reload) the enqueue
    must still fire.
    """
    holder, settings_path, raw, cfg = _boot_holder(tmp_path)

    await _reload_with_mode(holder, settings_path, raw, cfg, mode=_RELOADED_MODE_RAM)

    snapshot = holder.snapshot_for(_INSTANCE_ID)
    controller = _RecordingPersistController()
    instance_ctx = MagicMock()
    instance_ctx.persist_controller = controller
    threshold = snapshot.persist_trigger.body_size_threshold_bytes
    assert threshold is not None and threshold > 0, (
        "precondition: the probe resolved a positive immediate-persist threshold"
    )

    await _maybe_enqueue_immediate_persist(
        instance_ctx,
        chain_id=uuid4(),
        stored_size=threshold,
        snapshot=snapshot,
    )

    assert len(controller.enqueued) == 1, (
        "a threshold-sized body no longer enqueues immediate persistence "
        "after the mode-change reload: the leaked all_ram mode disabled the "
        "hybrid-only trigger while the boot-wired PersistController is still "
        "running - the body_size_threshold_bytes knob silently died"
    )
