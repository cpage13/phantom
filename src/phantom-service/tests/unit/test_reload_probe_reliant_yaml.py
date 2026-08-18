"""Hot reload of a probe-reliant YAML must not degrade live enforcement (R7-1).

The smart-defaults contract (CONTEXT.md "Smart defaults from system
probe", plan Family 6) is that operators may omit every probe-fillable
knob from YAML: ``_resolve_defaults`` fills ``saturation.*``,
``body_store.ram_ceiling_bytes``, and
``persist_trigger.body_size_threshold_bytes`` from machine facts at
boot. ADR-013 then promises that operational config reloads via SIGHUP
or ``POST /v1/admin/reload``, and that on a reload failure "the
in-flight SettingsHolder is not swapped, so the running config is
unaffected".

Before the R7-1 fix the reload path broke both promises for exactly the
deployment the probe-fill feature exists for - a YAML that pins none of
the probe-fillable knobs:

1. ``apply_reload`` loads the YAML with ``skip_probe=True``, so every
   unpinned probe-fillable field is ``None`` in the reloaded Settings.
2. ``holder.replace(snapshots)`` swaps those ``None`` holes into the
   LIVE per-instance snapshots BEFORE any consistency check runs.
3. ``SaturationGate.update_caps`` then hits its non-None assertions
   ("guaranteed non-None by the Settings validator" - untrue under
   ``skip_probe=True``) and raises ``AssertionError``, which is neither
   ``yaml.YAMLError`` nor ``ValidationError``, so:
   - ``POST /v1/admin/reload`` answers a raw 500 (outside the ADR-017
     envelope), and
   - the SIGHUP path leaks an unretrieved task exception,
   and in BOTH cases the half-applied swap from step 2 stays live.
4. The R6-2 fix made :class:`RamPressureWatcher` read
   ``current_settings().body_store.ram_ceiling_bytes`` per tick, and it
   treats ``None`` as "disabled". With the ``None`` hole now live, the
   watcher silently stops enforcing the RAM ceiling that the boot-time
   probe had been enforcing (before R6-2 the constructor-captured boot
   ceiling kept enforcement alive through such a reload, so this leg is
   a regression introduced by the fix). Admission's large-body
   immediate-persist threshold goes dark the same way.

Why it matters: the default Pi-class deployment posture is exactly
"omit the knobs, trust the probe". An operator who hot-reloads to tweak
ANY unrelated knob (retention, codec, retry) gets a 500, a half-applied
config, and a silent loss of the RAM memory-safety bound - the watcher
keeps ticking but never enforces again until restart.

Both tests drive the REAL ``apply_reload`` against the REAL boot
artifacts (Settings probe-fill, SettingsHolder, SaturationGate). They
pin the operator-observable contract, not an implementation. The R7-1
fix chose re-resolving holes at reload time: ``apply_reload`` now loads
with the host probe ON (the classmethod's default), so omitted knobs
re-resolve from current machine facts and resolution + validation
complete before any swap; operator-pinned YAML always wins.
"""

from __future__ import annotations

import contextlib
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock
from uuid import UUID, uuid4

import yaml
from phantom.config.settings import Settings
from phantom.instances.context import InstanceContext
from phantom.instances.settings_holder import SettingsHolder
from phantom.instances.snapshot import InstanceSettingsSnapshot, _build_snapshot
from phantom.models.upload import UploadRow
from phantom.runtime.reload import apply_reload
from phantom.storage.interface import PersistCandidateState
from phantom.strategies import build_retry_strategy
from phantom.workers.ram_pressure import RamPressureWatcher
from phantom.workers.saturation import SaturationGate
from pydantic import ValidationError

# Margin parked above the probe-resolved boot ceiling so the RAM total
# is a genuine breach under the boot configuration whatever this
# machine's probe derives. One byte over the ceiling is the minimal
# unambiguous breach (the watcher's gate is ``current < max_bytes``).
_PARKED_MARGIN_BYTES: int = 1

# R7-1 (fixed): apply_reload of a probe-reliant YAML used to swap None
# holes into the live snapshots, detonate update_caps' non-None asserts
# after the swap (raw 500 / unretrieved SIGHUP task exception,
# half-applied reload), and the R6-2 per-tick ceiling read then treated
# the None as "no cap". The fix loads the reload with the probe ON (the
# classmethod's default), so omitted knobs re-resolve before any swap.


def _probe_reliant_yaml_payload(data_dir: Path) -> dict[str, Any]:
    """A one-instance config that pins NO probe-fillable knob.

    This is the documented default deployment posture: storage paths and
    instance topology in YAML, every capacity knob left to the boot-time
    machine probe (CONTEXT.md "Operator-supplied YAML values always
    win" - and absent values are filled, not required).
    """
    return {
        "storage": {"data_dir": str(data_dir)},
        "instances": [
            {
                "id": "inst-a",
                "host_prefixes": ["files.example.com"],
                "data_dir": "inst-a",
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


def _boot_context(
    boot: Settings,
    holder: SettingsHolder,
) -> InstanceContext:
    """Assemble the minimal real InstanceContext ``apply_reload`` touches.

    The saturation gate is REAL and built from the probe-resolved boot
    values, exactly as the composition root builds it; the storage
    components are inert mocks (``apply_reload`` never touches them), so
    nothing here needs a teardown.
    """
    cfg = boot.instances[0]
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


class _RecordingPersistController:
    """Records every ``enqueue`` chain_id; otherwise inert."""

    def __init__(self) -> None:
        self.enqueued: list[UUID] = []

    async def enqueue(self, chain_id: UUID) -> None:
        """Record the migration request the watcher would issue."""
        self.enqueued.append(chain_id)


class _StaticRamBodyStore:
    """RAM body store whose ``total_bytes`` is a fixed parked value."""

    def __init__(self, *, total: int) -> None:
        self._total = total

    async def total_bytes(self) -> int:
        """Return the fixed parked RAM byte total."""
        return self._total


class _OneCandidateStore:
    """Upload store exposing exactly one oldest RAM-resident chain."""

    def __init__(self, *, chain_id: UUID, row: UploadRow) -> None:
        self._chain_id = chain_id
        self._row = row

    async def list_oldest_ram_bodies(self, limit: int) -> list[UUID]:
        """Return the one oldest RAM-resident chain_id (ignores ``limit``)."""
        del limit
        return [self._chain_id]

    async def get_persist_candidate_state(self, chain_id: UUID) -> PersistCandidateState | None:
        """Return the seeded ``queued`` row's state and stamp for the candidate."""
        if chain_id != self._chain_id:
            return None
        return PersistCandidateState(state=self._row.state, updated_at=self._row.updated_at)


class _WatcherInstance:
    """Instance surface for the watcher, reading the LIVE holder per tick.

    Unlike a swapped-by-hand fake, ``current_settings`` goes through the
    real :class:`SettingsHolder`, so the watcher sees exactly what the
    real reload installed (or, under a clean-reject fix, did not).
    """

    def __init__(
        self,
        *,
        holder: SettingsHolder,
        instance_id: str,
        ram_body_store: _StaticRamBodyStore,
        store: _OneCandidateStore,
    ) -> None:
        self._holder = holder
        self._instance_id = instance_id
        self.ram_body_store = ram_body_store
        self.store = store

    def current_settings(self) -> InstanceSettingsSnapshot:
        """Return the live snapshot the worker reads per tick."""
        return self._holder.snapshot_for(self._instance_id)


def _queued_ram_row(chain_id: UUID, instance_id: str) -> UploadRow:
    """Build a minimal ``queued`` RAM-resident row for the candidate."""
    now = datetime.now(tz=UTC)
    return UploadRow.model_validate(
        {
            "chain_id": chain_id,
            "instance_id": instance_id,
            "group_id": uuid4(),
            "multifile_id": None,
            "send_order": 0,
            "route_name": "files",
            "state": "queued",
            "body_location": "ram",
            "received_at": now,
            "updated_at": now,
            "endpoint": "upstream.example.com",
            "uid": "user-1",
            "chain_envelope_json": "{}",
            "idempotency_key": "k",
            "capture_reexecution_active": False,
        }
    )


def _boot_probe_reliant(tmp_path: Path) -> tuple[Settings, Path]:
    """Write the probe-reliant YAML and boot Settings from it.

    The boot load runs the REAL machine probe (``skip_probe`` defaults
    to ``False``), so every capacity knob is resolved to a non-None
    machine-derived value - the state a real process boots into.
    """
    settings_path = tmp_path / "phantom.yaml"
    settings_path.write_text(yaml.safe_dump(_probe_reliant_yaml_payload(tmp_path / "data")))
    boot = Settings.reload_from_yaml(settings_path)
    assert boot.storage.body_store.ram_ceiling_bytes is not None, (
        "precondition: the boot probe must fill the RAM ceiling"
    )
    assert boot.saturation.max_in_flight is not None, (
        "precondition: the boot probe must fill the saturation caps"
    )
    return boot, settings_path


async def test_reload_of_probe_reliant_yaml_keeps_resolved_knobs_enforceable(
    tmp_path: Path,
) -> None:
    """A reload of the unmodified boot YAML must leave enforceable knobs live.

    Attack: boot from a YAML that pins no probe-fillable knob (the
    documented smart-defaults posture), then drive the REAL
    ``apply_reload`` against the very same file - the shape of an
    operator hot-reloading after tweaking an unrelated knob. ADR-013
    allows exactly two failure modes (``yaml.YAMLError`` /
    ``ValidationError``), both of which must leave the running config
    unaffected; any other escape is the route's raw 500 and the SIGHUP
    path's unretrieved task exception. Afterwards the LIVE snapshot must
    still carry non-None capacity knobs - either the reload completed
    with values carried over or re-resolved, or it was rejected cleanly
    and the boot snapshot survived.

    Before the R7-1 fix the call raised ``AssertionError`` from
    ``SaturationGate.update_caps`` AFTER ``holder.replace`` had already
    installed snapshots whose probe-fillable fields were all ``None``
    (a half-applied reload); the fix loads with the probe ON, so the
    holes re-resolve and validation completes before any swap.
    """
    boot, settings_path = _boot_probe_reliant(tmp_path)
    cfg = boot.instances[0]
    holder = SettingsHolder({cfg.id: _build_snapshot(boot, cfg)})
    ctx = _boot_context(boot, holder)

    # The documented clean-reject pair (ADR-013) is tolerated ONLY
    # because the holder must then still carry the boot snapshot, which
    # the assertions below verify. Any other escape fails the test.
    with contextlib.suppress(yaml.YAMLError, ValidationError):
        await apply_reload(holder, settings_path, [ctx])

    live = holder.snapshot_for(cfg.id)
    assert live.body_store.ram_ceiling_bytes is not None, (
        "the live snapshot lost its RAM ceiling: the reload swapped a "
        "None hole into the running config (ADR-013 promises a failed "
        "reload leaves the running config unaffected)"
    )
    assert live.saturation.max_in_flight is not None, (
        "the live snapshot lost its saturation caps to the same swap"
    )
    assert live.persist_trigger.body_size_threshold_bytes is not None, (
        "the live snapshot lost the large-body persist threshold to the same swap"
    )


async def test_ram_ceiling_enforcement_survives_probe_reliant_reload(
    tmp_path: Path,
) -> None:
    """The RAM ceiling enforced at boot must survive an unrelated reload.

    Attack: same probe-reliant boot + real ``apply_reload`` as above,
    then park RAM one byte over the BOOT (probe-resolved) ceiling and
    tick the REAL :class:`RamPressureWatcher`, whose R6-2 fix reads the
    ceiling from the live holder per tick. The operator never touched
    ``ram_ceiling_bytes``, so the watcher must still enqueue the oldest
    RAM body. Before the R7-1 fix the live snapshot's ceiling was the
    swapped-in ``None`` hole, the watcher's gate treated it as "no
    cap", and the enqueue never happened (a regression of the R6-2
    fix); with the probe-on reload the ceiling re-resolves and
    enforcement survives.
    """
    boot, settings_path = _boot_probe_reliant(tmp_path)
    cfg = boot.instances[0]
    assert boot.storage.body_store.ram_ceiling_bytes is not None  # narrowed for typing
    boot_ceiling: int = boot.storage.body_store.ram_ceiling_bytes
    holder = SettingsHolder({cfg.id: _build_snapshot(boot, cfg)})
    ctx = _boot_context(boot, holder)

    # Documented clean-reject posture tolerated: the boot snapshot must
    # then still be live, and the enforcement assertion below holds.
    with contextlib.suppress(yaml.YAMLError, ValidationError):
        await apply_reload(holder, settings_path, [ctx])

    chain_id = uuid4()
    watcher_instance = _WatcherInstance(
        holder=holder,
        instance_id=cfg.id,
        ram_body_store=_StaticRamBodyStore(total=boot_ceiling + _PARKED_MARGIN_BYTES),
        store=_OneCandidateStore(
            chain_id=chain_id,
            row=_queued_ram_row(chain_id, cfg.id),
        ),
    )
    controller = _RecordingPersistController()
    watcher = RamPressureWatcher(
        instance=watcher_instance,  # type: ignore[arg-type]  # duck-typed minimal surface
        persist_controller=controller,  # type: ignore[arg-type]  # recording stand-in
    )

    await watcher._check_once()

    assert controller.enqueued == [chain_id], (
        "RAM parked over the boot-probed ceiling after a reload that "
        "never mentioned ram_ceiling_bytes: the watcher must still "
        f"enforce a ceiling, but it enqueued {controller.enqueued} - "
        "the live snapshot's ceiling is the reload's None hole and "
        "enforcement is silently disabled"
    )
