"""RamPressureWatcher vs a hot-reloaded ``ram_ceiling_bytes`` (round 6, R6-2).

ADR-013's "Body-store tuning" line is explicit:

    body_store.linger_seconds, body_store.ram_ceiling_bytes,
    body_store.ram_pressure_poll_seconds (read from the live snapshot
    per worker tick).

CONTEXT.md and docs/architecture-intent.md repeat that
``ram_ceiling_bytes`` reloads via SIGHUP / ``POST /v1/admin/reload``.
Two of those three knobs honor the contract:
``ram_pressure_poll_seconds`` (the cadence and the fresh-attempt window)
and ``linger_seconds`` (read by the sender at
``current_settings().body_store.linger_seconds``) are both re-read from
the live snapshot per tick. ``ram_ceiling_bytes`` is the odd one out:
:class:`RamPressureWatcher` captures it ONCE at construction
(``app.py`` passes ``max_bytes=settings.storage.body_store.ram_ceiling_bytes``
from the boot-time ``Settings``) into ``self._max_bytes`` and never
re-reads it. ``_check_once`` gates pressure on ``self._max_bytes`` (the
boot value), and :func:`phantom.runtime.reload.apply_reload` never
touches the watcher, so a reloaded ceiling never reaches the enforcer.

Why it matters: this is the same defect class as R5-2 (codec choice and
retry parameters reloaded the snapshot but never reached the consuming
component). An operator on Pi-class hardware watching RAM climb who
LOWERS ``ram_ceiling_bytes`` via the documented hot-reload surface gets
a 200, and the live snapshot observably carries the tighter ceiling
(``GET /v1/admin/observability/ram_pressure`` reads it straight from
``current_settings()`` and reports the new number), yet the watcher
keeps enforcing the OLD, looser ceiling. RAM is silently NOT bounded to
the operator's new limit. The two observability surfaces even disagree
after the reload: ``/observability/ram_pressure`` reports the new
ceiling while the watcher's own ``ram_ceiling_bytes`` gauge (under
``/observability/gauges``) reports the stale boot value it is actually
enforcing. Silent config no-op on a memory-safety knob.

The repro is a focused unit test because the ceiling-enforcement
decision is an internal worker decision, not wire-visible: the watcher
is constructed with a boot ceiling, its instance's live snapshot is then
swapped to a TIGHTER ceiling (exactly what ``apply_reload`` does to the
snapshot), RAM is parked between the new ceiling and the old, and a
single ``_check_once`` tick must enqueue the oldest RAM body. Under the
defect the tick reads the stale boot ceiling, sees RAM below it, and
enqueues nothing.

The R6-2 fix made ``_check_once`` read
``current_settings().body_store.ram_ceiling_bytes`` (the live snapshot)
for the pressure gate, the gauge emit, and the post-enqueue re-sample,
aligning the third body-store-tuning knob with the other two; the
now-dead ``max_bytes`` and ``poll_interval_seconds`` constructor
parameters were removed outright.
"""

from __future__ import annotations

from datetime import UTC, datetime
from uuid import UUID, uuid4

from phantom.config.settings import BodyStoreCfg
from phantom.instances.snapshot import InstanceSettingsSnapshot
from phantom.models.upload import UploadRow
from phantom.storage.interface import PersistCandidateState
from phantom.workers.ram_pressure import RamPressureWatcher

from .conftest import make_snapshot

# Boot-time ceiling the watcher is constructed with (app.py passes the
# resolved ``ram_ceiling_bytes`` here). 1 MiB.
_BOOT_CEILING_BYTES: int = 1 * 1024 * 1024
# Reloaded (tighter) ceiling the operator pushes via POST /reload. 256
# KiB - well below the boot ceiling and below the parked RAM bytes.
_RELOADED_CEILING_BYTES: int = 256 * 1024
# RAM bytes currently parked: above the reloaded ceiling, below the boot
# ceiling. Under the live ceiling this is pressure; under the boot
# ceiling it is not. 512 KiB.
_PARKED_RAM_BYTES: int = 512 * 1024
# Poll cadence for the watcher; small and irrelevant to the gate (the
# test calls _check_once directly, never run()), but a valid positive.
_POLL_SECONDS: float = 1.0

assert _RELOADED_CEILING_BYTES <= _PARKED_RAM_BYTES < _BOOT_CEILING_BYTES, (
    "test constants must place parked RAM strictly between the reloaded "
    "ceiling (inclusive) and the boot ceiling (exclusive)"
)


class _RecordingPersistController:
    """Records every ``enqueue`` chain_id; otherwise inert.

    Stands in for the real :class:`phantom.workers.persist_controller.PersistController`.
    The watcher only ever ``await``s ``enqueue`` and ignores the return,
    so a coroutine returning ``None`` is a faithful duck-typed stand-in
    without spinning up real stores.
    """

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
    """Upload store exposing exactly one oldest RAM-resident chain.

    ``list_oldest_ram_bodies`` yields the single seeded chain_id;
    ``get_persist_candidate_state`` reports it ``queued`` (state !=
    ``attempting`` so
    the fresh-attempt skip never applies and a healthy ceiling breach
    enqueues it).
    """

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


class _FakeInstance:
    """Minimal instance surface the watcher's ``_check_once`` touches.

    Carries a mutable ``_snapshot`` so the test can swap in a tighter
    ceiling AFTER construction - the exact shape of a hot reload, where
    ``apply_reload`` atomically swaps the snapshot the worker reads via
    ``current_settings()`` on each tick.
    """

    def __init__(
        self,
        *,
        snapshot: InstanceSettingsSnapshot,
        ram_body_store: _StaticRamBodyStore,
        store: _OneCandidateStore,
    ) -> None:
        self._snapshot = snapshot
        self.ram_body_store = ram_body_store
        self.store = store

    def current_settings(self) -> InstanceSettingsSnapshot:
        """Return the live (post-reload) snapshot the worker reads per tick."""
        return self._snapshot


def _queued_ram_row(chain_id: UUID) -> UploadRow:
    """Build a minimal ``queued`` RAM-resident row for the candidate."""
    now = datetime.now(tz=UTC)
    return UploadRow.model_validate(
        {
            "chain_id": chain_id,
            "instance_id": "primary",
            "group_id": uuid4(),
            "multifile_id": None,
            "send_order": 0,
            "route_name": "upstream-files",
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


async def test_reloaded_ram_ceiling_reaches_the_watcher() -> None:
    """A reloaded (tighter) ``ram_ceiling_bytes`` must bound the live tick.

    Build the instance under the BOOT snapshot, construct the watcher
    (so any capture-at-construction regression captures the boot
    ceiling), then swap the live snapshot to a TIGHTER ceiling exactly
    as ``apply_reload`` swaps the snapshot map. Park RAM bytes strictly
    between the reloaded ceiling and the boot ceiling so the breach is
    real under the live ceiling and absent under the boot ceiling. One
    ``_check_once`` tick must observe pressure against the LIVE ceiling
    and enqueue the oldest RAM body. Before the R6-2 fix the tick read
    a constructor-captured boot ceiling, saw RAM below it, and enqueued
    nothing; the fix reads the live snapshot per tick.
    """
    chain_id = uuid4()
    boot_snapshot = make_snapshot(
        body_store=BodyStoreCfg(
            mode="hybrid",
            ram_ceiling_bytes=_BOOT_CEILING_BYTES,
            ram_pressure_poll_seconds=_POLL_SECONDS,
        )
    )
    # The post-reload live snapshot: a tighter ceiling than boot, same
    # mode + poll cadence.
    reloaded_snapshot = make_snapshot(
        body_store=BodyStoreCfg(
            mode="hybrid",
            ram_ceiling_bytes=_RELOADED_CEILING_BYTES,
            ram_pressure_poll_seconds=_POLL_SECONDS,
        )
    )
    instance = _FakeInstance(
        snapshot=boot_snapshot,
        ram_body_store=_StaticRamBodyStore(total=_PARKED_RAM_BYTES),
        store=_OneCandidateStore(chain_id=chain_id, row=_queued_ram_row(chain_id)),
    )
    controller = _RecordingPersistController()
    watcher = RamPressureWatcher(
        instance=instance,  # type: ignore[arg-type]  # duck-typed minimal surface
        persist_controller=controller,  # type: ignore[arg-type]  # recording stand-in
    )

    # The hot reload: the live snapshot the worker reads per tick is
    # atomically replaced with one carrying the tighter ceiling.
    instance._snapshot = reloaded_snapshot

    await watcher._check_once()

    assert controller.enqueued == [chain_id], (
        "watcher must enqueue the oldest RAM body once the LIVE (reloaded) "
        f"ceiling {_RELOADED_CEILING_BYTES} is breached by parked RAM "
        f"{_PARKED_RAM_BYTES}; it enqueued {controller.enqueued} instead, "
        "meaning it is still enforcing the stale boot ceiling "
        f"{_BOOT_CEILING_BYTES}"
    )
