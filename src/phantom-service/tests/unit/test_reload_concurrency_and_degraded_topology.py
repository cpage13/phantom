"""Two reload faults: an interleaved swap, and a degraded instance misread.

S3-5. ``apply_reload`` held no lock across the snapshot swap and the
post-swap pushes. ``SettingsHolder.replace`` locks only its own dict swap,
and both triggers can fire concurrently (the SIGHUP handler tracks several
in-flight reload tasks by design; ``POST /v1/admin/reload`` is an ordinary
handler). Two reloads could therefore interleave and leave live state
split across generations, uncorrected until the next reload. Measured
here: the loser's post-swap pushes run LAST, so the retry strategy every
scheduling decision reads is built from the reload whose snapshots were
overwritten, while every per-tick snapshot reader sees the other. (The
saturation cap happens to escape, but only by accident: its push re-reads
the live holder, so it lands on whatever generation is installed at that
instant rather than on the one being applied.) The two generations need
not differ in YAML: ``reload_from_yaml`` re-probes the host, so
``max_disk_bytes`` and ``ram_ceiling_bytes`` legitimately differ between
two reads of one file.

S3-7. The "instance added by the YAML" arm compared the new snapshot set
against the LIVE CONTEXT list. An instance that booted DEGRADED (ADR-027:
a classified storage fault, so no context and no dispatcher entry) is
configured but has no context, so every reload called it newly added:
a warning pointing the operator at a topology change instead of at the
storage fault, its holder entry deleted, and a 200 body that silently
omitted a configured instance.

Both tests drive the REAL ``apply_reload`` over a real
:class:`SettingsHolder`; only the storage-side members of the context are
mocks, since ``apply_reload`` never touches them.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import timedelta
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import yaml
from phantom.config.settings import Settings
from phantom.instances.settings_holder import SettingsHolder
from phantom.instances.snapshot import InstanceSettingsSnapshot, _build_snapshot
from phantom.runtime.reload import apply_reload
from phantom.strategies.interface import UploadStrategy
from phantom.workers.saturation import SaturationGate

_HEALTHY_INSTANCE_ID = "alpha"
# The instance that booted degraded: configured, so the holder carries its
# snapshot, but no InstanceContext was ever built for it.
_DEGRADED_INSTANCE_ID = "beta"

# Two clearly distinguishable generations. The values are arbitrary; all
# that matters is that a reader can tell which reload a piece of live state
# came from. The retry base doubles as the generation marker on the live
# strategy slot: with jitter pinned to zero, the first scheduled delay IS
# the base, so the strategy reports which reload built it.
_GENERATION_A_MAX_IN_FLIGHT = 32
_GENERATION_B_MAX_IN_FLIGHT = 128
_GENERATION_A_RETRY_BASE_SECONDS = 7.0
_GENERATION_B_RETRY_BASE_SECONDS = 23.0


def _instance_block(instance_id: str) -> dict[str, Any]:
    """One YAML instance block with a unique host prefix and data_dir."""
    host = f"{instance_id}.example.com"
    return {
        "id": instance_id,
        "host_prefixes": [host],
        "data_dir": instance_id,
        "routes": [{"name": "files", "hosts": [host], "auth_mode": "phantom_bearer"}],
    }


def _yaml_payload(
    data_dir: Path,
    instance_ids: list[str],
    max_in_flight: int,
    retry_base_seconds: float = _GENERATION_A_RETRY_BASE_SECONDS,
) -> dict[str, Any]:
    """A probe-reliant config stamped with one generation's marker values."""
    return {
        "storage": {"data_dir": str(data_dir)},
        "saturation": {"max_in_flight": max_in_flight},
        "retry": {
            "default_strategy": {
                "type": "exponential_backoff",
                "base_seconds": retry_base_seconds,
                "factor": 2.0,
                "cap_seconds": 1000.0,
                "jitter": 0.0,
                "max_attempts": -1,
                "max_duration_seconds": 86_400,
            }
        },
        "instances": [_instance_block(i) for i in instance_ids],
    }


def _first_delay_seconds(strategy: UploadStrategy) -> float:
    """Return the strategy's first scheduled delay, its generation marker."""
    delay = strategy.schedule_next_attempt(
        attempts=0,
        since_received=timedelta(seconds=0),
        last_error=None,
        route_name="files",
    )
    return delay.total_seconds()


def _context_for(cfg: Any, saturation: Any) -> Any:
    """Build the minimal context ``apply_reload`` touches, with a real gate."""
    ctx = MagicMock()
    ctx.cfg = cfg
    ctx.token_cache = MagicMock()
    ctx.saturation = saturation
    return ctx


@contextmanager
def _captured_reload_warnings() -> Iterator[list[logging.LogRecord]]:
    """Capture ``phantom.runtime.reload`` WARNINGs, hermetically.

    Attaches a handler to the emitting logger and pins ITS level, rather
    than relying on propagation to a root handler: ``configure_logging``
    clears the root handlers and sets a level on the ``phantom`` loggers
    themselves, so a caplog-based capture in this process silently sees
    nothing once any earlier test has built an app. The handler and the
    previous level are restored on exit so the capture leaks nothing into
    the next test.

    Yields:
        The list that accrues every record the reload logger emits.
    """
    records: list[logging.LogRecord] = []

    class _ListHandler(logging.Handler):
        def emit(self, record: logging.LogRecord) -> None:
            records.append(record)

    handler = _ListHandler()
    reload_logger = logging.getLogger("phantom.runtime.reload")
    previous_level = reload_logger.level
    reload_logger.addHandler(handler)
    reload_logger.setLevel(logging.WARNING)
    try:
        yield records
    finally:
        reload_logger.removeHandler(handler)
        reload_logger.setLevel(previous_level)


async def test_two_concurrent_reloads_land_on_one_generation(tmp_path: Path) -> None:
    """S3-5. Interleaved reloads must not split the live state across generations.

    Objective: force the exact interleave. Reload A swaps its snapshot map
    and is then suspended before its post-swap pushes; reload B is released
    and runs as far as it can get. The invariant under test is that when
    both have finished, the saturation gate's live cap and the holder's
    live snapshot describe the SAME reload.

    Expected outcome: every piece of live state describes the LAST reload
    to run. Unserialised, B installs its snapshots and finishes, then A
    wakes and pushes its own generation over the top, so the sender
    schedules retries on A's base while every snapshot reader is told B -
    and nothing corrects it until the next reload.
    """
    data_dir = tmp_path / "data"
    path_a = tmp_path / "phantom-a.yaml"
    path_b = tmp_path / "phantom-b.yaml"
    path_a.write_text(
        yaml.safe_dump(
            _yaml_payload(
                data_dir,
                [_HEALTHY_INSTANCE_ID],
                _GENERATION_A_MAX_IN_FLIGHT,
                _GENERATION_A_RETRY_BASE_SECONDS,
            )
        )
    )
    path_b.write_text(
        yaml.safe_dump(
            _yaml_payload(
                data_dir,
                [_HEALTHY_INSTANCE_ID],
                _GENERATION_B_MAX_IN_FLIGHT,
                _GENERATION_B_RETRY_BASE_SECONDS,
            )
        )
    )

    boot = Settings.reload_from_yaml(path_a)
    cfg = boot.instances[0]
    boot_snapshot = _build_snapshot(boot, cfg)
    holder = SettingsHolder({cfg.id: boot_snapshot})
    sat = boot_snapshot.saturation
    assert sat.max_in_flight is not None
    assert sat.max_in_flight_bytes is not None
    assert sat.max_disk_bytes is not None
    assert sat.large_body_threshold_bytes is not None
    assert sat.max_large_in_flight is not None
    gate = SaturationGate(
        max_in_flight=sat.max_in_flight,
        max_in_flight_bytes=sat.max_in_flight_bytes,
        max_disk_bytes=sat.max_disk_bytes,
        large_body_threshold_bytes=sat.large_body_threshold_bytes,
        max_large_in_flight=sat.max_large_in_flight,
    )
    ctx = _context_for(cfg, gate)

    # The interleave harness. The FIRST reload to reach the swap parks
    # there until the second reload has been given the loop; whether the
    # second one can then finish is precisely what serialisation decides.
    second_reload_entered = asyncio.Event()
    real_replace = holder.replace
    swaps = {"n": 0}

    async def _replace_then_yield(snapshots: dict[str, InstanceSettingsSnapshot]) -> None:
        swaps["n"] += 1
        await real_replace(snapshots)
        if swaps["n"] == 1:
            await second_reload_entered.wait()
            await asyncio.sleep(0)

    holder.replace = _replace_then_yield  # type: ignore[method-assign]

    async def _second_reload() -> None:
        second_reload_entered.set()
        await apply_reload(holder, path_b, [ctx])

    async with asyncio.TaskGroup() as tg:
        tg.create_task(apply_reload(holder, path_a, [ctx]), name="reload-a")
        # Let reload A run up to its swap and park before B is scheduled.
        await asyncio.sleep(0)
        tg.create_task(_second_reload(), name="reload-b")

    # Both reloads have finished, so every piece of live state must
    # describe the LAST one to run - B. The snapshot map is the reference
    # (it is what every per-tick reader resolves), and the retry strategy
    # and the gate are the live state the reload pushes alongside it.
    live_cap = holder.snapshot_for(cfg.id).saturation.max_in_flight
    assert live_cap == _GENERATION_B_MAX_IN_FLIGHT, (
        f"the last reload to run must own the live snapshot; got {live_cap}"
    )
    assert _first_delay_seconds(ctx.retry_strategy) == _GENERATION_B_RETRY_BASE_SECONDS, (
        "the retry strategy and the live snapshot must describe ONE reload "
        f"generation: the sender schedules on a "
        f"{_first_delay_seconds(ctx.retry_strategy)}s base while every "
        f"snapshot reader is told generation {live_cap}, and nothing "
        "corrects it until the next reload"
    )
    assert gate.max_in_flight == live_cap, (
        "the gate and the live snapshot must describe ONE reload generation: "
        f"the gate enforces {gate.max_in_flight} while every snapshot reader "
        f"is told {live_cap}"
    )


async def test_a_degraded_instance_is_not_reported_as_added(tmp_path: Path) -> None:
    """S3-7. A configured-but-degraded instance survives a reload intact.

    Objective: the holder carries snapshots for BOTH configured instances
    (the lifespan installs them before any context is built), while only
    the healthy one has an ``InstanceContext``. The YAML is unchanged, so
    this reload adds nothing.

    Expected outcome: no "added instance" warning naming the degraded id,
    its holder entry survives the reload (a deleted entry would turn the
    next live read into a ``KeyError``), and the reload report lists both
    configured instances. Comparing against the live contexts instead
    produces all three faults on EVERY reload, for the lifetime of the
    process.
    """
    data_dir = tmp_path / "data"
    settings_path = tmp_path / "phantom.yaml"
    settings_path.write_text(
        yaml.safe_dump(
            _yaml_payload(
                data_dir,
                [_HEALTHY_INSTANCE_ID, _DEGRADED_INSTANCE_ID],
                _GENERATION_A_MAX_IN_FLIGHT,
            )
        )
    )
    boot = Settings.reload_from_yaml(settings_path)
    # Boot installs one snapshot per CONFIGURED instance - degrade happens
    # later in the ladder and removes no holder entry.
    holder = SettingsHolder({cfg.id: _build_snapshot(boot, cfg) for cfg in boot.instances})
    healthy_cfg = next(cfg for cfg in boot.instances if cfg.id == _HEALTHY_INSTANCE_ID)
    ctx = _context_for(healthy_cfg, MagicMock(update_caps=AsyncMock()))

    with _captured_reload_warnings() as records:
        reloaded = await apply_reload(holder, settings_path, [ctx])

    added_warnings = [
        r.getMessage()
        for r in records
        if r.levelno >= logging.WARNING and "added instance" in r.getMessage()
    ]
    assert not added_warnings, (
        "a degraded instance is configured, not added; warning about it on "
        f"every reload points the operator at the wrong fault: {added_warnings}"
    )
    assert holder.snapshot_for(_DEGRADED_INSTANCE_ID) is not None, (
        "the degraded instance's holder entry must survive the reload"
    )
    assert reloaded == [_HEALTHY_INSTANCE_ID, _DEGRADED_INSTANCE_ID], (
        "the reload report must name every configured instance whose snapshot "
        f"was installed, not just the running ones; got {reloaded}"
    )
