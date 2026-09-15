"""The last boot stage is classified, and the build loop is torn down.

Two boot-ladder holes in ``phantom.app``'s lifespan, both of which turn a
single recoverable fault into a supervisor crash loop that leaks SQLite
connections once per cycle:

S3-1. Body-store + upstream-client construction was the one per-instance
boot stage with no typed fault classification and no store cleanup. A
stray FILE at ``<data_root>/bodies`` passes the integrity gate, the backup
reconcile, the mode guard and the schema gate (all four key off the DB),
then makes ``FileBodyStore.start()`` raise ``OSError`` out of the
lifespan. uvicorn aborts, the orchestrator restarts, and each cycle
abandons the upload store, the token cache and the credential store.
ADR-025's "always boot, degrade loudly, never crash-loop" posture and
ADR-027's typed ``BootOutcome`` fold both say this must be a
``DegradeReason``, not an escape.

S3-2. The per-instance build loop sat OUTSIDE the ``try``/``finally`` that
tears instances down, so a boot fault that deliberately PROPAGATES rather
than degrades (``IntegrityFailClosedError``,
``ConfigCredentialError``, ``RecoveryLockError``) abandoned every
already-built instance: its three SQLite connections, its httpx client
and its started body store.

Both tests drive the REAL ``create_app`` lifespan.
"""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import TYPE_CHECKING

import pytest
from phantom.app import create_app
from phantom.config.settings import (
    BodyStoreCfg,
    InstanceCfg,
    RouteCfg,
    Settings,
    StorageCfg,
)
from phantom.runtime.startup_checks import DegradeReason
from phantom.storage import SqliteUploadStore
from phantom.workers.recovery import RecoveryLockError

if TYPE_CHECKING:
    from phantom.instances.context import InstanceContext
    from phantom.storage.interface import BodyStore

# Bound every lifespan entry so a hang fails fast rather than wedging the
# lane (mirrors test_startup_guards_prod_path.py's _LIFESPAN_TIMEOUT_SECONDS).
_LIFESPAN_TIMEOUT_SECONDS: float = 30.0
# A small RAM ceiling keeps the hybrid wiring honest without reserving real
# memory; any non-None value satisfies the resolved-defaults invariant.
_RAM_CEILING_BYTES: int = 1024 * 1024


def _instance(instance_id: str, data_dir: str) -> InstanceCfg:
    """Build a minimal single-route instance whose host prefix is unique."""
    hosts = [f"{instance_id}.example.com"]
    return InstanceCfg(
        id=instance_id,
        host_prefixes=hosts,
        data_dir=data_dir,
        routes=[RouteCfg(name="files", hosts=hosts, auth_mode="phantom_bearer")],
    )


def _settings(data_root: Path, instances: list[InstanceCfg]) -> Settings:
    """Build production-shaped hybrid-mode settings rooted at ``data_root``."""
    return Settings(
        storage=StorageCfg(
            data_dir=str(data_root),
            body_store=BodyStoreCfg(mode="hybrid", ram_ceiling_bytes=_RAM_CEILING_BYTES),
        ),
        instances=instances,
    )


def _track_upload_store_stops(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    """Record every ``SqliteUploadStore.stop`` call, delegating to the real one.

    The upload store is the FIRST connection a per-instance boot opens, so
    "was it closed" is the sharpest observable for a connection leak on a
    path that never reaches ``_stop_instance``.

    Args:
        monkeypatch: The active pytest monkeypatch fixture.

    Returns:
        A list that accrues one DB path string per ``stop()`` call.
    """
    stopped: list[str] = []
    real_stop = SqliteUploadStore.stop

    async def _recording_stop(self: SqliteUploadStore) -> None:
        stopped.append(str(self._db_path))  # private read: test observability only
        await real_stop(self)

    monkeypatch.setattr(SqliteUploadStore, "stop", _recording_stop)
    return stopped


async def test_stray_file_at_bodies_degrades_instead_of_crashing_the_boot(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """S3-1. A file where ``bodies/`` belongs degrades the instance, not the process.

    Objective: prove the body-store stage is classified like every stage
    above it. With ``<data_root>/bodies`` occupied by a regular file,
    ``FileBodyStore.start()`` raises ``FileExistsError`` (an ``OSError``).

    Expected outcome: the lifespan ENTERS (no raise), the instance appears
    in ``app.state.degraded_boot`` with reason
    ``BODY_STORE_UNAVAILABLE``, it is absent from ``app.state.instances``,
    and the upload store opened earlier in the same boot was CLOSED before
    the degrade returned (no leaked connection per restart cycle).

    Without the fix the ``OSError`` escapes the lifespan (the ``async
    with`` raises) and nothing is closed.
    """
    stopped = _track_upload_store_stops(monkeypatch)
    instance_root = tmp_path / "primary"
    instance_root.mkdir(parents=True)
    # The stray artifact: a FILE exactly where the body tree must be built.
    (instance_root / "bodies").write_text("not a directory")

    app = create_app(_settings(tmp_path, [_instance("primary", "primary")]))
    async with asyncio.timeout(_LIFESPAN_TIMEOUT_SECONDS), app.router.lifespan_context(app):
        degraded = app.state.degraded_boot
        assert [d.instance_id for d in degraded] == ["primary"], (
            "a body-store fault must degrade the ONE instance, not crash the boot"
        )
        assert degraded[0].reason is DegradeReason.BODY_STORE_UNAVAILABLE, degraded[0]
        assert "bodies" in degraded[0].detail, degraded[0].detail
        assert app.state.instances == [], "a degraded instance must build no context"

    assert any(path.endswith("uploads.db") for path in stopped), (
        "the upload store opened before the body-store fault must be closed "
        f"before the degrade returns; stop() calls seen: {stopped}"
    )


async def test_a_propagating_fault_tears_down_already_built_instances(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """S3-2. A later instance's non-degrade fault closes the earlier instance.

    Objective: prove the build loop runs INSIDE the teardown guard.
    ``RecoveryLockError`` (raised by the recovery sweep when the DB write
    lock is held past the boot budget) is one of the three documented
    faults that propagate rather than degrade. It is injected on the
    SECOND instance, after the first has been fully built and recovered.

    Expected outcome: the error still propagates out of the lifespan (the
    posture is unchanged - a real fault must crash loudly), and BOTH
    instances that reached the context list have their upload store
    closed on the way out.

    Without the fix the loop sits outside the ``try``/``finally``, so
    ``_stop_instance`` never runs and every connection opened by this boot
    is abandoned - once per 120-second supervisor restart cycle.
    """
    stopped = _track_upload_store_stops(monkeypatch)
    calls = {"n": 0}
    lock_error = RecoveryLockError("recovery could not acquire the write lock")

    async def _failing_recovery(store: object, body_store: BodyStore) -> None:
        """Succeed for the first instance, fail for the second."""
        del store, body_store
        calls["n"] += 1
        if calls["n"] >= 2:
            raise lock_error

    monkeypatch.setattr("phantom.app.run_recovery", _failing_recovery)

    app = create_app(_settings(tmp_path, [_instance("alpha", "alpha"), _instance("beta", "beta")]))
    with pytest.raises(RecoveryLockError):
        async with asyncio.timeout(_LIFESPAN_TIMEOUT_SECONDS), app.router.lifespan_context(app):
            pass  # pragma: no cover - lifespan startup must raise

    built: list[InstanceContext] = app.state.instances
    assert [ctx.cfg.id for ctx in built] == ["alpha", "beta"], (
        "both instances must have been built before recovery failed on the second"
    )
    assert len(stopped) == 2, (
        "every instance that reached the context list must be torn down when a "
        f"propagating boot fault escapes the build loop; stop() calls seen: {stopped}"
    )
