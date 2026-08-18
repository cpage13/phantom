"""Shared test fixtures for phantom unit tests."""

from __future__ import annotations

import inspect
import threading
from collections.abc import AsyncIterator, Callable, Iterator
from datetime import UTC, datetime
from typing import Protocol
from uuid import uuid4

import pytest
from phantom.config.settings import (
    AdminLookupCfg,
    BodyStoreCfg,
    CompressionCfg,
    PersistTriggerCfg,
    RetentionCfg,
    SaturationCfg,
)
from phantom.instances.context import InstanceContext
from phantom.instances.snapshot import InstanceSettingsSnapshot
from phantom.models.upload import UploadRow

# Baseline retention values for tests that just need a populated
# snapshot. Tests that exercise retention directly construct their
# own :class:`RetentionCfg`. These values match the probe-derived
# defaults for a typical mid-tier host; any value satisfying the
# validator's "non-None" invariant works here.
_TEST_RETENTION_DEFAULTS: dict[str, int] = {
    "succeeded_metadata_seconds": 300,
    "failed_body_seconds": 14 * 86_400,
    "auth_expired_body_seconds": 60 * 86_400,
    "stored_body_seconds": 60 * 86_400,
}

# Baseline saturation values for tests that just need a populated
# snapshot. Same convention as above — values match a typical mid-
# tier host but only the "non-None" invariant matters for tests.
_TEST_SATURATION_DEFAULTS: dict[str, int] = {
    "max_in_flight": 100,
    "max_in_flight_bytes": 1_073_741_824,
    "max_disk_bytes": 137_438_953_472,
    "large_body_threshold_bytes": 100 * 1024 * 1024,
    "max_large_in_flight": 4,
}


def make_snapshot(
    *,
    persist_trigger: PersistTriggerCfg | None = None,
    body_store: BodyStoreCfg | None = None,
    retention: RetentionCfg | None = None,
    compression: CompressionCfg | None = None,
    saturation: SaturationCfg | None = None,
    capture_reexecution: bool = False,
    admin_lookup: AdminLookupCfg | None = None,
) -> InstanceSettingsSnapshot:
    """Build a populated :class:`InstanceSettingsSnapshot` for tests.

    Every field defaults to a sensible value so tests that don't care
    about a specific sub-block can call ``make_snapshot()`` without
    arguments. Tests that exercise a hot-reloadable knob pass an explicit
    sub-config; the helper applies the storage M-profile defaults for any
    other profile-fillable retention/saturation fields so the result
    satisfies the validator's non-None invariant.

    Phase 1: removed ``default_tier`` kwarg (subsumed by
    ``body_store.mode``); added ``body_store`` projection. F5 added
    ``admin_lookup``, which moved onto the snapshot when the ``cfg``
    repoint was deleted, so instance builders can wire a binding.
    """
    if persist_trigger is None:
        persist_trigger = PersistTriggerCfg()
    if body_store is None:
        # Default to hybrid + explicit ceiling so the validator doesn't
        # touch the host probe in unit tests.
        body_store = BodyStoreCfg(ram_ceiling_bytes=1_073_741_824)
    if retention is None:
        retention = RetentionCfg(**_TEST_RETENTION_DEFAULTS)  # type: ignore[arg-type]
    if compression is None:
        compression = CompressionCfg()
    if saturation is None:
        saturation = SaturationCfg(**_TEST_SATURATION_DEFAULTS)  # type: ignore[arg-type]
    return InstanceSettingsSnapshot(
        persist_trigger=persist_trigger,
        body_store=body_store,
        retention=retention,
        compression=compression,
        saturation=saturation,
        capture_reexecution=capture_reexecution,
        admin_lookup=admin_lookup,
    )


def snapshot_thunk(
    snapshot: InstanceSettingsSnapshot,
) -> Callable[[], InstanceSettingsSnapshot]:
    """Wrap a static snapshot into the ``current_settings`` thunk shape.

    Used by tests that don't exercise hot reload — the worker calls the
    thunk on every tick and gets the same instance back.
    """
    return lambda: snapshot


# --------------------------------------------------------------------
# Started-component teardown registry (round 5 fix R5-3).
#
# Many unit modules build an InstanceContext (or bare stores) through a
# per-module helper that STARTS persistent components and historically
# never stopped them. Each leaked aiosqlite connection keeps a worker
# thread alive holding futures bound to the test's event loop, which
# pytest-asyncio closes at test end; the leaked thread's eventual
# finalization raises RuntimeError('Event loop is closed') and pytest's
# threadexception machinery pins it on whatever INNOCENT test is
# running, making full-suite runs nondeterministic under load. The
# registry gives every builder a one-line fix: wrap the built object in
# track_instance / track_started, and the autouse fixture below stops
# everything after each test, on the same event loop that started it.
# Every stop() in the storage layer is idempotent, so components a test
# also stops itself are safe to track.
# --------------------------------------------------------------------


class AsyncStoppable(Protocol):
    """Anything with an async ``stop()`` (stores, caches, body stores)."""

    async def stop(self) -> None:
        """Release the component's resources."""
        ...


# Components awaiting post-test teardown; drained LIFO by the autouse
# fixture so composed stores stop before the stores they wrap.
_STARTED_COMPONENTS: list[AsyncStoppable] = []


def track_started[StoppableT: AsyncStoppable](component: StoppableT) -> StoppableT:
    """Register one started component for post-test teardown."""
    _STARTED_COMPONENTS.append(component)
    return component


def track_instance(instance: InstanceContext) -> InstanceContext:
    """Register every started component on a test-built InstanceContext.

    Registered in construction order; the LIFO drain therefore stops
    the composed body store first (HybridBodyStore stops its halves),
    then the halves, then the sqlite-backed token cache and upload
    store whose aiosqlite worker threads are the leak hazard.
    """
    for component in (
        instance.store,
        instance.token_cache,
        instance.file_body_store,
        instance.ram_body_store,
        instance.body_store,
    ):
        _STARTED_COMPONENTS.append(component)
    return instance


# Marker aiosqlite embeds in its worker-thread names (Python names a
# thread "Thread-N (<target name>)" when constructed with a target;
# aiosqlite's target is ``_connection_worker_thread``).
_AIOSQLITE_WORKER_MARKER = "_connection_worker_thread"
# Grace budget for a closing connection's worker to exit after its stop
# future resolved (the worker resolves the future, then returns); a
# leaked thread blocks on its queue forever, so this only smooths the
# legal exit race of a teardown that just stopped a store.
_TRIPWIRE_JOIN_GRACE_SECONDS = 1.0


def _surviving_aiosqlite_workers() -> list[str]:
    """Names of aiosqlite worker threads alive after a join grace."""
    lingering = [
        thread for thread in threading.enumerate() if _AIOSQLITE_WORKER_MARKER in thread.name
    ]
    for thread in lingering:
        thread.join(timeout=_TRIPWIRE_JOIN_GRACE_SECONDS)
    return sorted(
        thread.name for thread in threading.enumerate() if _AIOSQLITE_WORKER_MARKER in thread.name
    )


@pytest.fixture(autouse=True)
async def _stop_started_components() -> AsyncIterator[None]:
    """Stop every tracked component after each test, then trip on leaks (R5-3).

    Builders that never touch a component slot fill it with a plain
    ``MagicMock()`` (its ``stop()`` returns a non-awaitable mock), so
    the drain awaits only genuinely awaitable results.

    The post-drain check is the per-test leak tripwire: every store is
    function-scoped in this suite (no module/session-scoped stores
    exist), so NO aiosqlite worker thread may survive a test. Failing
    here pins the leak on the test that made it, instead of letting the
    leaked worker detonate on an innocent test later (the session-end
    tripwire below cannot see a leak whose detonation already killed
    the thread mid-suite).
    """
    yield
    while _STARTED_COMPONENTS:
        result = _STARTED_COMPONENTS.pop().stop()
        if inspect.isawaitable(result):
            await result
    leaked = _surviving_aiosqlite_workers()
    assert not leaked, (
        f"aiosqlite worker threads leaked by this test: {leaked}; "
        "stop the started SqliteUploadStore/SqliteTokenCache or track it "
        "via the R5-3 teardown registry in tests/unit/conftest.py"
    )


@pytest.fixture(scope="session", autouse=True)
def _aiosqlite_leak_tripwire() -> Iterator[None]:
    """Session-end backstop: no aiosqlite worker thread survives the suite.

    The per-test check in ``_stop_started_components`` pins
    function-scoped leaks on the test that made them; this backstop
    catches anything holding a started store at a wider scope (e.g., a
    future module/session-scoped fixture). A leaked non-daemon worker
    also HANGS the interpreter at exit (threading shutdown joins it
    forever), so failing the session loudly here is strictly cheaper
    than the hang.
    """
    yield
    leaked = _surviving_aiosqlite_workers()
    assert not leaked, (
        f"aiosqlite worker threads leaked past the test session: {leaked}; "
        "a test started a SqliteUploadStore or SqliteTokenCache without "
        "stopping it (track it via the R5-3 teardown registry in "
        "tests/unit/conftest.py)"
    )


@pytest.fixture
def make_upload_row():
    """Factory fixture for building :class:`UploadRow` test objects."""

    def _build(**overrides: object) -> UploadRow:
        now = datetime.now(tz=UTC)
        base: dict[str, object] = {
            "chain_id": uuid4(),
            "instance_id": "primary",
            "group_id": uuid4(),
            "multifile_id": uuid4(),
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
        base.update(overrides)
        return UploadRow.model_validate(base)

    return _build
