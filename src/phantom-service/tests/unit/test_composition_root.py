"""Unit tests for the mode-wiring decision table (plan § 2.3.10).

The one shared mode-wiring table is
:func:`phantom.runtime.startup_checks.build_body_store` — both the
production composition root (``app.py``'s lifespan) and (formerly) the
deleted ``compose_and_run`` selected the body-store binding through it.
This file tests that helper DIRECTLY (the seam the production path calls),
so the per-mode contract is pinned at its single source of truth:

* ``hybrid`` → :class:`HybridBodyStore` + a :class:`PersistController`;
* ``all_ram`` → the :class:`RamBodyStore` half, no controller;
* ``all_disk`` → the :class:`FileBodyStore` half, no controller;
* an unknown mode raises ``ValueError`` (the defensive branch behind
  Pydantic's Literal — reachable only by post-construction mutation);
* Pydantic refuses to construct a :class:`BodyStoreCfg` with a bad mode
  in the first place (the operator-facing first line of defense).

Provenance: the per-mode ``Runtime``-shape assertions used to drive the
deleted ``compose_and_run``. The shape *as production wires it* now lives
in ``test_composition.py`` (the ``create_app`` lifespan +
``InstanceContext`` shape); the wiring *decision* lives here, tested at
the helper. (R-2.)
"""

from __future__ import annotations

from pathlib import Path

import pytest
from phantom.config.settings import BodyStoreCfg
from phantom.observability.metrics import MetricsRegistry
from phantom.runtime.startup_checks import build_body_store
from phantom.storage.file_body_store import FileBodyStore
from phantom.storage.hybrid_body_store import HybridBodyStore
from phantom.storage.ram_body_store import RamBodyStore
from phantom.storage.sqlite_store import SqliteUploadStore

pytestmark = pytest.mark.asyncio


async def _started_halves(
    data_root: Path,
) -> tuple[SqliteUploadStore, RamBodyStore, FileBodyStore, MetricsRegistry]:
    """Construct + start the store and both body-store halves.

    ``build_body_store`` takes already-started halves (production keeps
    both on the InstanceContext regardless of mode), so the caller starts
    them exactly as ``_build_instance_context`` does.
    """
    data_root.mkdir(parents=True, exist_ok=True)
    registry = MetricsRegistry()
    store = SqliteUploadStore(str(data_root / "uploads.db"), metrics_registry=registry)
    ram = RamBodyStore()
    file_bs = FileBodyStore(data_root / "bodies", shard_prefix_chars=2)
    await store.start()
    await ram.start()
    await file_bs.start()
    return store, ram, file_bs, registry


async def test_hybrid_wires_hybrid_store_and_persist_controller(tmp_path: Path) -> None:
    """``hybrid`` → HybridBodyStore + a PersistController."""
    store, ram, file_bs, registry = await _started_halves(tmp_path)
    try:
        body_store, controller = await build_body_store(
            mode="hybrid",
            ram_body_store=ram,
            file_body_store=file_bs,
            store=store,
            metrics_registry=registry,
        )
        assert isinstance(body_store, HybridBodyStore)
        assert controller is not None
    finally:
        await store.stop()


async def test_all_ram_wires_ram_half_no_controller(tmp_path: Path) -> None:
    """``all_ram`` → the RamBodyStore half, no PersistController target."""
    store, ram, file_bs, registry = await _started_halves(tmp_path)
    try:
        body_store, controller = await build_body_store(
            mode="all_ram",
            ram_body_store=ram,
            file_body_store=file_bs,
            store=store,
            metrics_registry=registry,
        )
        assert body_store is ram
        assert controller is None
    finally:
        await store.stop()


async def test_all_disk_wires_file_half_no_controller(tmp_path: Path) -> None:
    """``all_disk`` → the FileBodyStore half, no PersistController source."""
    store, ram, file_bs, registry = await _started_halves(tmp_path)
    try:
        body_store, controller = await build_body_store(
            mode="all_disk",
            ram_body_store=ram,
            file_body_store=file_bs,
            store=store,
            metrics_registry=registry,
        )
        assert body_store is file_bs
        assert controller is None
    finally:
        await store.stop()


async def test_unknown_mode_raises(tmp_path: Path) -> None:
    """The defensive branch behind Pydantic's Literal raises ValueError.

    Real operators cannot reach this (Pydantic rejects the value at
    construction — see ``test_pydantic_rejects_unknown_mode``); the
    ``raise ValueError`` is the documented contract for a post-construction
    mutation that bypasses the Literal.
    """
    store, ram, file_bs, registry = await _started_halves(tmp_path)
    try:
        with pytest.raises(ValueError, match=r"Unknown body_store\.mode"):
            await build_body_store(
                mode="frobozz",  # type: ignore[arg-type]
                ram_body_store=ram,
                file_body_store=file_bs,
                store=store,
                metrics_registry=registry,
            )
    finally:
        await store.stop()


async def test_pydantic_rejects_unknown_mode_at_construction() -> None:
    """A typo'd mode is rejected by :class:`BodyStoreCfg` validation.

    The first line of defense — Pydantic refuses to construct a
    :class:`BodyStoreCfg` with anything other than the three Literal
    values, so an operator-typed YAML hitting an unknown mode fails at
    Settings load time (before the composition root ever sees it).
    """
    from pydantic import ValidationError

    with pytest.raises(ValidationError):
        BodyStoreCfg(mode="frobozz")  # type: ignore[arg-type]
