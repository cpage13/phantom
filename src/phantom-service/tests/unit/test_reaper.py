"""Unit tests for phantom.workers.reaper.

Slice 1.D rewrite (plan § 2.3.16). The reaper now operates on the
single persistent store + the mode-selected body store; the
dual-store ``(memory, disk)`` round-robin is gone. The
``after_seconds`` force-persist branch is also gone — retry-linger is
the PersistController's job.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path
from unittest.mock import MagicMock
from uuid import uuid4

import pytest
from phantom.config.settings import InstanceCfg, RetentionCfg, RouteCfg
from phantom.instances.context import InstanceContext
from phantom.models.upload import UploadRow
from phantom.storage import (
    FileBodyStore,
    RamBodyStore,
    SqliteTokenCache,
    SqliteUploadStore,
)
from phantom.storage.hybrid_body_store import HybridBodyStore
from phantom.workers.reaper import Reaper, RetentionConfigError

from .conftest import make_snapshot, snapshot_thunk, track_instance


def _full_retention(**overrides: int) -> RetentionCfg:
    """Build a RetentionCfg with every probe-fillable field populated."""
    defaults: dict[str, int] = {
        "succeeded_metadata_seconds": 300,
        "failed_body_seconds": 14 * 86_400,
        "auth_expired_body_seconds": 60 * 86_400,
        "stored_body_seconds": 60 * 86_400,
    }
    defaults.update(overrides)
    return RetentionCfg(**defaults)  # type: ignore[arg-type]


async def _build(tmp_path: Path, *, retention: RetentionCfg | None = None) -> InstanceContext:
    """Wire one InstanceContext with the given retention exposed via the snapshot thunk.

    Phase 1 Slice 1.E shape: single persistent SQLite + mode-selected
    body store. The reaper operates on ``instance.store`` +
    ``instance.body_store``.
    """
    store = SqliteUploadStore(str(tmp_path / "uploads.db"))
    ram = RamBodyStore()
    fbs = FileBodyStore(tmp_path / "bodies")
    body_store = HybridBodyStore(ram=ram, disk=fbs)
    tokens = SqliteTokenCache(str(tmp_path / "tokens.db"))
    await store.start()
    await body_store.start()
    await tokens.start()
    cfg = InstanceCfg(
        id="primary",
        host_prefixes=["*"],
        data_dir="primary",
        routes=[RouteCfg(name="r", hosts=["*"], auth_mode="none")],
    )
    snapshot = make_snapshot(retention=retention if retention is not None else _full_retention())
    instance = InstanceContext(
        cfg=cfg,
        store=store,
        ram_body_store=ram,
        file_body_store=fbs,
        body_store=body_store,
        persist_controller=None,
        token_cache=tokens,
        minter=None,
        retry_strategy=MagicMock(),
        upstream_client=MagicMock(),
        executor=MagicMock(),
        saturation=MagicMock(),
        codec_factory=MagicMock(),
        current_settings=snapshot_thunk(snapshot),
    )
    return track_instance(instance)


def _row(
    chain_id,
    *,
    state: str,
    updated_at: datetime,
    body_location: str = "ram",
) -> UploadRow:
    """Build a minimal UploadRow under the new schema."""
    now = datetime.now(tz=UTC)
    return UploadRow.model_validate(
        {
            "chain_id": chain_id,
            "instance_id": "primary",
            "group_id": chain_id,
            "multifile_id": chain_id,
            "send_order": 0,
            "route_name": "r",
            "state": state,
            "body_location": body_location,
            "received_at": now,
            "updated_at": updated_at,
            "endpoint": "e",
            "uid": "u",
            "chain_envelope_json": "{}",
            "idempotency_key": "k",
            "capture_reexecution_active": False,
        },
    )


@pytest.mark.asyncio
async def test_succeeded_metadata_window(tmp_path: Path) -> None:
    """Succeeded rows are reaped once metadata window elapses."""
    instance = await _build(
        tmp_path,
        retention=_full_retention(
            succeeded_metadata_seconds=1,
            succeeded_body_seconds=0,
        ),
    )
    old = datetime.now(tz=UTC) - timedelta(seconds=10_000)
    chain_id = uuid4()
    await instance.store.insert(_row(chain_id, state="succeeded", updated_at=old))
    reaper = Reaper(instances=[instance])
    await reaper._sweep_once()
    assert await instance.store.get(chain_id) is None


@pytest.mark.asyncio
async def test_succeeded_body_dropped_immediately(tmp_path: Path) -> None:
    """succeeded_body_seconds=0 deletes the body without touching metadata yet."""
    instance = await _build(
        tmp_path,
        retention=_full_retention(
            succeeded_metadata_seconds=10_000,
            succeeded_body_seconds=0,
        ),
    )
    old = datetime.now(tz=UTC) - timedelta(seconds=10)
    chain_id = uuid4()
    await instance.store.insert(
        _row(chain_id, state="succeeded", updated_at=old, body_location="file"),
    )
    await instance.file_body_store.put(chain_id, {"body": b"x"})
    reaper = Reaper(instances=[instance])
    await reaper._sweep_once()
    surviving = await instance.store.get(chain_id)
    assert surviving is not None
    assert chain_id not in await instance.file_body_store.list_chain_ids()


@pytest.mark.asyncio
async def test_idempotency_index_cleanup(tmp_path: Path) -> None:
    """Reaper drops idempotency rows referencing deleted uploads.

    Slice 1.D: the dual-store carve-out is gone. The single store's
    ``cleanup_idempotency_index`` drops every index row whose linked
    upload is absent from this store's ``uploads`` table.
    """
    instance = await _build(tmp_path)
    chain_id = uuid4()
    await instance.store.claim_idempotency("k1", chain_id)
    reaper = Reaper(instances=[instance])
    await reaper._sweep_once()
    deleted = await instance.store.cleanup_idempotency_index()
    # After our sweep already ran, the row is gone; second cleanup deletes 0.
    assert deleted == 0


@pytest.mark.asyncio
async def test_unresolved_metadata_window_raises_typed_error(tmp_path: Path) -> None:
    """D5: a None metadata window aborts the sweep with RetentionConfigError.

    ``model_copy(update=...)`` bypasses validation, simulating a config
    that escaped resolution. The raise must survive ``python -O``, which
    the asserts it replaced did not.
    """
    broken = _full_retention().model_copy(update={"succeeded_metadata_seconds": None})
    instance = await _build(tmp_path, retention=broken)
    reaper = Reaper(instances=[instance])
    with pytest.raises(RetentionConfigError, match="metadata window for state 'succeeded'"):
        await reaper._sweep_once()


@pytest.mark.asyncio
async def test_unresolved_body_window_raises_typed_error(tmp_path: Path) -> None:
    """D5: a None body window aborts the sweep with RetentionConfigError."""
    broken = _full_retention().model_copy(update={"failed_body_seconds": None})
    instance = await _build(tmp_path, retention=broken)
    reaper = Reaper(instances=[instance])
    with pytest.raises(RetentionConfigError, match="body window for state 'failed'"):
        await reaper._sweep_once()
