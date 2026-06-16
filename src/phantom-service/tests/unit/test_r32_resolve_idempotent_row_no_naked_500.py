"""Proposed regression test for aggressor Round-3 finding R3-2.

Bug in the DEFENDER'S R2 code (G-1 fix, commit 54075c8). When an
``idempotency_index`` entry outlives its ``uploads`` row (the reaper
deletes the upload row at reaper.py:167 and cleans the index later at
reaper.py:183; admin bulk_delete / store.delete never touch the index),
a re-submit under that key collides on the surviving index entry and
``_resolve_existing_idempotent_row`` hits ``assert existing_row is not
None`` -> AssertionError -> naked HTTP 500. This is the SAME anti-pattern
finding D-1 set out to eliminate (every error must carry an ADR-017
envelope), reintroduced in the G-1 resolution helper.

This test opens the window deterministically: insert a row + claim, delete
the uploads row (exactly what reaper.py:167 does) leaving the index entry,
then re-submit with the same key. RED on current code (raises a bare
AssertionError, not a typed ChainAdmissionError). GREEN once the helper
treats "index entry points at a vanished row" as a recoverable condition —
e.g. re-claim a fresh row (this submit becomes the new owner of the key,
202) OR raise a typed ChainAdmissionError that carries the ADR-017
envelope. The bright line: admission must NOT let a bare exception escape.

Belongs under ``src/phantom-service/tests/unit/``.
"""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

import pytest
from phantom.chain.executor import ChainExecutor
from phantom.compression import select_codec
from phantom.config.settings import (
    BodyStoreCfg,
    CompressionCfg,
    InstanceCfg,
    PersistTriggerCfg,
    RouteCfg,
)
from phantom.instances.context import InstanceContext
from phantom.models.chain import ChainEnvelope, ChainStep
from phantom.routes.admission import AdmissionInputs, ChainAdmissionError, admit_chain
from phantom.routing import resolve_route
from phantom.storage import FileBodyStore, RamBodyStore, SqliteTokenCache, SqliteUploadStore
from phantom.storage.hybrid_body_store import HybridBodyStore
from phantom.strategies import FixedIntervalsStrategy
from phantom.transport import UpstreamRequest, UpstreamResponse
from phantom.workers.saturation import SaturationGate

from .conftest import make_snapshot, snapshot_thunk, track_instance


class _FakeUpstream:
    async def start(self) -> None: ...
    async def stop(self) -> None: ...
    async def send(self, _req: UpstreamRequest) -> UpstreamResponse:
        return UpstreamResponse(status=200, body=b"{}")


async def _build_instance(tmp_path: Path) -> InstanceContext:
    store = SqliteUploadStore(str(tmp_path / "uploads.db"))
    ram = RamBodyStore()
    fbs = FileBodyStore(tmp_path / "bodies")
    tokens = SqliteTokenCache(str(tmp_path / "tokens.db"))
    for s in (store, ram, fbs, tokens):
        await s.start()
    cfg = InstanceCfg(
        id="primary",
        host_prefixes=["files.example.com"],
        data_dir="primary",
        routes=[RouteCfg(name="files", hosts=["files.example.com"], auth_mode="phantom_bearer")],
    )
    upstream = _FakeUpstream()
    executor = ChainExecutor(
        token_cache=tokens,
        upstream_client=upstream,
        resolve_route=resolve_route,
        clock=lambda: datetime.now(tz=UTC),
        instance=cfg,
    )

    def codec_factory() -> object:  # type: ignore[type-arg]
        return select_codec(CompressionCfg(algorithm="original"))

    body_store = HybridBodyStore(ram=ram, disk=fbs)
    await body_store.start()
    instance = InstanceContext(
        cfg=cfg,
        store=store,
        ram_body_store=ram,
        file_body_store=fbs,
        body_store=body_store,
        persist_controller=None,
        token_cache=tokens,
        minter=None,
        retry_strategy=FixedIntervalsStrategy([1, 5]),
        upstream_client=upstream,
        executor=executor,
        saturation=SaturationGate(
            max_in_flight=10, max_in_flight_bytes=10_000_000, max_disk_bytes=10_000_000
        ),
        codec_factory=codec_factory,  # type: ignore[arg-type]
        current_settings=snapshot_thunk(
            make_snapshot(
                persist_trigger=PersistTriggerCfg(body_size_threshold_bytes=0),
                body_store=BodyStoreCfg(ram_ceiling_bytes=1_073_741_824),
            )
        ),
    )
    return track_instance(instance)


def _envelope() -> ChainEnvelope:
    return ChainEnvelope(  # type: ignore[call-arg]
        chain_id=uuid4(),
        idempotency_key="k",
        steps=[
            ChainStep(  # type: ignore[call-arg]
                name="step", method="POST", url="https://files.example.com/v2/files"
            )
        ],
    )


def _inputs(envelope, *, body_refs=None, idempotency_header=None):  # type: ignore[no-untyped-def]
    return AdmissionInputs(
        request_id="r-1",
        uid_header="user-1",
        instance_header=None,
        idempotency_header=idempotency_header,
        envelope=envelope,
        body_refs=body_refs or {},
        authorization=None,
        content_encoding=None,
    )


@pytest.mark.asyncio
async def test_idempotency_index_outlives_row_no_naked_exception(tmp_path: Path) -> None:
    """A reaped row whose index entry survives must not crash admission.

    Admission must surface a typed/recoverable outcome (a fresh 202, or a
    typed ChainAdmissionError with an ADR-017 envelope) — never a bare
    AssertionError that becomes a naked HTTP 500.
    """
    instance = await _build_instance(tmp_path)
    key = "reaped-key"

    env1 = _envelope()
    res1 = await admit_chain(
        _inputs(env1, body_refs={"body": b"orig"}, idempotency_header=key), instance
    )
    assert res1.status_code == 202
    original = res1.row.chain_id

    # Open the reaper window: delete the uploads row, leave the index entry.
    await instance.store.delete(original)
    assert await instance.store.get(original) is None

    env2 = _envelope()
    try:
        res2 = await admit_chain(
            _inputs(env2, body_refs={"body": b"orig"}, idempotency_header=key), instance
        )
        # Acceptable: admission re-claimed a fresh row (202) — the key's
        # prior owner is gone, so this submit becomes the new owner.
        assert res2.status_code in (200, 202)
    except ChainAdmissionError:
        # Acceptable: a typed error carries the ADR-017 envelope.
        pass
    except AssertionError as exc:
        raise AssertionError(
            "admission let a bare AssertionError escape on a reaped-row idempotency "
            "collision — naked 500, no ADR-017 envelope (the D-1 anti-pattern)."
        ) from exc
