"""Proposed regression test for aggressor Round-3 finding R3-8.

`in_flight_bytes` leaks on every compressed upload. Admission credits the
gate the RAW body size (`declared_bytes`, admission.py:308 -> admit at
:324) but the row persists the COMPRESSED size (`body_size_bytes =
stored_body_size`, admission.py:446) and the sender releases that
compressed size on every terminal transition (sender.py:282/369/440/471:
`release(row.body_size_bytes)`). With storage compression (zstd/gzip —
documented modes) `stored < raw`, so each upload leaks `raw - stored`
bytes from `_in_flight_bytes`, which monotonically climbs until the byte
cap wedges all admissions.

This test admits a highly-compressible body through the REAL `admit_chain`
with a zstd codec, then performs the EXACT release the sender does on a
terminal transition (`saturation.release(row.body_size_bytes)`), and
asserts the gate's `in_flight_bytes` returns to 0. RED on current code
(leaks the raw-minus-compressed delta). GREEN once admit and release
account the same byte unit.

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
from phantom.routes.admission import AdmissionInputs, admit_chain
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


async def _build_instance(tmp_path: Path, *, saturation: SaturationGate) -> InstanceContext:
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

    # zstd codec so stored bytes < raw bytes for a compressible body.
    def codec_factory() -> object:  # type: ignore[type-arg]
        return select_codec(CompressionCfg(algorithm="zstd"))

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
        saturation=saturation,
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


def _inputs(envelope, *, body_refs):  # type: ignore[no-untyped-def]
    return AdmissionInputs(
        request_id="r-1",
        uid_header="user-1",
        instance_header=None,
        idempotency_header=None,
        envelope=envelope,
        body_refs=body_refs,
        authorization=None,
        content_encoding=None,
    )


@pytest.mark.asyncio
async def test_in_flight_bytes_returns_to_zero_after_compressed_upload(tmp_path: Path) -> None:
    """A compressed upload must not leak in_flight_bytes on terminal release.

    Admission credits the RAW size; the sender releases the row's persisted
    (compressed) body_size_bytes. With zstd the two differ, so the gate's
    byte counter must still net to zero — otherwise it leaks every upload.
    """
    sat = SaturationGate(
        max_in_flight=10, max_in_flight_bytes=10_000_000, max_disk_bytes=10_000_000
    )
    instance = await _build_instance(tmp_path, saturation=sat)

    # Highly compressible body so stored << raw under zstd.
    body = b"A" * 50_000
    env = _envelope()
    res = await admit_chain(_inputs(env, body_refs={"body": body}), instance)
    assert res.status_code == 202
    row = res.row
    assert row.storage_encoding == "zstd", "premise: body stored compressed"
    assert row.body_size_bytes < len(body), "premise: compressed size < raw size"

    # Simulate the sender's terminal release — the EXACT call sender.py makes
    # on a terminal transition (succeeded/failed/stored/auth_expired).
    await instance.saturation.release(row.body_size_bytes)

    assert sat.in_flight == 0, f"row-count leak: in_flight={sat.in_flight}"
    assert sat.in_flight_bytes == 0, (
        f"in_flight_bytes LEAKED {sat.in_flight_bytes} bytes after a compressed upload "
        f"terminated: admit credited raw {len(body)} bytes but release subtracted compressed "
        f"{row.body_size_bytes} bytes. The gate's byte cap erodes by (raw - compressed) per "
        f"upload and eventually wedges all admissions."
    )
