"""Proposed regression test for aggressor Round-3 finding R3-1.

Bug in the DEFENDER'S R2 code (D-1 + G-1 fix, commit 54075c8). The
chain_id_in_use and idempotency_key_conflict reject paths release the
saturation slot TWICE: once explicitly before the ``raise`` (admission.py
line 486) and once again in the outer ``except BaseException`` clause
(line 561) that the raise unwinds into. The 200-replay path RETURNS, so
it releases once (correct) — only the two raise paths double-release.

``SaturationGate.release`` clamps with ``max(0, ...)``, so on an idle
gate the double-release is invisible (floors at 0). But the gate is a
shared concurrent counter: with OTHER rows legitimately in flight, the
extra release steals a slot / bytes that were never admitted for the
rejected request, so the gate under-counts and admits beyond
``max_in_flight``.

This test keeps a live row in flight, then triggers each reject path and
asserts the in-flight accounting returns to EXACTLY the live-row baseline
(the rejected request's own admit is cleanly undone, nothing more). RED
on current code (counts drop below baseline). GREEN once the explicit
release before the raise is removed (let the except clause own it), OR
the conflict paths return a typed refusal instead of raising into the
catch-all.

Belongs under ``src/phantom-service/tests/unit/`` (no E2E stack needed). Mirrors
the existing ``test_admit_chain_idempotency_collision_releases_saturation``
which only covers the 200-replay path.
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
async def test_chain_id_conflict_does_not_double_release(tmp_path: Path) -> None:
    """D-1 reject path must release the slot exactly once (not twice).

    With a live row in flight, a chain_id_in_use reject must restore the
    in-flight counts to the live-row baseline — not below it.
    """
    sat = SaturationGate(
        max_in_flight=10, max_in_flight_bytes=10_000_000, max_disk_bytes=10_000_000
    )
    instance = await _build_instance(tmp_path, saturation=sat)

    env0 = _envelope()
    res0 = await admit_chain(_inputs(env0, body_refs={"body": b"live-row-body"}), instance)
    assert res0.status_code == 202
    baseline_rows = sat.in_flight
    baseline_bytes = sat.in_flight_bytes

    env_dup = _envelope()
    env_dup.chain_id = env0.chain_id
    with pytest.raises(ChainAdmissionError) as exc:
        await admit_chain(
            _inputs(env_dup, body_refs={"body": b"x"}, idempotency_header="fresh"), instance
        )
    assert exc.value.code == "chain_id_in_use"

    assert sat.in_flight == baseline_rows, (
        f"chain_id reject double-released: in_flight {sat.in_flight} < baseline "
        f"{baseline_rows}; the live row's slot was stolen."
    )
    assert sat.in_flight_bytes == baseline_bytes, (
        f"chain_id reject double-released bytes: {sat.in_flight_bytes} != baseline "
        f"{baseline_bytes}; the live row's bytes were stolen."
    )


@pytest.mark.asyncio
async def test_idempotency_conflict_does_not_double_release(tmp_path: Path) -> None:
    """G-1 reject path must release the slot exactly once (not twice).

    With a live row in flight under key K, an idempotency_key_conflict
    reject must restore the in-flight counts to the live-row baseline.
    """
    sat = SaturationGate(
        max_in_flight=10, max_in_flight_bytes=10_000_000, max_disk_bytes=10_000_000
    )
    instance = await _build_instance(tmp_path, saturation=sat)

    env1 = _envelope()
    res1 = await admit_chain(
        _inputs(env1, body_refs={"body": b"body-A"}, idempotency_header="K"), instance
    )
    assert res1.status_code == 202
    baseline_rows = sat.in_flight
    baseline_bytes = sat.in_flight_bytes

    env2 = _envelope()
    with pytest.raises(ChainAdmissionError) as exc:
        await admit_chain(
            _inputs(env2, body_refs={"body": b"body-B-different"}, idempotency_header="K"),
            instance,
        )
    assert exc.value.code == "idempotency_key_conflict"

    assert sat.in_flight == baseline_rows, (
        f"idempotency reject double-released: in_flight {sat.in_flight} < baseline "
        f"{baseline_rows}; the live row's slot was stolen."
    )
    assert sat.in_flight_bytes == baseline_bytes, (
        f"idempotency reject double-released bytes: {sat.in_flight_bytes} != baseline "
        f"{baseline_bytes}; the live row's bytes were stolen."
    )
