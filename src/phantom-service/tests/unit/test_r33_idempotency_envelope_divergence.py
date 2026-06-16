"""Proposed regression test for aggressor Round-3 finding R3-3.

DESIGN-DECISION FLAVORED (defender must choose the contract). G-1's
`_body_hashes_diverge` (commit 54075c8) decides idempotency
replay-vs-conflict on the per-ref raw `body_hash` ALONE. It ignores the
envelope. So a re-POST with the SAME idempotency key and the SAME body
bytes but a DIFFERENT destination URL replays the first chain (200) and
silently drops the second submit's destination — the producer gets a
success-shaped 200 for an upload Phantom will deliver to the WRONG place.

This is the same silent-data-drop shape G-1 set out to kill, shifted from
"different body" to "same body, different envelope".

This test asserts the REJECT posture (a 4xx, parallel to G-1's
`idempotency_key_conflict`) when the envelope's destination diverges under
a reused key. If the maintainer decides the idempotency binding is body
ONLY (envelope divergence is out of scope), replace the assertion with one
documenting that the first destination wins AND that a WARNING is logged —
but DO NOT leave it a silent 200. RED on current code (the second submit
returns 200 replaying chain-1).

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

SAME_BODY = b"R3-3-identical-body-bytes"
KEY = "shared-key-R3-3"
URL_1 = "https://files.example.com/v2/files"
URL_2 = "https://other.example.com/v2/files"


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
        host_prefixes=["files.example.com", "other.example.com"],
        data_dir="primary",
        routes=[
            RouteCfg(name="files", hosts=["files.example.com"], auth_mode="phantom_bearer"),
            RouteCfg(name="other", hosts=["other.example.com"], auth_mode="phantom_bearer"),
        ],
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


def _envelope(url: str) -> ChainEnvelope:
    return ChainEnvelope(  # type: ignore[call-arg]
        chain_id=uuid4(),
        idempotency_key="k",
        steps=[ChainStep(name="step", method="POST", url=url)],  # type: ignore[call-arg]
    )


def _inputs(envelope):  # type: ignore[no-untyped-def]
    return AdmissionInputs(
        request_id="r-1",
        uid_header="user-1",
        instance_header=None,
        idempotency_header=KEY,
        envelope=envelope,
        body_refs={"body": SAME_BODY},
        authorization=None,
        content_encoding=None,
    )


@pytest.mark.asyncio
async def test_same_key_same_body_different_destination_not_silently_dropped(
    tmp_path: Path,
) -> None:
    """A reused key + same body + different destination must not silently 200-replay.

    The second submit's destination diverges from the first; replaying the
    first chain delivers the bytes to the WRONG place behind a success.
    Assert a 4xx conflict (the reject posture). If the maintainer chooses
    body-only idempotency, swap this for a WARNING-log assertion — but not
    a silent 200.
    """
    instance = await _build_instance(tmp_path)

    res1 = await admit_chain(_inputs(_envelope(URL_1)), instance)
    assert res1.status_code == 202

    with pytest.raises(ChainAdmissionError) as exc:
        await admit_chain(_inputs(_envelope(URL_2)), instance)
    # The exact code is the maintainer's choice; the bright line is "not a
    # silent 200 that drops the divergent destination".
    assert "idempotency" in exc.value.code or "conflict" in exc.value.code
