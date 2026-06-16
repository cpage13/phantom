"""Unit tests for :mod:`phantom.routes.admission`.

Phase 1 Slice 1.E rewrite (plan § 2.3.17 / § 2.3.21).

Covers the post-Slice-1.E admission flow: saturation gate, codec +
body-hash, body_store.put, single atomic
insert_with_idempotency_claim transaction (closes H7), and the
hybrid-mode size-threshold immediate-persist hook. The pre-1.E
``persist_trigger_override`` / disk-tier-on-receipt fixture is gone
with the schema collapse (F-P1-B).
"""

from __future__ import annotations

import hashlib
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import MagicMock
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
from phantom.routes.admission import (
    AdmissionInputs,
    ChainAdmissionError,
    admit_chain,
)
from phantom.routing import resolve_route
from phantom.storage import (
    FileBodyStore,
    RamBodyStore,
    SqliteTokenCache,
    SqliteUploadStore,
)
from phantom.strategies import FixedIntervalsStrategy
from phantom.transport import UpstreamRequest, UpstreamResponse
from phantom.workers.saturation import (
    AdmissionRefusedDiskPressure,
    AdmissionRefusedSaturation,
    SaturationGate,
)

from .conftest import make_snapshot, snapshot_thunk, track_instance


class _FakeUpstream:
    async def start(self) -> None:
        pass

    async def stop(self) -> None:
        pass

    async def send(self, _req: UpstreamRequest) -> UpstreamResponse:
        return UpstreamResponse(status=200, body=b"{}")


async def _build_instance(
    tmp_path: Path,
    *,
    persist_trigger: PersistTriggerCfg | None = None,
    body_store_cfg: BodyStoreCfg | None = None,
    saturation: SaturationGate | None = None,
) -> InstanceContext:
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
    if saturation is None:
        saturation = SaturationGate(
            max_in_flight=10, max_in_flight_bytes=10_000_000, max_disk_bytes=10_000_000
        )
    resolved_persist_trigger = persist_trigger or PersistTriggerCfg(body_size_threshold_bytes=0)
    resolved_body_store_cfg = body_store_cfg or BodyStoreCfg(ram_ceiling_bytes=1_073_741_824)

    def codec_factory() -> object:  # type: ignore[type-arg]
        return select_codec(CompressionCfg(algorithm="original"))

    from phantom.storage.hybrid_body_store import HybridBodyStore

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
                persist_trigger=resolved_persist_trigger,
                body_store=resolved_body_store_cfg,
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
                name="step",
                method="POST",
                url="https://files.example.com/v2/files",
            )
        ],
    )


def _inputs(
    envelope: ChainEnvelope,
    *,
    body_refs: dict[str, bytes] | None = None,
    idempotency_header: str | None = None,
    authorization: str | None = None,
) -> AdmissionInputs:
    return AdmissionInputs(
        request_id="r-1",
        uid_header="user-1",
        instance_header=None,
        idempotency_header=idempotency_header,
        envelope=envelope,
        body_refs=body_refs or {},
        authorization=authorization,
        content_encoding=None,
    )


@pytest.mark.asyncio
async def test_admit_chain_saturation_refused(tmp_path: Path) -> None:
    """Saturation refusal raises ChainAdmissionError(code='saturation_cap')."""
    sat = MagicMock(spec=SaturationGate)

    async def _refuse_saturation(_n: int) -> AdmissionRefusedSaturation:
        return AdmissionRefusedSaturation()

    sat.admit = _refuse_saturation
    instance = await _build_instance(tmp_path, saturation=sat)
    envelope = _envelope()
    inputs = _inputs(envelope)
    with pytest.raises(ChainAdmissionError) as exc_info:
        await admit_chain(inputs, instance)
    assert exc_info.value.code == "saturation_cap"
    assert exc_info.value.headers is not None
    assert exc_info.value.headers.get("Retry-After")


@pytest.mark.asyncio
async def test_admit_chain_disk_pressure_refused(tmp_path: Path) -> None:
    """Disk-pressure refusal raises ChainAdmissionError(code='disk_pressure')."""
    sat = MagicMock(spec=SaturationGate)

    async def _refuse_disk(_n: int) -> AdmissionRefusedDiskPressure:
        return AdmissionRefusedDiskPressure()

    sat.admit = _refuse_disk
    instance = await _build_instance(tmp_path, saturation=sat)
    envelope = _envelope()
    inputs = _inputs(envelope)
    with pytest.raises(ChainAdmissionError) as exc_info:
        await admit_chain(inputs, instance)
    assert exc_info.value.code == "disk_pressure"


@pytest.mark.asyncio
async def test_admit_chain_idempotency_replay(tmp_path: Path) -> None:
    """Submitting twice with the same idempotency header replays (status 200).

    The atomic insert_with_idempotency_claim transaction returns False
    on the duplicate; admission rolls back the body-store put + the
    saturation grant and returns the existing row.
    """
    instance = await _build_instance(tmp_path)
    envelope = _envelope()
    inputs = _inputs(envelope, idempotency_header="ikey-1")
    first = await admit_chain(inputs, instance)
    assert first.status_code == 202

    # Second submit with same idempotency_header but different envelope chain_id.
    envelope2 = _envelope()
    inputs2 = _inputs(envelope2, idempotency_header="ikey-1")
    second = await admit_chain(inputs2, instance)
    assert second.status_code == 200
    # The replayed row should be the original.
    assert second.row.chain_id == first.row.chain_id


@pytest.mark.asyncio
async def test_admit_chain_idempotency_collision_releases_saturation(tmp_path: Path) -> None:
    """The collision-replay path releases the gate slot taken on the duplicate.

    Without the release the duplicate would hold a slot until the
    body's saturation window expires — the test asserts the gate
    counters do not grow past the first admission.
    """
    instance = await _build_instance(tmp_path)
    envelope = _envelope()
    inputs = _inputs(envelope, idempotency_header="ikey-1", body_refs={"body": b"hi"})
    first = await admit_chain(inputs, instance)
    assert first.status_code == 202
    in_flight_after_first = instance.saturation.in_flight
    in_flight_bytes_after_first = instance.saturation.in_flight_bytes

    envelope2 = _envelope()
    inputs2 = _inputs(envelope2, idempotency_header="ikey-1", body_refs={"body": b"hi"})
    second = await admit_chain(inputs2, instance)
    assert second.status_code == 200
    assert instance.saturation.in_flight == in_flight_after_first
    assert instance.saturation.in_flight_bytes == in_flight_bytes_after_first


@pytest.mark.asyncio
async def test_admit_chain_idempotency_collision_cleans_up_body(tmp_path: Path) -> None:
    """Body bytes written for the duplicate are deleted on collision.

    Otherwise the body store accumulates orphan bytes for every
    duplicate POST. Finding G-1: this duplicate carries a DIFFERENT body
    under the same idempotency key, so admission now rejects with
    ``idempotency_key_conflict`` (was a silent 200 replay that dropped
    the second body). The body cleanup must still happen on the reject
    path.
    """
    instance = await _build_instance(tmp_path)
    envelope = _envelope()
    inputs = _inputs(envelope, idempotency_header="ikey-1", body_refs={"body": b"hi"})
    first = await admit_chain(inputs, instance)
    assert first.status_code == 202
    first_body_chain_id = first.row.chain_id

    envelope2 = _envelope()
    inputs2 = _inputs(envelope2, idempotency_header="ikey-1", body_refs={"body": b"hi-dup"})
    duplicate_chain_id = envelope2.chain_id
    with pytest.raises(ChainAdmissionError) as exc_info:
        await admit_chain(inputs2, instance)
    assert exc_info.value.code == "idempotency_key_conflict"
    # The duplicate's body bytes must not be present in the body store.
    assert duplicate_chain_id not in await instance.body_store.list_chain_ids()
    # The original's body is still there.
    assert first_body_chain_id in await instance.body_store.list_chain_ids()


@pytest.mark.asyncio
async def test_admit_chain_body_hashes_computed(tmp_path: Path) -> None:
    """The orchestrator computes body_hash + storage_hash per body_ref."""
    instance = await _build_instance(tmp_path)
    envelope = _envelope()
    body = b"hello-body"
    inputs = _inputs(envelope, body_refs={"body": body})
    outcome = await admit_chain(inputs, instance)
    assert "body" in outcome.row.body_hashes
    hashes = outcome.row.body_hashes["body"]
    expected = hashlib.sha256(body).hexdigest()
    # Passthrough codec — body and storage hashes both equal the raw SHA-256.
    assert hashes.body_hash == expected
    assert hashes.storage_hash == expected


@pytest.mark.asyncio
async def test_admit_chain_authorization_writes_token_cache(tmp_path: Path) -> None:
    """An incoming ``Authorization`` header is written to the token cache."""
    instance = await _build_instance(tmp_path)
    envelope = _envelope()
    inputs = _inputs(envelope, authorization="Bearer abcdef")
    await admit_chain(inputs, instance)
    slot = await instance.token_cache.get("files.example.com", "user-1")
    assert slot is not None
    assert slot.status == "fresh"


@pytest.mark.asyncio
async def test_admit_chain_in_hybrid_lands_body_in_ram(tmp_path: Path) -> None:
    """In hybrid mode the new row starts ``body_location='ram'``."""
    instance = await _build_instance(
        tmp_path,
        body_store_cfg=BodyStoreCfg(mode="hybrid", ram_ceiling_bytes=1_073_741_824),
    )
    envelope = _envelope()
    inputs = _inputs(envelope, body_refs={"body": b"x" * 100})
    outcome = await admit_chain(inputs, instance)
    assert outcome.status_code == 202
    assert outcome.row.body_location == "ram"


@pytest.mark.asyncio
async def test_admit_chain_body_above_threshold_enqueues_persist_controller(
    tmp_path: Path,
) -> None:
    """In hybrid mode with a size threshold, large bodies enqueue persist immediately.

    Closes plan § 2.3.21 #9. The threshold knob lets the operator say
    "bodies bigger than N skip the retry-linger and migrate to disk
    right away." Below threshold (or no controller) — no enqueue, the
    body stays in RAM until linger or RAM-pressure kicks in.

    Uses a recording stub for the persist controller so the test
    asserts on the call without standing up a real controller +
    TaskGroup.
    """
    from uuid import UUID

    enqueued: list[UUID] = []

    class _RecordingController:
        async def enqueue(self, chain_id: UUID) -> object:
            enqueued.append(chain_id)
            return object()  # Stand-in for the Future the real method returns.

    instance = await _build_instance(
        tmp_path,
        persist_trigger=PersistTriggerCfg(body_size_threshold_bytes=10),
        body_store_cfg=BodyStoreCfg(mode="hybrid", ram_ceiling_bytes=1_073_741_824),
    )
    # Inject the recording controller after the InstanceContext is built
    # so the `mode=='hybrid' and threshold and controller is not None`
    # branch fires inside admit_chain.
    instance.persist_controller = _RecordingController()  # type: ignore[assignment]

    envelope = _envelope()
    # Body > 10 bytes — above threshold.
    above = _inputs(envelope, body_refs={"body": b"x" * 50})
    above_outcome = await admit_chain(above, instance)
    assert above_outcome.status_code == 202
    assert enqueued == [above_outcome.row.chain_id]

    # Body < 10 bytes — below threshold; controller NOT invoked.
    envelope2 = _envelope()
    below = _inputs(envelope2, body_refs={"body": b"x"})
    below_outcome = await admit_chain(below, instance)
    assert below_outcome.status_code == 202
    # Still just the one prior enqueue — small body did not trigger.
    assert enqueued == [above_outcome.row.chain_id]


def _envelope_with_step_headers(headers: dict[str, str]) -> ChainEnvelope:
    """Build a single-step envelope carrying ``headers`` on the step."""
    return ChainEnvelope(  # type: ignore[call-arg]
        chain_id=uuid4(),
        idempotency_key="k",
        steps=[
            ChainStep(  # type: ignore[call-arg]
                name="step",
                method="POST",
                url="https://files.example.com/v2/files",
                headers=headers,
            )
        ],
    )


@pytest.mark.asyncio
async def test_admit_chain_rejects_whitespace_padded_header_name(
    tmp_path: Path,
) -> None:
    """Step headers with leading/trailing whitespace are rejected at admission.

    Locks in the round-3 fix for the X-Phantom-* strip evasion: a producer
    that sends ``"  X-Phantom-Probe  "`` (whitespace-padded) slips past
    the case-insensitive ``startswith("x-phantom-")`` strip in the
    executor and stalls the chain in retry under the default
    ``max_attempts=-1``. Admission must reject the envelope as
    malformed (RFC 7230 §3.2 forbids whitespace in header names).
    """
    instance = await _build_instance(tmp_path)
    envelope = _envelope_with_step_headers({"  X-Phantom-Probe  ": "leak-evading-strip"})
    inputs = _inputs(envelope)
    with pytest.raises(ChainAdmissionError) as exc_info:
        await admit_chain(inputs, instance)
    assert exc_info.value.code == "envelope_invalid"
    assert "X-Phantom-Probe" in exc_info.value.message


@pytest.mark.asyncio
async def test_admit_chain_rejects_empty_header_name(tmp_path: Path) -> None:
    """An empty header-name string is rejected at admission."""
    instance = await _build_instance(tmp_path)
    envelope = _envelope_with_step_headers({"": "value"})
    inputs = _inputs(envelope)
    with pytest.raises(ChainAdmissionError) as exc_info:
        await admit_chain(inputs, instance)
    assert exc_info.value.code == "envelope_invalid"


@pytest.mark.asyncio
async def test_admit_chain_rejects_control_char_in_header_name(
    tmp_path: Path,
) -> None:
    """Control characters in header names are rejected at admission."""
    instance = await _build_instance(tmp_path)
    envelope = _envelope_with_step_headers({"X-Bad\x01Header": "value"})
    inputs = _inputs(envelope)
    with pytest.raises(ChainAdmissionError) as exc_info:
        await admit_chain(inputs, instance)
    assert exc_info.value.code == "envelope_invalid"


@pytest.mark.asyncio
async def test_admit_chain_accepts_legal_header_names(tmp_path: Path) -> None:
    """RFC 7230-legal header names — including punctuation — admit cleanly."""
    instance = await _build_instance(tmp_path)
    envelope = _envelope_with_step_headers(
        {
            "X-Phantom-Probe": "https://files.example.com/v2/files",
            "User-Agent": "phantom-defender/round-3",
            "X-Amz-Meta-Ref-Id": "h-001",
            "X-Custom_Trace.Id": "trace-12345",
        }
    )
    inputs = _inputs(envelope)
    outcome = await admit_chain(inputs, instance)
    assert outcome.status_code == 202


# -----------------------------------------------------------------------------
# H1 regression tests — saturation try/except (Phase 2 § 3.2.2)
# -----------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_admit_chain_releases_slot_on_codec_failure(tmp_path: Path) -> None:
    """If the codec raises mid-admit, the saturation slot is released (H1).

    Before this fix the gate took the slot in ``saturation.admit``, then
    the codec exception unwound straight out — the slot leaked. The
    try/except wrapper now releases on every non-happy-path exit so the
    gate stays accurate.
    """

    class _FailingCodec:
        algorithm_name: str = "zstd-3"

        def encode(self, _data: bytes) -> bytes:
            raise RuntimeError("simulated codec failure")

    instance = await _build_instance(tmp_path)
    instance.codec_factory = lambda: _FailingCodec()  # type: ignore[assignment]
    in_flight_before = instance.saturation.in_flight
    in_flight_bytes_before = instance.saturation.in_flight_bytes

    envelope = _envelope()
    inputs = _inputs(envelope, body_refs={"body": b"payload"})
    with pytest.raises(RuntimeError, match="simulated codec failure"):
        await admit_chain(inputs, instance)

    # H1 closure: the slot is freed even though the admission failed
    # between admit and insert.
    assert instance.saturation.in_flight == in_flight_before
    assert instance.saturation.in_flight_bytes == in_flight_bytes_before

    # And the gate is still functional — a subsequent admission lands.
    envelope2 = _envelope()
    inputs2 = _inputs(envelope2)
    outcome = await admit_chain(inputs2, instance)
    assert outcome.status_code == 202


@pytest.mark.asyncio
async def test_admit_chain_releases_slot_on_body_store_failure(tmp_path: Path) -> None:
    """A ``body_store.put`` OSError releases the slot (H1) AND surfaces ADR-017.

    Findings R7-1-A/B / R7-2-A: a storage-layer ``OSError`` (fsync EIO /
    ENOSPC) during admission's body buffering must NOT escape as a naked
    HTTP 500 — it is mapped to the registered ``storage_unavailable`` 503
    (retryable, Retry-After). The original H1 contract still holds: the
    saturation slot is released exactly once because no row committed.
    """

    class _FailingBodyStore:
        async def delete(self, _chain_id: object) -> None:
            # The R11-1 namespace clear precedes the put; it succeeds
            # here so the test pins the PUT failure arm specifically.
            return None

        async def put(self, _chain_id: object, _refs: object) -> int:
            raise OSError("simulated disk error")

        # The other Protocol methods are not invoked in this test — admission
        # calls ``delete`` (the R11-1 namespace clear) then ``put`` between
        # admit and the SQL transaction.

    instance = await _build_instance(tmp_path)
    instance.body_store = _FailingBodyStore()  # type: ignore[assignment]
    in_flight_before = instance.saturation.in_flight
    in_flight_bytes_before = instance.saturation.in_flight_bytes

    envelope = _envelope()
    inputs = _inputs(envelope, body_refs={"body": b"payload"})
    with pytest.raises(ChainAdmissionError) as exc_info:
        await admit_chain(inputs, instance)
    # Mapped to the retryable storage code, not a naked OSError → 500.
    assert exc_info.value.code == "storage_unavailable"
    assert exc_info.value.headers is not None
    assert exc_info.value.headers.get("Retry-After")

    # H1: the slot is released (no in-flight row committed).
    assert instance.saturation.in_flight == in_flight_before
    assert instance.saturation.in_flight_bytes == in_flight_bytes_before


@pytest.mark.asyncio
async def test_admit_chain_releases_slot_on_namespace_clear_failure(tmp_path: Path) -> None:
    """An OSError from the R11-1 namespace clear rides the same 503 arm.

    The clear (``body_store.delete``) shares the put's try/except: a
    storage fault while clearing a reused chain_id's namespace maps to
    the retryable ``storage_unavailable`` envelope (never a naked 500),
    no row commits, and the slot unwinds into the ``__aexit__`` release
    (H1) - the identical posture as a put failure.
    """

    class _ClearFailingBodyStore:
        async def delete(self, _chain_id: object) -> None:
            raise OSError("simulated rm failure during the namespace clear")

        # ``put`` is unreachable: the clear precedes it and raises.

    instance = await _build_instance(tmp_path)
    instance.body_store = _ClearFailingBodyStore()  # type: ignore[assignment]
    in_flight_before = instance.saturation.in_flight
    in_flight_bytes_before = instance.saturation.in_flight_bytes

    envelope = _envelope()
    inputs = _inputs(envelope, body_refs={"body": b"payload"})
    with pytest.raises(ChainAdmissionError) as exc_info:
        await admit_chain(inputs, instance)
    assert exc_info.value.code == "storage_unavailable"
    assert exc_info.value.headers is not None
    assert exc_info.value.headers.get("Retry-After")

    # H1: the slot is released (no in-flight row committed).
    assert instance.saturation.in_flight == in_flight_before
    assert instance.saturation.in_flight_bytes == in_flight_bytes_before


@pytest.mark.asyncio
async def test_admit_chain_happy_path_holds_slot(tmp_path: Path) -> None:
    """The happy path does NOT release the slot — sender owns the release.

    Counter-regression to the H1 fix: the new try/except wrapper must
    not over-release on the success branch. The slot stays held until
    the sender records a terminal outcome.
    """
    instance = await _build_instance(tmp_path)
    in_flight_before = instance.saturation.in_flight
    in_flight_bytes_before = instance.saturation.in_flight_bytes

    envelope = _envelope()
    inputs = _inputs(envelope, body_refs={"body": b"hi"})
    outcome = await admit_chain(inputs, instance)
    assert outcome.status_code == 202

    # The slot stays held — declared bytes (len(b"hi") == 2) accounted.
    assert instance.saturation.in_flight == in_flight_before + 1
    assert instance.saturation.in_flight_bytes == in_flight_bytes_before + 2


# -----------------------------------------------------------------------------
# § 2.1: server-side idempotency mint. Dedup is never skipped for lack of a
# client X-Phantom-Idempotency-Key. The minted ingress key is str(chain_id).
# -----------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_admit_no_header_writes_idempotency_claim(tmp_path: Path) -> None:
    """A submission with NO X-Phantom-Idempotency-Key still writes a claim (§ 2.1).

    The server mints ``str(chain_id)`` as the ingress dedup key and uses it at
    BOTH the row's ``chain_id_at_ingress`` column and the ``idempotency_index``
    claim, so a naive raw-HTTP client that omits the header gets full dedup
    protection.
    """
    instance = await _build_instance(tmp_path)
    envelope = _envelope()
    chain_id = envelope.chain_id
    inputs = _inputs(envelope, idempotency_header=None)
    outcome = await admit_chain(inputs, instance)
    assert outcome.status_code == 202

    # The row's ingress column was set to the minted key (the duplicate-lookup
    # fallback reads THIS column).
    found = await instance.store.find_by_chain_id_at_ingress(str(chain_id))
    assert found == chain_id

    # A LIVE idempotency_index claim exists under the minted key and points at
    # the original chain (INSERT-OR-IGNORE preserves the existing claim, so the
    # probe with a fresh uuid returns the original chain_id).
    surviving = await instance.store.claim_idempotency(str(chain_id), uuid4())
    assert surviving == chain_id


@pytest.mark.asyncio
async def test_admit_no_header_resend_same_chain_dedupes(tmp_path: Path) -> None:
    """A no-header resend of the SAME chain is deduped, not admitted twice (§ 2.1).

    With the header absent the ingress key is ``str(chain_id)``; a resend of the
    same chain shares both the chain_id PK and that ingress key, so the server
    refuses to admit a second live copy (``chain_id_in_use``). Exactly one live
    row exists either way: the dedup claim is never skipped.
    """
    instance = await _build_instance(tmp_path)
    envelope = _envelope()
    first = await admit_chain(_inputs(envelope, idempotency_header=None), instance)
    assert first.status_code == 202

    # Resend the identical envelope (same chain_id) with no header.
    with pytest.raises(ChainAdmissionError) as exc_info:
        await admit_chain(_inputs(envelope, idempotency_header=None), instance)
    assert exc_info.value.code == "chain_id_in_use"


@pytest.mark.asyncio
async def test_admit_no_header_two_distinct_chains_do_not_collide(tmp_path: Path) -> None:
    """Two different chains with no header each get their own claim (§ 2.1).

    The minted key is ``str(chain_id)``, which is distinct per chain, so two
    independent no-header submissions never collide on a shared ``""`` key.
    Both admit (202) and each owns a distinct ingress claim.
    """
    instance = await _build_instance(tmp_path)
    env_a = _envelope()
    env_b = _envelope()
    assert env_a.chain_id != env_b.chain_id

    out_a = await admit_chain(_inputs(env_a, idempotency_header=None), instance)
    out_b = await admit_chain(_inputs(env_b, idempotency_header=None), instance)
    assert out_a.status_code == 202
    assert out_b.status_code == 202

    assert await instance.store.find_by_chain_id_at_ingress(str(env_a.chain_id)) == env_a.chain_id
    assert await instance.store.find_by_chain_id_at_ingress(str(env_b.chain_id)) == env_b.chain_id


@pytest.mark.asyncio
async def test_admit_blank_header_is_minted_not_shared(tmp_path: Path) -> None:
    """A blank/whitespace inbound header is treated as absent and minted (§ 2.1).

    Two submissions whose inbound header is whitespace-only must NOT collide on
    a shared blank key: each is filled with its own ``str(chain_id)``.
    """
    instance = await _build_instance(tmp_path)
    env_a = _envelope()
    env_b = _envelope()
    out_a = await admit_chain(_inputs(env_a, idempotency_header="   "), instance)
    out_b = await admit_chain(_inputs(env_b, idempotency_header="   "), instance)
    assert out_a.status_code == 202
    assert out_b.status_code == 202
    assert await instance.store.find_by_chain_id_at_ingress(str(env_a.chain_id)) == env_a.chain_id
    assert await instance.store.find_by_chain_id_at_ingress(str(env_b.chain_id)) == env_b.chain_id


@pytest.mark.asyncio
async def test_admit_nonblank_client_header_wins_and_dedupes(tmp_path: Path) -> None:
    """A non-blank client header is kept verbatim and still dedupes (§ 2.1).

    Two DIFFERENT chains submitted under the same explicit inbound header
    collide on that header (IDEMPOTENCY_COLLISION -> 200 replay); the claim key
    is the client header, NOT ``str(chain_id)``.
    """
    instance = await _build_instance(tmp_path)
    env_first = _envelope()
    first = await admit_chain(_inputs(env_first, idempotency_header="client-key-1"), instance)
    assert first.status_code == 202

    # The claim is keyed by the client header (not str(chain_id)).
    assert await instance.store.find_by_chain_id_at_ingress("client-key-1") == env_first.chain_id
    assert await instance.store.find_by_chain_id_at_ingress(str(env_first.chain_id)) is None

    # A second, different chain under the same header replays the first.
    env_second = _envelope()
    second = await admit_chain(_inputs(env_second, idempotency_header="client-key-1"), instance)
    assert second.status_code == 200
    assert second.row.chain_id == env_first.chain_id
