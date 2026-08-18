"""Per-stage unit tests for the decomposed :mod:`phantom.routes.admission`.

Cycle-7 plan section 3, task 2.1: ``admit_chain`` is a staged
orchestrator; each stage is a typed module function. This module tests
every stage directly. The orchestrator-level behavior (collision
replay-or-conflict, H1 slot accounting through the full flow, the
idempotency mint) stays covered by the pre-existing ``test_admission.py``
and the R3-x regression modules, which pass unchanged against the
decomposition.

Stage map:

* parse/validate: ``_validate_step_headers`` (covered in
  ``test_admission.py``; not duplicated here)
* encode + dual-hash: ``_encode_and_hash_bodies`` -> ``_EncodedBodies``
* saturation admit: ``_admit_saturation_slot`` -> ``_AdmittedSlot``
* row preparation: ``_build_row`` -> ``_PreparedRow``
* persist: ``_persist_row_and_claim``; collision arm
  ``_resolve_collision``
* respond: ``_maybe_enqueue_immediate_persist``
"""

from __future__ import annotations

import hashlib
from datetime import UTC, datetime
from pathlib import Path
from uuid import UUID, uuid4

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
    _admit_saturation_slot,
    _AdmittedSlot,
    _build_row,
    _encode_and_hash_bodies,
    _maybe_enqueue_immediate_persist,
    _persist_row_and_claim,
    _resolve_collision,
)
from phantom.routing import resolve_route
from phantom.storage import (
    FileBodyStore,
    RamBodyStore,
    SqliteTokenCache,
    SqliteUploadStore,
)
from phantom.storage.hybrid_body_store import HybridBodyStore
from phantom.storage.interface import InsertClaimOutcome
from phantom.strategies import FixedIntervalsStrategy
from phantom.transport import UpstreamRequest, UpstreamResponse
from phantom.workers.saturation import SaturationGate, SlotReservation

from .conftest import make_snapshot, snapshot_thunk, track_instance

# Gate limits for the per-stage tests: ample headroom so only the
# explicitly arranged refusals fire.
_GATE_MAX_IN_FLIGHT = 10
_GATE_MAX_BYTES = 10_000_000


class _FakeUpstream:
    async def start(self) -> None: ...

    async def stop(self) -> None: ...

    async def send(self, _req: UpstreamRequest) -> UpstreamResponse:
        return UpstreamResponse(status=200, body=b"{}")


async def _build_instance(
    tmp_path: Path,
    *,
    body_store_cfg: BodyStoreCfg | None = None,
    persist_trigger: PersistTriggerCfg | None = None,
) -> InstanceContext:
    """Standard single-instance context mirroring test_admission.py's."""
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
    saturation = SaturationGate(
        max_in_flight=_GATE_MAX_IN_FLIGHT,
        max_in_flight_bytes=_GATE_MAX_BYTES,
        max_disk_bytes=_GATE_MAX_BYTES,
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
                persist_trigger=persist_trigger or PersistTriggerCfg(body_size_threshold_bytes=0),
                body_store=body_store_cfg or BodyStoreCfg(ram_ceiling_bytes=1_073_741_824),
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


# ---------------------------------------------------------------------------
# Stage: encode + dual-hash (_encode_and_hash_bodies -> _EncodedBodies).
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_encode_stage_empty_refs(tmp_path: Path) -> None:
    """No body_refs: original encoding, empty maps, zero byte counts."""
    instance = await _build_instance(tmp_path)
    encoded = await _encode_and_hash_bodies(instance, {})
    assert encoded.storage_encoding == "original"
    assert encoded.stored_body_refs == {}
    assert encoded.body_hashes_map == {}
    assert encoded.stored_size == 0
    assert encoded.admit_bytes == 0


@pytest.mark.asyncio
async def test_encode_stage_identity_codec_single_hash(tmp_path: Path) -> None:
    """Identity codec: encoded == raw and one SHA-256 covers both hashes."""
    instance = await _build_instance(tmp_path)
    body = b"stage-two-body"
    encoded = await _encode_and_hash_bodies(instance, {"body": body})
    expected = hashlib.sha256(body).hexdigest()
    assert encoded.stored_body_refs == {"body": body}
    assert encoded.body_hashes_map["body"].body_hash == expected
    assert encoded.body_hashes_map["body"].storage_hash == expected
    assert encoded.stored_size == len(body)
    # R3-8 unit-symmetry: the gate basis is the stored size.
    assert encoded.admit_bytes == encoded.stored_size


@pytest.mark.asyncio
async def test_encode_stage_real_codec_dual_hash(tmp_path: Path) -> None:
    """A non-identity codec yields distinct raw/storage hashes and sizes."""
    instance = await _build_instance(tmp_path)

    def zstd_factory() -> object:  # type: ignore[type-arg]
        return select_codec(CompressionCfg(algorithm="zstd"))

    instance.codec_factory = zstd_factory  # type: ignore[assignment]
    body = b"compressible-" * 100
    encoded = await _encode_and_hash_bodies(instance, {"body": body})
    stored = encoded.stored_body_refs["body"]
    assert stored != body
    assert encoded.body_hashes_map["body"].body_hash == hashlib.sha256(body).hexdigest()
    assert encoded.body_hashes_map["body"].storage_hash == hashlib.sha256(stored).hexdigest()
    assert encoded.stored_size == len(stored)
    assert encoded.admit_bytes == len(stored)


# ---------------------------------------------------------------------------
# Stage: saturation admit (_admit_saturation_slot + _AdmittedSlot).
# ---------------------------------------------------------------------------

# Byte quantity used by the slot-ownership tests; arbitrary non-zero so
# the byte counter visibly moves.
_SLOT_BYTES = 64


@pytest.mark.asyncio
async def test_admit_slot_grant_and_exit_releases(tmp_path: Path) -> None:
    """An uncommitted slot is released on exit (the H1 leak protection)."""
    instance = await _build_instance(tmp_path)
    slot = await _admit_saturation_slot(instance, _SLOT_BYTES)
    assert instance.saturation.in_flight == 1
    assert instance.saturation.in_flight_bytes == _SLOT_BYTES
    async with slot:
        pass  # Neither committed nor released.
    assert instance.saturation.in_flight == 0
    assert instance.saturation.in_flight_bytes == 0


@pytest.mark.asyncio
async def test_admit_slot_commit_keeps_slot_held(tmp_path: Path) -> None:
    """A committed slot is NOT released on exit; the sender owns it."""
    instance = await _build_instance(tmp_path)
    slot = await _admit_saturation_slot(instance, _SLOT_BYTES)
    async with slot:
        slot.commit()
    assert instance.saturation.in_flight == 1
    assert instance.saturation.in_flight_bytes == _SLOT_BYTES


@pytest.mark.asyncio
async def test_admit_slot_releases_on_exception(tmp_path: Path) -> None:
    """An exception inside the ownership scope releases the slot exactly once."""
    instance = await _build_instance(tmp_path)
    slot = await _admit_saturation_slot(instance, _SLOT_BYTES)
    with pytest.raises(RuntimeError, match="boom"):
        async with slot:
            raise RuntimeError("boom")
    assert instance.saturation.in_flight == 0
    assert instance.saturation.in_flight_bytes == 0


@pytest.mark.asyncio
async def test_admit_slot_release_on_rejection_is_single(tmp_path: Path) -> None:
    """release_on_rejection frees the slot once; exit and re-calls are no-ops.

    R3-1 single-release invariant at the stage level: with a second live
    slot held, the rejection release must restore the counters to the
    live baseline, not below it.
    """
    instance = await _build_instance(tmp_path)
    live = await _admit_saturation_slot(instance, _SLOT_BYTES)
    live.commit()  # A different in-flight row's slot, owned by the sender.
    baseline_rows = instance.saturation.in_flight
    baseline_bytes = instance.saturation.in_flight_bytes

    slot = await _admit_saturation_slot(instance, _SLOT_BYTES)
    with pytest.raises(ChainAdmissionError):
        async with slot:
            await slot.release_on_rejection()
            await slot.release_on_rejection()  # Idempotent second call.
            raise ChainAdmissionError(
                code="idempotency_key_conflict",
                message="expected rejection",
                instance_id="primary",
            )
    assert instance.saturation.in_flight == baseline_rows
    assert instance.saturation.in_flight_bytes == baseline_bytes


@pytest.mark.asyncio
async def test_admit_slot_disk_pressure_refusal(tmp_path: Path) -> None:
    """A disk-pressure refusal maps to disk_pressure with Retry-After."""
    instance = await _build_instance(tmp_path)
    instance.saturation.set_disk_usage_bytes(instance.saturation.max_disk_bytes)
    with pytest.raises(ChainAdmissionError) as exc_info:
        await _admit_saturation_slot(instance, 0)
    assert exc_info.value.code == "disk_pressure"
    assert exc_info.value.headers is not None
    assert exc_info.value.headers.get("Retry-After")


@pytest.mark.asyncio
async def test_admit_slot_saturation_refusal(tmp_path: Path) -> None:
    """A saturation refusal maps to saturation_cap with Retry-After."""
    instance = await _build_instance(tmp_path)
    for _ in range(_GATE_MAX_IN_FLIGHT):
        (await _admit_saturation_slot(instance, 0)).commit()
    with pytest.raises(ChainAdmissionError) as exc_info:
        await _admit_saturation_slot(instance, 0)
    assert exc_info.value.code == "saturation_cap"
    assert exc_info.value.headers is not None
    assert exc_info.value.headers.get("Retry-After")


# ---------------------------------------------------------------------------
# Stage: row preparation (_build_row -> _PreparedRow).
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_build_row_fields_and_dedup_key_minted(tmp_path: Path) -> None:
    """Header-absent input mints str(chain_id) as the dedup key; row is sound."""
    instance = await _build_instance(tmp_path)
    envelope = _envelope()
    encoded = await _encode_and_hash_bodies(instance, {"body": b"abc"})
    prepared = await _build_row(_inputs(envelope), instance, encoded)
    row = prepared.row
    assert prepared.ingress_dedup_key == str(envelope.chain_id)
    assert row.chain_id == envelope.chain_id
    assert row.chain_id_at_ingress == str(envelope.chain_id)
    assert row.state == "queued"
    assert row.body_location == "ram"  # hybrid default mode
    assert row.endpoint == "files.example.com"
    assert row.route_name == "files"
    assert row.body_size_bytes == encoded.admit_bytes
    assert row.storage_encoding == encoded.storage_encoding
    assert row.body_hashes == encoded.body_hashes_map
    # Grouping defaults (task 2.3): group of one, standalone, position 0.
    assert row.group_id == envelope.chain_id
    assert row.multifile_id is None
    assert row.send_order == 0


@pytest.mark.asyncio
async def test_build_row_stores_parsed_grouping_inputs(tmp_path: Path) -> None:
    """Pre-parsed grouping inputs land on the row verbatim (task 2.3)."""
    instance = await _build_instance(tmp_path)
    envelope = _envelope()
    encoded = await _encode_and_hash_bodies(instance, {})
    group_id = uuid4()
    multifile_id = uuid4()
    inputs = AdmissionInputs(
        request_id="r-1",
        uid_header="user-1",
        instance_header=None,
        idempotency_header=None,
        envelope=envelope,
        body_refs={},
        authorization=None,
        content_encoding=None,
        group_id=group_id,
        multifile_id=multifile_id,
        send_order=7,
    )
    prepared = await _build_row(inputs, instance, encoded)
    assert prepared.row.group_id == group_id
    assert prepared.row.multifile_id == multifile_id
    assert prepared.row.send_order == 7


@pytest.mark.asyncio
async def test_build_row_verbatim_header_key(tmp_path: Path) -> None:
    """A non-blank idempotency header is kept verbatim as the dedup key."""
    instance = await _build_instance(tmp_path)
    envelope = _envelope()
    encoded = await _encode_and_hash_bodies(instance, {})
    prepared = await _build_row(
        _inputs(envelope, idempotency_header="client-key-7"), instance, encoded
    )
    assert prepared.ingress_dedup_key == "client-key-7"
    assert prepared.row.chain_id_at_ingress == "client-key-7"


@pytest.mark.asyncio
async def test_build_row_blank_header_minted(tmp_path: Path) -> None:
    """A whitespace-only idempotency header is treated as absent and minted."""
    instance = await _build_instance(tmp_path)
    envelope = _envelope()
    encoded = await _encode_and_hash_bodies(instance, {})
    prepared = await _build_row(_inputs(envelope, idempotency_header="   "), instance, encoded)
    assert prepared.ingress_dedup_key == str(envelope.chain_id)


@pytest.mark.asyncio
async def test_build_row_caches_authorization(tmp_path: Path) -> None:
    """An inbound Authorization header is written to the token cache."""
    instance = await _build_instance(tmp_path)
    envelope = _envelope()
    encoded = await _encode_and_hash_bodies(instance, {})
    await _build_row(_inputs(envelope, authorization="Bearer xyz"), instance, encoded)
    slot = await instance.token_cache.get("files.example.com", "user-1")
    assert slot is not None
    assert slot.status == "fresh"


@pytest.mark.asyncio
async def test_build_row_all_disk_mode_starts_on_file(tmp_path: Path) -> None:
    """In all_disk mode the row is born body_location='file'."""
    instance = await _build_instance(
        tmp_path,
        body_store_cfg=BodyStoreCfg(mode="all_disk", ram_ceiling_bytes=1_073_741_824),
    )
    envelope = _envelope()
    encoded = await _encode_and_hash_bodies(instance, {})
    prepared = await _build_row(_inputs(envelope), instance, encoded)
    assert prepared.row.body_location == "file"


# ---------------------------------------------------------------------------
# Stage: persist (_persist_row_and_claim) and collision (_resolve_collision).
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_persist_stage_clean_insert(tmp_path: Path) -> None:
    """A fresh row inserts cleanly and its body lands in the body store."""
    instance = await _build_instance(tmp_path)
    envelope = _envelope()
    encoded = await _encode_and_hash_bodies(instance, {"body": b"persist-me"})
    prepared = await _build_row(_inputs(envelope), instance, encoded)
    outcome = await _persist_row_and_claim(
        instance,
        row=prepared.row,
        idempotency_key=prepared.ingress_dedup_key,
        stored_body_refs=encoded.stored_body_refs,
    )
    assert outcome is InsertClaimOutcome.INSERTED
    assert await instance.store.get(envelope.chain_id) is not None
    assert envelope.chain_id in await instance.body_store.list_chain_ids()


@pytest.mark.asyncio
async def test_persist_stage_precheck_rejects_live_chain_id(tmp_path: Path) -> None:
    """The chain_id pre-check rejects a live duplicate WITHOUT a body write.

    Finding R7-4b: the original's body at the shared chain_id key must
    never be touched by the duplicate's rejection.
    """
    instance = await _build_instance(tmp_path)
    envelope = _envelope()
    encoded = await _encode_and_hash_bodies(instance, {"body": b"original-bytes"})
    prepared = await _build_row(_inputs(envelope), instance, encoded)
    assert (
        await _persist_row_and_claim(
            instance,
            row=prepared.row,
            idempotency_key=prepared.ingress_dedup_key,
            stored_body_refs=encoded.stored_body_refs,
        )
        is InsertClaimOutcome.INSERTED
    )

    dup_encoded = await _encode_and_hash_bodies(instance, {"body": b"duplicate-bytes"})
    dup_prepared = await _build_row(
        _inputs(envelope, idempotency_header="fresh-key"), instance, dup_encoded
    )
    with pytest.raises(ChainAdmissionError) as exc_info:
        await _persist_row_and_claim(
            instance,
            row=dup_prepared.row,
            idempotency_key="fresh-key",
            stored_body_refs=dup_encoded.stored_body_refs,
        )
    assert exc_info.value.code == "chain_id_in_use"
    # The original's body bytes are untouched at the shared key.
    refs = await instance.body_store.get_all(envelope.chain_id)
    assert refs == {"body": b"original-bytes"}


@pytest.mark.asyncio
async def test_persist_stage_body_store_fault_maps_to_storage_unavailable(
    tmp_path: Path,
) -> None:
    """A body_store.put OSError surfaces as the retryable storage_unavailable."""

    class _FailingBodyStore:
        async def delete(self, _chain_id: object) -> None:
            # The R11-1 namespace clear precedes the put and succeeds
            # here; this test pins the PUT failure arm. (The clear's
            # own failure arm is pinned in test_admission.py.)
            return None

        async def put(self, _chain_id: object, _refs: object) -> int:
            raise OSError("simulated disk error")

    instance = await _build_instance(tmp_path)
    instance.body_store = _FailingBodyStore()  # type: ignore[assignment]
    envelope = _envelope()
    encoded = await _encode_and_hash_bodies(instance, {"body": b"payload"})
    prepared = await _build_row(_inputs(envelope), instance, encoded)
    with pytest.raises(ChainAdmissionError) as exc_info:
        await _persist_row_and_claim(
            instance,
            row=prepared.row,
            idempotency_key=prepared.ingress_dedup_key,
            stored_body_refs=encoded.stored_body_refs,
        )
    assert exc_info.value.code == "storage_unavailable"
    assert exc_info.value.headers is not None
    assert exc_info.value.headers.get("Retry-After")


@pytest.mark.asyncio
async def test_resolve_collision_chain_id_arm_preserves_shared_body(
    tmp_path: Path,
) -> None:
    """The CHAIN_ID_COLLISION arm releases the slot but never deletes the body.

    Finding R7-4b: the body at the shared chain_id key belongs to the
    winning live row; the loser's rejection must not destroy it.
    """
    instance = await _build_instance(tmp_path)
    envelope = _envelope()
    encoded = await _encode_and_hash_bodies(instance, {"body": b"winning-bytes"})
    prepared = await _build_row(_inputs(envelope), instance, encoded)
    # Simulate the winning row's body already in the store at the shared key.
    await instance.body_store.put(envelope.chain_id, encoded.stored_body_refs)

    slot = await _admit_saturation_slot(instance, encoded.admit_bytes)
    with pytest.raises(ChainAdmissionError) as exc_info:
        async with slot:
            await _resolve_collision(
                _inputs(envelope),
                instance,
                outcome=InsertClaimOutcome.CHAIN_ID_COLLISION,
                prepared=prepared,
                encoded=encoded,
                slot=slot,
            )
    assert exc_info.value.code == "chain_id_in_use"
    # Slot released exactly once (back to idle), body preserved.
    assert instance.saturation.in_flight == 0
    assert instance.saturation.in_flight_bytes == 0
    assert await instance.body_store.get_all(envelope.chain_id) == {"body": b"winning-bytes"}


# ---------------------------------------------------------------------------
# Stage: respond (_maybe_enqueue_immediate_persist).
# ---------------------------------------------------------------------------


class _RecordingController:
    def __init__(self) -> None:
        self.enqueued: list[UUID] = []

    async def enqueue(self, chain_id: UUID) -> object:
        self.enqueued.append(chain_id)
        return object()


# Threshold for the persist-trigger stage tests; small so a modest body
# crosses it.
_TRIGGER_THRESHOLD = 10


@pytest.mark.asyncio
async def test_persist_trigger_fires_above_threshold_in_hybrid(tmp_path: Path) -> None:
    """hybrid + threshold crossed + controller wired: enqueue fires."""
    instance = await _build_instance(
        tmp_path,
        body_store_cfg=BodyStoreCfg(mode="hybrid", ram_ceiling_bytes=1_073_741_824),
        persist_trigger=PersistTriggerCfg(body_size_threshold_bytes=_TRIGGER_THRESHOLD),
    )
    controller = _RecordingController()
    instance.persist_controller = controller  # type: ignore[assignment]
    chain_id = uuid4()
    await _maybe_enqueue_immediate_persist(
        instance,
        chain_id=chain_id,
        stored_size=_TRIGGER_THRESHOLD * 5,
        snapshot=instance.current_settings(),
    )
    assert controller.enqueued == [chain_id]


@pytest.mark.asyncio
async def test_persist_trigger_skips_below_threshold(tmp_path: Path) -> None:
    """A body under the threshold does not enqueue."""
    instance = await _build_instance(
        tmp_path,
        body_store_cfg=BodyStoreCfg(mode="hybrid", ram_ceiling_bytes=1_073_741_824),
        persist_trigger=PersistTriggerCfg(body_size_threshold_bytes=_TRIGGER_THRESHOLD),
    )
    controller = _RecordingController()
    instance.persist_controller = controller  # type: ignore[assignment]
    await _maybe_enqueue_immediate_persist(
        instance,
        chain_id=uuid4(),
        stored_size=_TRIGGER_THRESHOLD - 1,
        snapshot=instance.current_settings(),
    )
    assert controller.enqueued == []


@pytest.mark.asyncio
async def test_persist_trigger_inert_outside_hybrid(tmp_path: Path) -> None:
    """The knob has no effect in all_ram mode (no disk target)."""
    instance = await _build_instance(
        tmp_path,
        body_store_cfg=BodyStoreCfg(mode="all_ram", ram_ceiling_bytes=1_073_741_824),
        persist_trigger=PersistTriggerCfg(body_size_threshold_bytes=_TRIGGER_THRESHOLD),
    )
    controller = _RecordingController()
    instance.persist_controller = controller  # type: ignore[assignment]
    await _maybe_enqueue_immediate_persist(
        instance,
        chain_id=uuid4(),
        stored_size=_TRIGGER_THRESHOLD * 5,
        snapshot=instance.current_settings(),
    )
    assert controller.enqueued == []


@pytest.mark.asyncio
async def test_persist_trigger_inert_without_controller(tmp_path: Path) -> None:
    """No wired controller: the stage is a no-op (hybrid, threshold crossed)."""
    instance = await _build_instance(
        tmp_path,
        body_store_cfg=BodyStoreCfg(mode="hybrid", ram_ceiling_bytes=1_073_741_824),
        persist_trigger=PersistTriggerCfg(body_size_threshold_bytes=_TRIGGER_THRESHOLD),
    )
    assert instance.persist_controller is None
    await _maybe_enqueue_immediate_persist(
        instance,
        chain_id=uuid4(),
        stored_size=_TRIGGER_THRESHOLD * 5,
        snapshot=instance.current_settings(),
    )
    # Reaching here without an AttributeError IS the assertion.


@pytest.mark.asyncio
async def test_admitted_slot_direct_lifecycle() -> None:
    """_AdmittedSlot against a bare gate: full commit/release matrix."""
    gate = SaturationGate(
        max_in_flight=_GATE_MAX_IN_FLIGHT,
        max_in_flight_bytes=_GATE_MAX_BYTES,
        max_disk_bytes=_GATE_MAX_BYTES,
    )
    await gate.admit(_SLOT_BYTES)
    slot = _AdmittedSlot(gate, SlotReservation(_SLOT_BYTES))
    async with slot:
        pass
    assert gate.in_flight == 0 and gate.in_flight_bytes == 0

    await gate.admit(_SLOT_BYTES)
    slot = _AdmittedSlot(gate, SlotReservation(_SLOT_BYTES))
    slot.commit()
    async with slot:
        pass
    assert gate.in_flight == 1 and gate.in_flight_bytes == _SLOT_BYTES
    await gate.release(_SLOT_BYTES)

    await gate.admit(_SLOT_BYTES)
    slot = _AdmittedSlot(gate, SlotReservation(_SLOT_BYTES))
    async with slot:
        await slot.release_on_rejection()
    assert gate.in_flight == 0 and gate.in_flight_bytes == 0
