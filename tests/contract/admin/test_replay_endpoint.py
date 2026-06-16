"""Contract tests for ``POST /v1/admin/chains/{chain_id}/replay``.

Cycle-7 phase 7 pre-round defender fix: replaying a row whose body was
already discarded (``body_discarded_at`` stamped by the one discard
owner, on the sender's immediate leg or the reaper's scheduled leg)
cannot possibly succeed; pre-fix it re-queued the row and the sender's
next claim landed it in ``corrupted``. The route now refuses up front
with the canonical 409 ``ErrorEnvelope`` (``error.code =
'replay_body_discarded'``) and leaves the row untouched.

Round 1 defender fix (R1-1): the SIBLING refusal on the same endpoint,
a replay against a row a sender is actively driving (state
``attempting``, the M-W4-F7 audit closure), used to escape as FastAPI's
raw ``{"detail": "replay_refused_attempting"}`` body via a bare
``HTTPException``. It now rides the same canonical envelope
(``error.code = 'replay_refused_attempting'``, 409) so the SDK raises
the typed ``PhantomConflictError`` instead of ``PhantomEnvelopeError``.

This module pins the WIRE SHAPE of both refusals (envelope fields, the
codes, the details payloads) plus the no-behavior-change leg: a row
still holding its body replays to ``queued`` exactly as before. The
storage-level predicates themselves are pinned in
``src/phantom-service/tests/unit/test_m_w4_f7_expected_state.py``
and the per-leg unit coverage in ``test_discard_single_owner.py``.
"""

from __future__ import annotations

from datetime import UTC, datetime
from uuid import uuid4

from fastapi import FastAPI
from fastapi.testclient import TestClient
from phantom.instances.context import InstanceContext
from phantom.models.errors import ErrorEnvelope
from phantom.models.upload import BodyHash, BodyHashes, StorageHash, UploadRow

# Stored size of the seeded body for rows that still hold bodies. Any
# non-zero value works; the replay predicate reads body_discarded_at,
# not the size.
_RETAINED_BODY_SIZE_BYTES = 100


async def _insert_row(
    ctx: InstanceContext,
    *,
    body_discarded_at: datetime | None,
    sent_at: datetime | None,
    body_size_bytes: int,
) -> UploadRow:
    """Insert one ``succeeded`` row, optionally carrying the discard stamp.

    Mirrors the discard owner's accounting: a discarded row carries the
    stamp AND a zeroed size; a retained row carries neither effect.
    ``body_hashes`` stay present either way (the discard op never
    clears them).
    """
    chain_id = uuid4()
    now = datetime.now(tz=UTC)
    row = UploadRow.model_validate(
        {
            "chain_id": chain_id,
            "instance_id": "primary",
            "group_id": chain_id,
            "multifile_id": None,
            "send_order": 0,
            "route_name": "files",
            "state": "succeeded",
            "body_location": "ram",
            "received_at": now,
            "updated_at": now,
            "sent_at": sent_at,
            "body_discarded_at": body_discarded_at,
            "endpoint": "files.example.com",
            "uid": "test-uid",
            "chain_envelope_json": "{}",
            "idempotency_key": f"k-{chain_id}",
            "capture_reexecution_active": False,
            "body_hashes": {
                "body": BodyHashes(
                    body_hash=BodyHash("a" * 64),
                    storage_hash=StorageHash("b" * 64),
                ),
            },
            "body_size_bytes": body_size_bytes,
        },
    )
    await ctx.store.insert(row)
    return row


async def test_replay_discarded_row_returns_409_envelope(
    admin_app: tuple[FastAPI, InstanceContext],
) -> None:
    """The refusal is the canonical envelope: 409, code, chain + instant."""
    app, ctx = admin_app
    delivered_at = datetime.now(tz=UTC)
    row = await _insert_row(
        ctx,
        body_discarded_at=delivered_at,
        sent_at=delivered_at,
        body_size_bytes=0,
    )
    client = TestClient(app)
    response = client.post(f"/v1/admin/chains/{row.chain_id}/replay")
    assert response.status_code == 409, response.text
    envelope = ErrorEnvelope.model_validate(response.json())
    assert envelope.error.code == "replay_body_discarded"
    assert str(row.chain_id) in envelope.error.message
    assert delivered_at.isoformat() in envelope.error.message
    assert envelope.error.instance_id == "primary"
    assert isinstance(envelope.error.request_id, str)
    assert envelope.error.details == {
        "chain_id": str(row.chain_id),
        "body_discarded_at": delivered_at.isoformat(),
    }


async def test_refused_replay_leaves_the_row_untouched(
    admin_app: tuple[FastAPI, InstanceContext],
) -> None:
    """The refusal is a pure read: state, sent_at, and stamp all survive."""
    app, ctx = admin_app
    delivered_at = datetime.now(tz=UTC)
    row = await _insert_row(
        ctx,
        body_discarded_at=delivered_at,
        sent_at=delivered_at,
        body_size_bytes=0,
    )
    client = TestClient(app)
    refused = client.post(f"/v1/admin/chains/{row.chain_id}/replay")
    assert refused.status_code == 409
    detail = client.get(f"/v1/admin/chains/{row.chain_id}")
    assert detail.status_code == 200, detail.text
    body = detail.json()
    assert body["state"] == "succeeded"
    assert datetime.fromisoformat(body["sent_at"]) == delivered_at
    fresh = await ctx.store.get(row.chain_id)
    assert fresh is not None
    assert fresh.model_dump() == row.model_dump()


async def test_replay_of_retained_body_row_still_requeues(
    admin_app: tuple[FastAPI, InstanceContext],
) -> None:
    """No behavior change: an unstamped row replays to ``queued`` (200).

    The delivery stamp survives the re-queue (write-once ``sent_at``,
    cycle-7 task 2.5); only the per-attempt fields reset.
    """
    app, ctx = admin_app
    delivered_at = datetime.now(tz=UTC)
    row = await _insert_row(
        ctx,
        body_discarded_at=None,
        sent_at=delivered_at,
        body_size_bytes=_RETAINED_BODY_SIZE_BYTES,
    )
    client = TestClient(app)
    response = client.post(f"/v1/admin/chains/{row.chain_id}/replay")
    assert response.status_code == 200, response.text
    # JSON-mode validation: the strict model accepts the wire's string
    # encodings for UUID/datetime fields only through the JSON loader.
    replayed = UploadRow.model_validate_json(response.content)
    assert replayed.chain_id == row.chain_id
    assert replayed.state == "queued"
    assert replayed.attempts == 0
    assert replayed.sent_at == delivered_at


async def test_replay_unknown_chain_is_404_not_409(
    admin_app: tuple[FastAPI, InstanceContext],
) -> None:
    """An unknown chain stays a 404 ``not_found``; the 409 is for known rows."""
    app, _ctx = admin_app
    client = TestClient(app)
    response = client.post(f"/v1/admin/chains/{uuid4()}/replay")
    assert response.status_code == 404, response.text
    envelope = ErrorEnvelope.model_validate(response.json())
    assert envelope.error.code == "not_found"


async def _insert_attempting_row(
    ctx: InstanceContext,
    *,
    body_discarded_at: datetime | None = None,
) -> UploadRow:
    """Insert one row in ``attempting`` (a sender is actively driving it).

    ``body_discarded_at`` builds the pathological attempting+stamped
    combination for the precedence test below; the default ``None`` is
    the plain in-flight row.
    """
    chain_id = uuid4()
    now = datetime.now(tz=UTC)
    discarded = body_discarded_at is not None
    row = UploadRow.model_validate(
        {
            "chain_id": chain_id,
            "instance_id": "primary",
            "group_id": chain_id,
            "multifile_id": None,
            "send_order": 0,
            "route_name": "files",
            "state": "attempting",
            "body_location": "ram",
            "received_at": now,
            "updated_at": now,
            "body_discarded_at": body_discarded_at,
            "endpoint": "files.example.com",
            "uid": "test-uid",
            "chain_envelope_json": "{}",
            "idempotency_key": f"k-{chain_id}",
            "capture_reexecution_active": False,
            "body_size_bytes": 0 if discarded else _RETAINED_BODY_SIZE_BYTES,
        },
    )
    await ctx.store.insert(row)
    return row


async def test_replay_attempting_row_returns_canonical_409_envelope(
    admin_app: tuple[FastAPI, InstanceContext],
) -> None:
    """The attempting-refusal 409 is the canonical envelope (R1-1, fixed).

    Round 1 adversary repro, flipped to a pass by the round 1 defender
    fix. A replay against an ``attempting`` row is refused with HTTP
    409, and that refusal decodes as an ``ErrorEnvelope`` whose
    ``error.code`` an SDK can dispatch on, exactly like the sibling
    ``replay_body_discarded`` refusal on the same endpoint. The refused
    row is left untouched (the sender's in-flight work survives).
    """
    app, ctx = admin_app
    row = await _insert_attempting_row(ctx)
    response = TestClient(app).post(f"/v1/admin/chains/{row.chain_id}/replay")
    assert response.status_code == 409, response.text
    envelope = ErrorEnvelope.model_validate(response.json())
    assert envelope.error.code == "replay_refused_attempting"
    assert envelope.error.instance_id == "primary"
    assert str(row.chain_id) in envelope.error.message
    assert isinstance(envelope.error.request_id, str)
    assert envelope.error.details == {"chain_id": str(row.chain_id)}
    fresh = await ctx.store.get(row.chain_id)
    assert fresh is not None
    assert fresh.model_dump() == row.model_dump()


async def test_body_discarded_takes_precedence_over_attempting_on_the_wire(
    admin_app: tuple[FastAPI, InstanceContext],
) -> None:
    """A stamped row in ``attempting`` refuses as ``replay_body_discarded``.

    Round 2 adversary probe on the R1-1 fix: the store checks
    ``body_discarded_at`` BEFORE the state guard (pinned at unit tier by
    ``test_replay_refuses_a_stamped_row_in_every_state``), so the
    pathological attempting+stamped combination must surface the
    body-accounting code on the wire, not the state refusal. An operator
    seeing ``replay_body_discarded`` knows waiting cannot help; the
    sibling ``replay_refused_attempting`` says retry-after-settle, which
    would be a lie for a row whose bytes are gone.
    """
    app, ctx = admin_app
    discarded_at = datetime.now(tz=UTC)
    row = await _insert_attempting_row(ctx, body_discarded_at=discarded_at)
    response = TestClient(app).post(f"/v1/admin/chains/{row.chain_id}/replay")
    assert response.status_code == 409, response.text
    envelope = ErrorEnvelope.model_validate(response.json())
    assert envelope.error.code == "replay_body_discarded"
    assert envelope.error.details == {
        "chain_id": str(row.chain_id),
        "body_discarded_at": discarded_at.isoformat(),
    }
    fresh = await ctx.store.get(row.chain_id)
    assert fresh is not None
    assert fresh.model_dump() == row.model_dump()
