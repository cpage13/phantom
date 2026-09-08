"""Unit tests for :mod:`phantom_emulator.state`."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from uuid import uuid4

from phantom_emulator.config import AppConfig
from phantom_emulator.state import (
    AcceptedBody,
    CredentialLedger,
    EmulatorState,
    IdempotencyEntry,
    MetadataCreateEvent,
    PendingUpload,
    RawBody,
    S3Object,
    UpstreamEventKind,
)

# Any positive window: these tests care that an entry EXISTS before the reset,
# not when it would have expired on its own.
_DEDUP_WINDOW_SECONDS: int = 60


def test_initial_state() -> None:
    cfg = AppConfig()
    started = datetime.now(UTC)
    state = EmulatorState(cfg=cfg, started_at=started)

    assert state.cfg is cfg
    assert state.started_at == started
    assert state.issued_tokens == {}
    assert state.pending_uploads == {}
    assert state.accepted_bodies == {}
    assert state.idempotency_cache == {}
    assert state.global_paused is False
    assert state.seed == 0
    assert state.failure_state is None
    assert state.jwt_minter is None
    assert state.rsa_keys is None
    assert state.auth_mode_overrides == {}
    assert state.credentials.seen == set()
    assert state.credentials.invalidated == set()


def test_credential_ledger_invalidates_what_it_has_seen() -> None:
    """Objective: pin the three ledger rules the auth controls rest on.

    Expected outcome: ``invalidate_all`` covers everything issued or accepted;
    a re-issue reinstates a credential (the mint is deterministic within a
    second, so a refresh can reproduce a revoked token byte for byte); and an
    acceptance never reinstates, since it is only reached after the ledger has
    already cleared the credential.
    """
    ledger = CredentialLedger()
    ledger.note_issued("minted")
    ledger.note_accepted("presented")

    ledger.invalidate_all()

    assert ledger.is_invalidated("minted") is True
    assert ledger.is_invalidated("presented") is True
    assert ledger.is_invalidated("never-seen") is False

    ledger.note_issued("minted")
    assert ledger.is_invalidated("minted") is False

    ledger.note_accepted("presented")
    assert ledger.is_invalidated("presented") is True


def test_clear_received_resets_every_received_store() -> None:
    """Objective: one reset that provably covers the whole received-side surface.

    The omission this pins is ``idempotency_cache``, which survived the reset
    and let one scenario's cached create response be served to the next. The
    test names every store the reset owns, so adding a store without adding it
    here fails visibly rather than leaking into the next scenario.

    Expected outcome: every received-side store is empty afterwards, while
    ``pending_uploads`` and ``file_id_to_token`` (issued state an in-flight
    chain still resolves against) are deliberately untouched.
    """
    now = datetime.now(UTC)
    state = EmulatorState(cfg=AppConfig(), started_at=now)
    file_id = uuid4()
    state.pending_uploads["tok"] = PendingUpload(
        upload_token="tok",
        file_id=file_id,
        file_information={},
        metadata_kvs={},
        created_at=now,
        presigned_ttl_seconds=_DEDUP_WINDOW_SECONDS,
        signature="sig",
        expires_epoch=int(now.timestamp()) + _DEDUP_WINDOW_SECONDS,
    )
    state.file_id_to_token[file_id] = "tok"
    state.accepted_bodies["tok"] = AcceptedBody(
        upload_token="tok",
        body=b"bytes",
        headers={},
        content_encoding=None,
        all_headers={},
        accepted_at=now,
    )
    state.accepted_idempotency_keys["tok"] = "key"
    state.upstream_events.append(
        MetadataCreateEvent(
            occurred_at=now,
            kind=UpstreamEventKind.METADATA_CREATE,
            chain_id=None,
            idempotency_key="key",
            file_id=file_id,
            upload_token="tok",
            upload_url="http://emulator/v1/files/upload/tok",
            cache_hit=False,
        )
    )
    state.s3_objects[("bucket", "key")] = S3Object(
        bucket="bucket",
        key="key",
        method="PUT",
        body=b"bytes",
        content_type=None,
        all_headers={},
        stored_at=now,
    )
    state.raw_bodies["raw/path"] = RawBody(
        path="raw/path",
        method="PUT",
        query="",
        body=b"bytes",
        content_type=None,
        all_headers={},
        stored_at=now,
    )
    state.idempotency_cache["key"] = IdempotencyEntry(
        key="key",
        response_json={},
        upload_token="tok",
        file_id=file_id,
        expires_at=now + timedelta(seconds=_DEDUP_WINDOW_SECONDS),
    )

    state.clear_received()

    assert state.accepted_bodies == {}
    assert state.accepted_idempotency_keys == {}
    assert state.upstream_events == []
    assert state.s3_objects == {}
    assert state.raw_bodies == {}
    assert state.idempotency_cache == {}
    assert set(state.pending_uploads) == {"tok"}
    assert state.file_id_to_token == {file_id: "tok"}
