"""Unit tests for :mod:`phantom_emulator.upload.presigned`."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from uuid import uuid4

from phantom_emulator.upload.presigned import PresignedTokenStore


def test_mint_then_resolve() -> None:
    store = PresignedTokenStore(base_url="http://emulator.test", default_ttl_seconds=3600)
    fid = uuid4()
    token, url, record = store.mint(
        file_id=fid,
        file_information={"id": str(fid), "name": "x"},
        metadata_kvs={"phantom_local_uuid": "abc-123"},
    )
    assert token in url
    assert url.startswith("http://emulator.test/v1/files/upload/")
    assert "expires=" in url
    assert "sig=" in url

    resolved = store.resolve(token)
    assert resolved is record
    assert resolved is not None
    assert resolved.metadata_kvs["phantom_local_uuid"] == "abc-123"
    assert resolved.file_id == fid


def test_expired_detection() -> None:
    base = datetime.now(UTC)
    store = PresignedTokenStore(base_url="http://emulator.test", default_ttl_seconds=10)
    fid = uuid4()
    token, _url, _record = store.mint(file_id=fid, file_information={}, metadata_kvs={}, now=base)
    assert store.is_expired(token, base) is False
    assert store.is_expired(token, base + timedelta(seconds=5)) is False
    assert store.is_expired(token, base + timedelta(seconds=11)) is True


def test_unknown_token_is_expired() -> None:
    store = PresignedTokenStore(base_url="http://emulator.test", default_ttl_seconds=10)
    assert store.is_expired("does-not-exist", datetime.now(UTC)) is True
    assert store.resolve("does-not-exist") is None


def test_ttl_override_per_call() -> None:
    base = datetime.now(UTC)
    store = PresignedTokenStore(base_url="http://emulator.test", default_ttl_seconds=3600)
    token, _url, record = store.mint(
        file_id=uuid4(),
        file_information={},
        metadata_kvs={},
        presigned_ttl_seconds=1,
        now=base,
    )
    assert record.presigned_ttl_seconds == 1
    assert store.is_expired(token, base + timedelta(seconds=2)) is True


def test_signature_round_trip() -> None:
    base = datetime.now(UTC)
    store = PresignedTokenStore(base_url="http://emulator.test", default_ttl_seconds=60)
    _token, url, record = store.mint(
        file_id=uuid4(), file_information={}, metadata_kvs={}, now=base
    )
    # The signature embedded in the URL matches the record's signature.
    assert f"sig={record.signature}" in url
