"""Unit tests for the admin credential-push endpoint (plan Phase 2 TASK 2.4).

``PUT /v1/admin/credentials/{dest_host}`` is the SigV4 analogue of the admin
token push (:func:`phantom.routes.admin.push_token_one`), a faithful copy per
the 2026-06-23 directive. These tests prove the acceptance criteria:

* a push STORES the credential in every instance's ``signer_creds`` store and
  returns ``204`` with NO body — the ``secret_access_key`` is never echoed
  (ADR-004, status-only);
* the ``{dest_host}`` segment is NORMALIZED through the same ``host_key_for``
  helper the executor uses, so a mixed-case push key equals the executor's
  forward-time lookup key (the silent-miss class is closed);
* the push FIRES the credential-store wake handler end-to-end: a parked
  ``aws_sigv4`` row for that host is woken (``auth_expired → queued``) by the
  sigv4-flavoured :class:`~phantom.workers.kicker.Kicker` that registered
  on the store;
* a bearer-only deployment (``signer_creds is None``) does not crash — the push
  is a graceful no-op ``204``.
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import MagicMock
from uuid import uuid4

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from phantom.config.settings import InstanceCfg, RouteCfg
from phantom.instances.context import InstanceContext
from phantom.instances.dispatcher import InstanceDispatcher
from phantom.models.credential import HostCredKey, SigV4StaticCredBody
from phantom.models.upload import UploadRow
from phantom.routes import admin as admin_routes
from phantom.storage import (
    FileBodyStore,
    RamBodyStore,
    SqliteTokenCache,
    SqliteUploadStore,
)
from phantom.storage.credential_store import SqliteCredentialStore
from phantom.storage.hybrid_body_store import HybridBodyStore
from phantom.workers.kicker import AWS_SIGV4_FLAVOUR, Kicker
from phantom.workers.saturation import SaturationGate
from pydantic import ValidationError

from .conftest import make_snapshot, snapshot_thunk, track_instance, track_started

# The SigV4 route host the executor would key its lookup on. The admin push is
# issued with MIXED CASE so the normalization (push-key == lookup-key) is proven.
_SIGV4_HOST = "s3.us-east-1.amazonaws.com"
_SIGV4_HOST_MIXED = "S3.US-East-1.AmazonAWS.CoM"

# A recognizable secret the push carries; the test greps the response body for
# it to prove the status-only (no-secret) posture.
_SENTINEL_SECRET = "wJalrXUtnFEMI-SENTINEL-SECRET-EXAMPLEKEY"
_SENTINEL_ACCESS_KEY = "AKIASENTINELEXAMPLE"


def _push_body() -> dict[str, str]:
    """The ``sigv4_static`` arm of the discriminated push body (resolved literals)."""
    return {
        "kind": "sigv4_static",
        "access_key_id": _SENTINEL_ACCESS_KEY,
        "secret_access_key": _SENTINEL_SECRET,
        "region": "us-east-1",
        "service": "s3",
    }


async def _build_instance(
    tmp_path: Path,
    *,
    saturation: SaturationGate | None = None,
    with_signer_creds: bool = True,
) -> tuple[InstanceContext, SqliteCredentialStore | None]:
    """Build an InstanceContext with an ``aws_sigv4`` route and a cred store.

    Mirrors ``test_credential_kicker._build`` but is self-contained here. When
    ``with_signer_creds`` the context carries a started
    :class:`SqliteCredentialStore` tracked for teardown separately (the conftest
    ``track_instance`` only tracks the fixed component set, not ``signer_creds``).
    """
    store = SqliteUploadStore(str(tmp_path / "uploads.db"))
    ram = RamBodyStore()
    fbs = FileBodyStore(tmp_path / "bodies")
    body_store = HybridBodyStore(ram=ram, disk=fbs)
    tokens = SqliteTokenCache(str(tmp_path / "tokens.db"))
    await store.start()
    await body_store.start()
    await tokens.start()

    signer_creds: SqliteCredentialStore | None = None
    if with_signer_creds:
        signer_creds = SqliteCredentialStore(str(tmp_path / "credential_store.db"))
        await signer_creds.start()
        track_started(signer_creds)

    cfg = InstanceCfg(
        id="primary",
        host_prefixes=["*"],
        data_dir="primary",
        routes=[
            RouteCfg(name="sigv4", hosts=[_SIGV4_HOST], auth_mode="aws_sigv4"),
        ],
    )
    sat = saturation or SaturationGate(
        max_in_flight=100,
        max_in_flight_bytes=10_000_000,
        max_disk_bytes=1_000_000_000,
    )
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
        saturation=sat,
        codec_factory=MagicMock(),
        current_settings=snapshot_thunk(make_snapshot()),
        signer_creds=signer_creds,
    )
    return track_instance(instance), signer_creds


def _app_for(dispatcher: InstanceDispatcher) -> FastAPI:
    """Mount the admin router behind ``dispatcher`` (the token-test wiring)."""
    app = FastAPI()
    app.include_router(admin_routes.router)
    admin_routes.register_admin_error_handlers(app)
    app.dependency_overrides[admin_routes.get_dispatcher] = lambda: dispatcher
    return app


def _parked_sigv4_row(*, endpoint: str, body_size_bytes: int = 500) -> UploadRow:
    """An ``auth_expired`` row pinned to ``endpoint`` (the SigV4 host)."""
    now = datetime.now(tz=UTC)
    row_uid = uuid4()
    return UploadRow(
        chain_id=row_uid,
        instance_id="primary",
        group_id=row_uid,
        multifile_id=row_uid,
        send_order=0,
        route_name="sigv4",
        state="auth_expired",
        body_location="ram",
        body_size_bytes=body_size_bytes,
        received_at=now,
        updated_at=now,
        endpoint=endpoint,
        uid="user-1",
        chain_envelope_json="{}",
        idempotency_key=str(row_uid),
        capture_reexecution_active=False,
    )


@pytest.fixture
async def wired() -> AsyncIterator[tuple[FastAPI, InstanceContext, SqliteCredentialStore]]:
    """An app + instance + started cred store, per the standard tmp build."""
    # tmp_path is function-scoped; request it via the inner pytest fixture path.
    import tempfile

    with tempfile.TemporaryDirectory() as tmp:
        instance, signer_creds = await _build_instance(Path(tmp))
        assert signer_creds is not None
        dispatcher = InstanceDispatcher([instance])
        yield _app_for(dispatcher), instance, signer_creds


@pytest.mark.asyncio
async def test_push_stores_credential_and_normalizes_host(
    wired: tuple[FastAPI, InstanceContext, SqliteCredentialStore],
) -> None:
    """A push stores the cred under the NORMALIZED host key and returns 204."""
    app, _instance, signer_creds = wired
    client = TestClient(app)

    # Push with MIXED CASE; the handler must normalize via the same host_key_for the
    # executor's forward lookup uses.
    resp = client.put(f"/v1/admin/credentials/{_SIGV4_HOST_MIXED}", json=_push_body())
    assert resp.status_code == 204, resp.text

    # push-key == lookup-key: the executor looks up HostCredKey(host_key_for(url));
    # host_key_for lower-cases, so the slot must resolve under the LOWER-CASED host.
    row = await signer_creds.get(HostCredKey(_SIGV4_HOST))
    assert row is not None
    assert row.dest_host == _SIGV4_HOST
    assert row.source == "admin_push"
    assert row.status == "fresh"
    # The structured value round-tripped the resolved literals.
    assert row.credential.kind == "sigv4_static"
    assert row.credential.access_key_id == _SENTINEL_ACCESS_KEY
    assert row.credential.secret_access_key == _SENTINEL_SECRET
    assert row.credential.region == "us-east-1"

    # The MIXED-CASE key must NOT be a separate slot — normalization collapsed it.
    assert await signer_creds.get(HostCredKey(_SIGV4_HOST_MIXED)) is None


@pytest.mark.asyncio
async def test_push_returns_status_only_no_secret_in_response(
    wired: tuple[FastAPI, InstanceContext, SqliteCredentialStore],
) -> None:
    """The push response is 204 with an EMPTY body — the secret is never echoed (ADR-004)."""
    app, _instance, _signer_creds = wired
    client = TestClient(app)

    resp = client.put(f"/v1/admin/credentials/{_SIGV4_HOST}", json=_push_body())
    assert resp.status_code == 204
    # 204 carries no body at all.
    assert resp.content == b""
    # Belt-and-suspenders: neither secret nor access key appears anywhere.
    assert _SENTINEL_SECRET not in resp.text
    assert _SENTINEL_ACCESS_KEY not in resp.text


@pytest.mark.asyncio
async def test_push_profile_ref_arm(
    wired: tuple[FastAPI, InstanceContext, SqliteCredentialStore],
) -> None:
    """The ``profile_ref`` arm of the discriminated body maps and stores too."""
    app, _instance, signer_creds = wired
    client = TestClient(app)

    resp = client.put(
        f"/v1/admin/credentials/{_SIGV4_HOST}",
        json={"kind": "profile_ref", "profile": "prod", "region": "us-east-1", "service": "s3"},
    )
    assert resp.status_code == 204, resp.text

    row = await signer_creds.get(HostCredKey(_SIGV4_HOST))
    assert row is not None
    assert row.credential.kind == "profile_ref"
    assert row.credential.profile == "prod"
    assert row.status == "fresh"


@pytest.mark.asyncio
async def test_push_fires_wake_and_requeues_parked_sigv4_row() -> None:
    """The HTTP push fires the store wake → a parked ``aws_sigv4`` row is requeued.

    End-to-end loop-closing proof: a row parked in ``auth_expired`` for the host
    is woken to ``queued`` by the sigv4 kicker that registered on the store,
    purely as a side effect of the admin push's ``set``.
    """
    import tempfile

    with tempfile.TemporaryDirectory() as tmp:
        sat = SaturationGate(max_in_flight=10, max_in_flight_bytes=10_000, max_disk_bytes=1_000_000)
        instance, signer_creds = await _build_instance(Path(tmp), saturation=sat)
        assert signer_creds is not None
        dispatcher = InstanceDispatcher([instance])
        app = _app_for(dispatcher)

        row = _parked_sigv4_row(endpoint=_SIGV4_HOST)
        await instance.store.insert(row)
        assert sat.in_flight == 0

        # The kicker registers its wake handler on the store at construction.
        cred_kicker = Kicker(instance=instance, flavour=AWS_SIGV4_FLAVOUR)
        stop_event = asyncio.Event()
        task = asyncio.create_task(cred_kicker.run(stop_event))

        # The admin push lands fresh creds → set() fires the wake handler.
        client = TestClient(app)
        resp = client.put(f"/v1/admin/credentials/{_SIGV4_HOST_MIXED}", json=_push_body())
        assert resp.status_code == 204, resp.text

        # The parked row should flip auth_expired → queued.
        last = ""
        for _ in range(50):
            await asyncio.sleep(0.02)
            fresh = await instance.store.get(row.chain_id)
            assert fresh is not None
            last = fresh.state
            if last == "queued":
                break
        stop_event.set()
        await asyncio.gather(task, return_exceptions=True)

        assert last == "queued"
        # Re-admitted through the gate (the sender released it at park time).
        assert sat.in_flight == 1
        assert sat.in_flight_bytes == 500


@pytest.mark.asyncio
async def test_push_no_signer_creds_is_graceful_noop() -> None:
    """A bearer-only deployment (signer_creds is None) does not crash — 204 no-op."""
    import tempfile

    with tempfile.TemporaryDirectory() as tmp:
        instance, signer_creds = await _build_instance(Path(tmp), with_signer_creds=False)
        assert signer_creds is None
        dispatcher = InstanceDispatcher([instance])
        app = _app_for(dispatcher)
        client = TestClient(app)

        resp = client.put(f"/v1/admin/credentials/{_SIGV4_HOST}", json=_push_body())
        # Graceful: the push is a no-op, never a 500.
        assert resp.status_code == 204, resp.text
        assert resp.content == b""


@pytest.mark.asyncio
async def test_push_rejects_unknown_kind() -> None:
    """An unknown discriminator is a 422 (the discriminated union rejects it)."""
    import tempfile

    with tempfile.TemporaryDirectory() as tmp:
        instance, signer_creds = await _build_instance(Path(tmp))
        assert signer_creds is not None
        dispatcher = InstanceDispatcher([instance])
        app = _app_for(dispatcher)
        client = TestClient(app)

        resp = client.put(
            f"/v1/admin/credentials/{_SIGV4_HOST}",
            json={"kind": "bearer", "token": "should-not-work"},
        )
        assert resp.status_code == 422
        # The store stays empty — nothing was written.
        assert await signer_creds.get(HostCredKey(_SIGV4_HOST)) is None
        # And the secret-shaped junk is not echoed back (envelope only).
        assert "should-not-work" not in json.dumps(resp.json())


def test_push_body_missing_service_is_value_error() -> None:
    """A SigV4 push body that omits ``service`` fails validation (``missing``).

    The primary provision-time fail-loud: under ``ConfigDict(strict=True)`` the
    required ``service`` field rejects a body with no ``service`` key, so an
    operator can never push a credential of unknown service scope.
    """
    with pytest.raises(ValidationError) as excinfo:
        SigV4StaticCredBody(
            access_key_id=_SENTINEL_ACCESS_KEY,
            secret_access_key=_SENTINEL_SECRET,
            region="us-east-1",
        )
    types = {e["type"] for e in excinfo.value.errors() if e["loc"] == ("service",)}
    assert types == {"missing"}, excinfo.value.errors()


def test_push_body_unknown_service_is_value_error() -> None:
    """A SigV4 push body naming an unknown service fails validation (``value_error``).

    ``"dynamodb"`` is the deliberate wire-coercion boundary (an inbound service
    string that is NOT a known :class:`SigningService`); the before-validator
    rejects it with the clean ``value_error``.
    """
    with pytest.raises(ValidationError) as excinfo:
        SigV4StaticCredBody(
            access_key_id=_SENTINEL_ACCESS_KEY,
            secret_access_key=_SENTINEL_SECRET,
            region="us-east-1",
            service="dynamodb",
        )
    types = {e["type"] for e in excinfo.value.errors() if e["loc"] == ("service",)}
    assert types == {"value_error"}, excinfo.value.errors()


@pytest.mark.asyncio
async def test_push_missing_service_rejected_at_door() -> None:
    """A PUT whose body omits ``service`` is rejected 422 before any store write."""
    import tempfile

    with tempfile.TemporaryDirectory() as tmp:
        instance, signer_creds = await _build_instance(Path(tmp))
        assert signer_creds is not None
        dispatcher = InstanceDispatcher([instance])
        app = _app_for(dispatcher)
        client = TestClient(app)

        resp = client.put(
            f"/v1/admin/credentials/{_SIGV4_HOST}",
            json={
                "kind": "sigv4_static",
                "access_key_id": _SENTINEL_ACCESS_KEY,
                "secret_access_key": _SENTINEL_SECRET,
                "region": "us-east-1",
                # service omitted.
            },
        )
        assert resp.status_code == 422, resp.text
        # Nothing was written — the door rejected the unscoped credential.
        assert await signer_creds.get(HostCredKey(_SIGV4_HOST)) is None
