"""Unit tests for ``phantom_client.client.PhantomClient``.

Every test uses :class:`httpx.MockTransport` so no real network I/O
happens.
"""

from __future__ import annotations

import json
from collections.abc import Callable
from typing import Any
from uuid import UUID, uuid4

import httpx
import pytest
from phantom_client.client import PhantomClient
from phantom_client.config import ClientConfig
from phantom_client.errors import EmptyFilterError
from phantom_client.models.admin import (
    DeleteFilter,
    ExtractFilter,
    ProfileRefCredBody,
    SigningService,
    SigV4StaticCredBody,
)
from phantom_client.models.chain import (
    ChainBodyJson,
    ChainEnvelope,
    ChainStep,
)
from pydantic import ValidationError


def _make_envelope() -> ChainEnvelope:
    chain_id = uuid4()
    return ChainEnvelope(
        chain_id=chain_id,
        idempotency_key=str(chain_id),
        steps=[
            ChainStep(
                name="create_file",
                method="POST",
                url="https://files.example.com/v2/files",
                body=ChainBodyJson(value={"x": 1}),
            ),
        ],
    )


def _ok_chain_response(chain_id: UUID, state: str = "queued") -> httpx.Response:
    return httpx.Response(
        202,
        content=json.dumps(
            {
                "chain_id": str(chain_id),
                "state": state,
                "last_step_completed": None,
                "captured": [],
            }
        ),
        headers={"Content-Type": "application/json"},
    )


def _ok_upload_row(chain_id: UUID, state: str = "queued") -> dict[str, Any]:
    return {
        "chain_id": str(chain_id),
        "instance_id": "primary",
        "group_id": str(chain_id),
        "route_name": "primary",
        "state": state,
        "body_location": "ram",
        "received_at": "2026-01-01T00:00:00+00:00",
        "updated_at": "2026-01-01T00:00:00+00:00",
        "endpoint": "x",
        "uid": "u",
        "idempotency_key": "k",
        "capture_reexecution_active": False,
    }


def _client(
    handler: Callable[[httpx.Request], httpx.Response],
    *,
    config: ClientConfig | None = None,
) -> PhantomClient:
    cfg = config or ClientConfig(phantom_url="http://test")
    return PhantomClient(cfg, transport=httpx.MockTransport(handler))


# ---------------------------------------------------------------------------
# Constructor.
# ---------------------------------------------------------------------------


def test_constructor_dual_shape() -> None:
    """Both the str and ClientConfig forms construct cleanly."""
    c1 = PhantomClient("http://phantom:8080")
    assert isinstance(c1, PhantomClient)
    c2 = PhantomClient(ClientConfig(phantom_url="http://phantom:8080"))
    assert isinstance(c2, PhantomClient)


def test_constructor_string_timeout_override() -> None:
    """timeout=… overrides the read timeout in the simple-constructor."""
    c = PhantomClient("http://phantom:8080", timeout=5.0)
    assert c._config.timeouts.read == 5.0


# ---------------------------------------------------------------------------
# Submit chain.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_submit_chain_round_trip() -> None:
    """submit_chain returns the parsed ChainResponse."""
    envelope = _make_envelope()

    def handler(request: httpx.Request) -> httpx.Response:
        assert str(request.url).endswith("/v1/send")
        return _ok_chain_response(envelope.chain_id)

    async with _client(handler) as client:
        response = await client.submit_chain(envelope, uid="u")
    assert response.chain_id == envelope.chain_id
    assert response.state == "queued"


@pytest.mark.asyncio
async def test_submit_chain_uses_default_uid() -> None:
    """When uid is omitted, ClientConfig.default_uid is used."""
    envelope = _make_envelope()
    captured: dict[str, str] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured.update(dict(request.headers))
        return _ok_chain_response(envelope.chain_id)

    cfg = ClientConfig(phantom_url="http://test", default_uid="cfg-uid")
    async with _client(handler, config=cfg) as client:
        await client.submit_chain(envelope)
    assert captured["x-phantom-uid"] == "cfg-uid"


# ---------------------------------------------------------------------------
# get_upload / list_uploads / find_by_metadata.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_upload() -> None:
    """get_upload returns the parsed ChainAdminDetail for ``chain_id``.

    The admin endpoint returns :class:`ChainAdminDetail` (admin-only,
    loopback) with extra inspection fields beyond the wire-facing
    :class:`ChainResponse`.
    """
    chain_id = uuid4()

    def handler(request: httpx.Request) -> httpx.Response:
        assert f"/v1/admin/chains/{chain_id}" in str(request.url)
        now = "2026-06-10T00:00:00+00:00"
        return httpx.Response(
            200,
            content=json.dumps(
                {
                    "chain_id": str(chain_id),
                    "state": "succeeded",
                    # Cycle-7 task 4.5 row-sourced fields (required on the model).
                    "received_at": now,
                    "updated_at": now,
                    "next_attempt_at": None,
                    "sent_at": "2026-06-10T00:00:05+00:00",
                    "group_id": str(chain_id),
                    "multifile_id": None,
                    "send_order": 0,
                    "body_location": "ram",
                    "last_step_completed": "step",
                    "captured": [],
                    "attempts": 1,
                    "last_error": None,
                }
            ),
            headers={"Content-Type": "application/json"},
        )

    async with _client(handler) as client:
        resp = await client.get_upload(chain_id)
    assert resp.state == "succeeded"
    assert resp.body_location == "ram"
    assert resp.sent_at is not None
    assert resp.group_id == chain_id


@pytest.mark.asyncio
async def test_list_uploads_returns_rows_and_cursor() -> None:
    """list_uploads parses the envelope shape and surfaces both halves."""
    chain_id = uuid4()
    captured: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["url"] = str(request.url)
        return httpx.Response(
            200,
            json={"uploads": [_ok_upload_row(chain_id)], "next_cursor": "abc"},
        )

    async with _client(handler) as client:
        rows, cursor = await client.list_uploads(state="queued", limit=10)
    assert len(rows) == 1
    assert rows[0].state == "queued"
    assert cursor == "abc"
    assert "state=queued" in captured["url"]
    assert "limit=10" in captured["url"]


@pytest.mark.asyncio
async def test_list_uploads_passes_multifile_and_group_filters() -> None:
    """list_uploads threads multifile_id and group_id as query params."""
    chain_id = uuid4()
    multifile_id = uuid4()
    group_id = uuid4()
    captured: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["url"] = str(request.url)
        return httpx.Response(
            200,
            json={"uploads": [_ok_upload_row(chain_id)], "next_cursor": None},
        )

    async with _client(handler) as client:
        rows, cursor = await client.list_uploads(
            multifile_id=multifile_id,
            group_id=group_id,
        )
    assert len(rows) == 1
    assert cursor is None
    assert f"multifile_id={multifile_id}" in captured["url"]
    assert f"group_id={group_id}" in captured["url"]


@pytest.mark.asyncio
async def test_find_by_metadata_passes_key_value() -> None:
    """find_by_metadata threads the key:value pair as a query param."""
    chain_id = uuid4()
    captured: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["url"] = str(request.url)
        return httpx.Response(200, json={"uploads": [_ok_upload_row(chain_id)]})

    async with _client(handler) as client:
        rows = await client.find_by_metadata(key="phantom_local_uuid", value="abc-123")
    assert len(rows) == 1
    assert "key_value_match=phantom_local_uuid%3Aabc-123" in captured["url"]


@pytest.mark.asyncio
async def test_find_by_metadata_quotes_colon_bearing_key() -> None:
    """A colon-bearing key rides the service's quoted-key wire form.

    Round 2 defender fix R2-3: pre-fix the SDK built 'tag:env:prod',
    which the server's first-colon split read as key 'tag', silently
    querying the WRONG key. The encoder now emits '"tag:env":prod'
    (the key as a double-quoted, backslash-escaped string), so every
    legal KVS key addresses exactly.
    """
    chain_id = uuid4()
    captured: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["params"] = dict(request.url.params)
        return httpx.Response(200, json={"uploads": [_ok_upload_row(chain_id)]})

    async with _client(handler) as client:
        rows = await client.find_by_metadata(key="tag:env", value="prod")
    assert len(rows) == 1
    assert captured["params"]["key_value_match"] == '"tag:env":prod'


# ---------------------------------------------------------------------------
# Lifecycle ops.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_replay_returns_upload_row() -> None:
    """replay returns the parsed UploadRow."""
    chain_id = uuid4()
    captured: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["method"] = request.method
        captured["url"] = str(request.url)
        return httpx.Response(200, json=_ok_upload_row(chain_id, state="queued"))

    async with _client(handler) as client:
        row = await client.replay(chain_id)
    assert captured["method"] == "POST"
    assert "/replay" in captured["url"]
    assert row.chain_id == chain_id


@pytest.mark.asyncio
async def test_cancel_returns_upload_row() -> None:
    """cancel returns the parsed UploadRow in cancelled state."""
    chain_id = uuid4()

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=_ok_upload_row(chain_id, state="cancelled"))

    async with _client(handler) as client:
        row = await client.cancel(chain_id)
    assert row.state == "cancelled"


@pytest.mark.asyncio
async def test_delete_upload() -> None:
    """delete_upload sends DELETE on the chain_id path."""
    chain_id = uuid4()
    captured: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["method"] = request.method
        captured["url"] = str(request.url)
        return httpx.Response(204)

    async with _client(handler) as client:
        await client.delete_upload(chain_id)
    assert captured["method"] == "DELETE"
    assert f"/v1/admin/chains/{chain_id}" in captured["url"]


@pytest.mark.asyncio
async def test_bulk_delete_empty_pre_flight() -> None:
    """bulk_delete with an empty filter raises EmptyFilterError without HTTP."""

    def handler(request: httpx.Request) -> httpx.Response:
        raise AssertionError("HTTP should not have been called")

    async with _client(handler) as client:
        with pytest.raises(EmptyFilterError):
            await client.bulk_delete(DeleteFilter())


@pytest.mark.asyncio
async def test_bulk_delete_returns_count() -> None:
    """bulk_delete returns deleted count from BulkDeleteResponse."""

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"deleted": 4})

    async with _client(handler) as client:
        deleted = await client.bulk_delete(DeleteFilter(state="failed"))
    assert deleted == 4


# ---------------------------------------------------------------------------
# Streaming endpoints.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_fetch_body_streams_chunks() -> None:
    """fetch_body yields body bytes."""
    chain_id = uuid4()

    def handler(request: httpx.Request) -> httpx.Response:
        assert "/body" in str(request.url)
        return httpx.Response(200, content=b"binary-blob")

    async with _client(handler) as client:
        body = b""
        async for chunk in await client.fetch_body(chain_id):
            body += chunk
    assert body == b"binary-blob"


@pytest.mark.asyncio
async def test_export_tar_streams() -> None:
    """export_tar yields the tar bytes."""

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=b"tar-archive-bytes")

    async with _client(handler) as client:
        body = b""
        async for chunk in await client.export_tar():
            body += chunk
    assert body == b"tar-archive-bytes"


@pytest.mark.asyncio
async def test_extract_streams_with_filter_body() -> None:
    """extract posts the ExtractFilter and streams the tar response."""

    def handler(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content.decode()) if request.content else {}
        assert body.get("state") == "failed"
        return httpx.Response(200, content=b"filtered-tar")

    async with _client(handler) as client:
        out = b""
        async for chunk in await client.extract(ExtractFilter(state="failed")):
            out += chunk
    assert out == b"filtered-tar"


@pytest.mark.asyncio
async def test_extract_raises_at_the_await_on_an_unstarted_client() -> None:
    """extract's not-started check is EAGER: the raise lands at the await.

    Objective: pin the one observable CL9 could have moved. ``extract`` is a
    coroutine that checks first and then returns a stream, so awaiting it on
    an unstarted client raises immediately; a naive unification that returned
    ``stream_request`` directly would defer that raise to the first
    ``__anext__``, which nothing else in the suite would notice.

    Success: ``await client.extract(...)`` raises RuntimeError with no
    iteration at all.
    """
    client = PhantomClient(ClientConfig(phantom_url="http://test"))
    with pytest.raises(RuntimeError):
        await client.extract(ExtractFilter(state="failed"))


# ---------------------------------------------------------------------------
# Tokens.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_list_tokens_no_bearer_in_response() -> None:
    """list_tokens parses the documented shape; bearer never present."""

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "tokens": [
                    {
                        "endpoint": "x",
                        "uid": "u",
                        "last_updated": "2026-01-01T00:00:00+00:00",
                        "status": "fresh",
                    }
                ]
            },
        )

    async with _client(handler) as client:
        slots = await client.list_tokens()
    assert len(slots) == 1
    assert slots[0].endpoint == "x"
    assert not hasattr(slots[0], "bearer")
    assert not hasattr(slots[0], "token")


@pytest.mark.asyncio
async def test_push_token_uses_put() -> None:
    """push_token sends PUT on the (endpoint, uid) slot."""
    captured: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["method"] = request.method
        captured["url"] = str(request.url)
        captured["body"] = request.content.decode()
        return httpx.Response(204)

    async with _client(handler) as client:
        await client.push_token(endpoint="x", uid="u", token="Bearer SECRET")
    assert captured["method"] == "PUT"
    assert "/v1/admin/tokens/x/u" in captured["url"]
    body = json.loads(captured["body"])
    assert body["token"] == "Bearer SECRET"


@pytest.mark.asyncio
async def test_invalidate_token() -> None:
    """invalidate_token sends DELETE on the slot."""
    captured: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["method"] = request.method
        captured["url"] = str(request.url)
        return httpx.Response(204)

    async with _client(handler) as client:
        await client.invalidate_token(endpoint="x", uid="u")
    assert captured["method"] == "DELETE"


# ---------------------------------------------------------------------------
# Destination credentials (SigV4 re-sign surface).
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_push_credential_static_uses_put() -> None:
    """push_credential PUTs the static body on the host-keyed slot."""
    captured: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["method"] = request.method
        captured["url"] = str(request.url)
        captured["body"] = request.content.decode()
        return httpx.Response(204)

    async with _client(handler) as client:
        await client.push_credential(
            dest_host="s3.amazonaws.com",
            credential=SigV4StaticCredBody(
                access_key_id="AKIAEXAMPLE",
                secret_access_key="wJalrSECRET",
                region="us-east-1",
                service=SigningService.S3,
            ),
        )
    assert captured["method"] == "PUT"
    assert "/v1/admin/credentials/s3.amazonaws.com" in captured["url"]
    body = json.loads(captured["body"])
    assert body["kind"] == "sigv4_static"
    assert body["access_key_id"] == "AKIAEXAMPLE"
    assert body["secret_access_key"] == "wJalrSECRET"
    assert body["region"] == "us-east-1"
    # The SigningService member serializes to its wire string.
    assert body["service"] == "s3"


@pytest.mark.asyncio
async def test_push_credential_profile_ref_uses_put() -> None:
    """push_credential PUTs the profile-ref body (the second discriminated arm)."""
    captured: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["method"] = request.method
        captured["url"] = str(request.url)
        captured["body"] = request.content.decode()
        return httpx.Response(204)

    async with _client(handler) as client:
        await client.push_credential(
            dest_host="s3.amazonaws.com",
            credential=ProfileRefCredBody(
                profile="prod",
                region="eu-west-1",
                service=SigningService.S3,
            ),
        )
    assert captured["method"] == "PUT"
    assert "/v1/admin/credentials/s3.amazonaws.com" in captured["url"]
    body = json.loads(captured["body"])
    assert body["kind"] == "profile_ref"
    assert body["profile"] == "prod"
    assert body["region"] == "eu-west-1"
    assert body["service"] == "s3"


@pytest.mark.asyncio
async def test_push_credential_quotes_dest_host() -> None:
    """A dest_host with reserved characters is path-escaped (quote safe='')."""
    captured: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["url"] = str(request.url)
        return httpx.Response(204)

    async with _client(handler) as client:
        await client.push_credential(
            dest_host="host:9000/weird",
            credential=ProfileRefCredBody(service=SigningService.S3),
        )
    # ':' -> %3A and '/' -> %2F so the host is one path segment, not a sub-path.
    assert "/v1/admin/credentials/host%3A9000%2Fweird" in captured["url"]


def test_cred_body_rejects_raw_service_string() -> None:
    """The N6 contract: the strict client model has NO coercer.

    A raw ``service="s3"`` string is rejected (``is_instance_of`` under
    ``strict=True``); callers MUST pass a :class:`SigningService` member, which
    serializes back to the ``"s3"`` wire string. This pins the deliberate
    omission of the server's ``@field_validator("service", mode="before")``.
    """
    with pytest.raises(ValidationError):
        SigV4StaticCredBody(
            access_key_id="AKIAEXAMPLE",
            secret_access_key="wJalrSECRET",
            region="us-east-1",
            service="s3",  # type: ignore[arg-type]  # raw string is the rejected case
        )

    ok = SigV4StaticCredBody(
        access_key_id="AKIAEXAMPLE",
        secret_access_key="wJalrSECRET",
        region="us-east-1",
        service=SigningService.S3,
    )
    # The member round-trips to the wire string the server's body accepts
    # (the two schemas are intentionally duplicated per ADR-012 — the
    # client emits "s3", the server's coercer ingests it).
    assert json.loads(ok.model_dump_json())["service"] == "s3"


def test_cred_body_requires_service() -> None:
    """``service`` is a required field — a body built without it fails client-side."""
    with pytest.raises(ValidationError):
        SigV4StaticCredBody(  # type: ignore[call-arg]  # missing required 'service'
            access_key_id="AKIAEXAMPLE",
            secret_access_key="wJalrSECRET",
            region="us-east-1",
        )


# ---------------------------------------------------------------------------
# Status.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_admin_status() -> None:
    """get_admin_status returns AdminStatusResponse."""

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "ready": True,
                "disk_usage_bytes": 1024,
                "total_backlog": 2,
                "instances": [{"id": "primary", "refresh_strategy": "wait", "in_flight": 1}],
            },
        )

    async with _client(handler) as client:
        status = await client.get_admin_status()
    assert status.ready is True
    assert status.total_backlog == 2


@pytest.mark.asyncio
async def test_get_instance_status() -> None:
    """get_instance_status hits the per-instance path."""
    captured: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["url"] = str(request.url)
        return httpx.Response(
            200,
            json={
                "id": "primary",
                "ready": True,
                "in_flight": {"count": 1, "bytes": 1024},
                "by_state": {
                    "queued": {"count": 1, "bytes": 1024},
                    "attempting": {"count": 0, "bytes": 0},
                    "auth_expired": {"count": 0, "bytes": 0},
                    "stored": {"count": 0, "bytes": 0},
                    "succeeded_recent": {"count": 0, "bytes": 0},
                    "failed_recent": {"count": 0, "bytes": 0},
                },
                "auth": {"phantom_token_expires_at": None, "auth_expired_count": 0},
                "disk_usage_bytes": 1024,
            },
        )

    async with _client(handler) as client:
        status = await client.get_instance_status("primary")
    assert "/v1/admin/instances/primary/status" in captured["url"]
    assert status.id == "primary"
    assert status.in_flight.count == 1
    assert status.by_state.queued.count == 1


@pytest.mark.asyncio
async def test_get_health_and_ready_and_stats() -> None:
    """Health, ready, and stats endpoints parse correctly."""

    def handler(request: httpx.Request) -> httpx.Response:
        # R12-1: liveness/readiness are public paths on the ingress base_url.
        if str(request.url).endswith("/v1/healthz"):
            return httpx.Response(200, json={"status": "ok", "version": "0.1.0"})
        if str(request.url).endswith("/v1/readyz"):
            return httpx.Response(200, json={"ready": True, "detail": None})
        if str(request.url).endswith("/v1/admin/stats"):
            return httpx.Response(
                200,
                json={
                    "in_flight": {"count": 0, "bytes": 0},
                    "by_state": {
                        "queued": {"count": 0, "bytes": 0},
                        "attempting": {"count": 0, "bytes": 0},
                        "auth_expired": {"count": 0, "bytes": 0},
                        "stored": {"count": 0, "bytes": 0},
                        "succeeded_recent": {"count": 0, "bytes": 0},
                        "failed_recent": {"count": 0, "bytes": 0},
                    },
                    "body_location": {
                        "ram": {"count": 0, "bytes": 0},
                        "file": {"count": 0, "bytes": 0},
                    },
                    "saturation": {
                        "max_in_flight": 100,
                        "max_in_flight_bytes": 0,
                        "saturated": False,
                    },
                    "auth": {
                        "phantom_token_expires_at": None,
                        "auth_expired_count": 0,
                    },
                    "parked_total": 0,
                },
            )
        raise AssertionError(f"unknown path: {request.url}")

    async with _client(handler) as client:
        h = await client.get_health()
        r = await client.get_ready()
        s = await client.get_stats()
    assert h.status == "ok"
    assert r.ready is True
    assert s.saturation.max_in_flight == 100
    assert s.parked_total == 0


@pytest.mark.asyncio
async def test_list_instances() -> None:
    """list_instances parses the envelope."""

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "instances": [
                    {"id": "primary", "refresh_strategy": "wait", "in_flight": 0},
                ]
            },
        )

    async with _client(handler) as client:
        instances = await client.list_instances()
    assert len(instances) == 1
    assert instances[0].id == "primary"


# ---------------------------------------------------------------------------
# Transport injection.
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# Quarantine inventory + restore (plan § 5.2.5 / cycle-7 seam 2).
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_quarantine_inventory_passes_instance_param() -> None:
    """get_quarantine_inventory forwards ``?instance=`` and parses the response."""
    captured: dict[str, Any] = {}
    backup_id = "12345678-1234-5678-1234-567812345678"

    def handler(request: httpx.Request) -> httpx.Response:
        captured["method"] = request.method
        captured["url"] = str(request.url)
        return httpx.Response(
            200,
            json={
                "quarantines": [
                    {
                        "backup_id": backup_id,
                        "reason": "mode_switch",
                        "iso_display": "20260527T140000Z",
                        "db_path": (
                            "/var/lib/phantom/primary/uploads.mode_switch.20260527T140000Z-12345678.db"
                        ),
                        "body_path": (
                            "/var/lib/phantom/primary/bodies.mode_switch.20260527T140000Z-12345678"
                        ),
                        "has_db": True,
                        "has_body": True,
                        "bytes": 9,
                        "anomaly": False,
                    }
                ]
            },
        )

    async with _client(handler) as client:
        resp = await client.get_quarantine_inventory(instance="primary")
    assert captured["method"] == "GET"
    assert "/v1/admin/quarantine" in captured["url"]
    assert "instance=primary" in captured["url"]
    assert resp.quarantines[0].reason == "mode_switch"
    assert resp.quarantines[0].backup_id == UUID(backup_id)
    assert resp.quarantines[0].iso_display == "20260527T140000Z"
    assert resp.quarantines[0].anomaly is False


@pytest.mark.asyncio
async def test_restore_quarantine_backup_sends_backup_id_query_param() -> None:
    """restore_quarantine_backup sends backup_id as a query param (no body)."""
    captured: dict[str, Any] = {}
    backup_id = UUID("12345678-1234-5678-1234-567812345678")

    def handler(request: httpx.Request) -> httpx.Response:
        captured["method"] = request.method
        captured["url"] = str(request.url)
        captured["content"] = request.content
        return httpx.Response(
            200,
            json={
                "restored_db": "/var/lib/phantom/primary/uploads.db",
                "restored_body": "/var/lib/phantom/primary/bodies",
                "interim_backup_db": None,
                "interim_backup_body": None,
                "restart_required": True,
                "detail": f"Restored backup {backup_id} into instance primary.",
            },
        )

    async with _client(handler) as client:
        resp = await client.restore_quarantine_backup(backup_id=backup_id, instance="primary")
    assert captured["method"] == "POST"
    assert "/v1/admin/quarantine/restore" in captured["url"]
    assert "instance=primary" in captured["url"]
    assert f"backup_id={backup_id}" in captured["url"]
    # The restore request carries NO JSON body (cycle-7 seam 2).
    assert captured["content"] == b""
    assert resp.restored_db == "/var/lib/phantom/primary/uploads.db"
    assert resp.restart_required is True
    assert resp.interim_backup_db is None


@pytest.mark.asyncio
async def test_transport_injection_no_network() -> None:
    """MockTransport prevents any real network from being touched."""
    envelope = _make_envelope()
    calls = {"count": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        calls["count"] += 1
        return _ok_chain_response(envelope.chain_id)

    async with _client(handler) as client:
        await client.submit_chain(envelope)
    assert calls["count"] == 1


# ---------------------------------------------------------------------------
# Smoke test: import + construct.
# ---------------------------------------------------------------------------


def test_smoke_import_and_construct() -> None:
    """The package imports cleanly and PhantomClient instantiates."""
    import phantom_client  # noqa: F401  # ensures top-level import works

    client = PhantomClient("http://127.0.0.1:8080")
    assert client._config.phantom_url == "http://127.0.0.1:8080"


# ---------------------------------------------------------------------------
# Group rollup + either-identifier lookups (cycle-7 task 5.1).
# ---------------------------------------------------------------------------


def _group_status_json(group_id: UUID, *, all_finished: bool) -> dict[str, Any]:
    """Minimal rollup body satisfying the strict GroupStatusResponse."""
    now = "2026-06-10T00:00:00+00:00"
    return {
        "group_id": str(group_id),
        "total": 1,
        "counts_by_state": {"succeeded" if all_finished else "queued": 1},
        "all_finished": all_finished,
        "first_received_at": now,
        "last_sent_at": now if all_finished else None,
        "members": [
            {
                "chain_id": str(uuid4()),
                "state": "succeeded" if all_finished else "queued",
                "received_at": now,
                "sent_at": now if all_finished else None,
                "attempts": 1,
                "last_error": None,
                "send_order": 0,
                "multifile_id": None,
            }
        ],
    }


def _lookup_json(kind: str, value: str, *, found: bool) -> dict[str, Any]:
    """Minimal lookup body satisfying the strict IdentifierLookupResponse."""
    now = "2026-06-10T00:00:00+00:00"
    matches = []
    if found:
        matches.append(
            {
                "chain_id": str(uuid4()),
                "state": "succeeded",
                "received_at": now,
                "sent_at": now,
                "attempts": 1,
                "last_error": None,
                "instance_id": "primary",
                "multifile_id": None,
                "send_order": 0,
                "captured_file_id": value if kind == "captured_file_id" else None,
                "local_uuid": value if kind == "local_uuid" else None,
            }
        )
    return {"kind": kind, "value": value, "found": found, "matches": matches}


@pytest.mark.asyncio
async def test_get_group_status_path_and_parse() -> None:
    """get_group_status hits /v1/admin/groups/{id} and parses the rollup."""
    group_id = uuid4()

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == f"/v1/admin/groups/{group_id}"
        assert request.url.params.get("instance") is None
        return httpx.Response(200, json=_group_status_json(group_id, all_finished=True))

    async with _client(handler) as client:
        rollup = await client.get_group_status(group_id)
    assert rollup.group_id == group_id
    assert rollup.all_finished is True
    assert rollup.total == 1
    assert rollup.members[0].state == "succeeded"


@pytest.mark.asyncio
async def test_get_group_status_threads_instance_param() -> None:
    """instance= becomes the ?instance= query parameter."""
    group_id = uuid4()

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.params["instance"] == "primary"
        return httpx.Response(200, json=_group_status_json(group_id, all_finished=False))

    async with _client(handler) as client:
        rollup = await client.get_group_status(group_id, instance="primary")
    assert rollup.all_finished is False


@pytest.mark.asyncio
async def test_get_group_status_404_raises_not_found() -> None:
    """An unknown group raises PhantomNotFoundError (canonical envelope)."""
    from phantom_client.errors import PhantomNotFoundError

    group_id = uuid4()

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            404,
            json={
                "error": {
                    "code": "not_found",
                    "message": f"group {group_id} not found",
                    "request_id": "r",
                    "instance_id": "unrouted",
                    "details": {},
                }
            },
        )

    async with _client(handler) as client:
        with pytest.raises(PhantomNotFoundError):
            await client.get_group_status(group_id)


@pytest.mark.asyncio
async def test_find_by_local_uuid_path_and_hit() -> None:
    """find_by_local_uuid hits the by-local-uuid path and parses a hit."""
    local_uuid = uuid4()

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == f"/v1/admin/uploads/by-local-uuid/{local_uuid}"
        return httpx.Response(200, json=_lookup_json("local_uuid", str(local_uuid), found=True))

    async with _client(handler) as client:
        result = await client.find_by_local_uuid(local_uuid)
    assert result.kind == "local_uuid"
    assert result.found is True
    assert len(result.matches) == 1
    assert result.matches[0].local_uuid == local_uuid


@pytest.mark.asyncio
async def test_find_by_local_uuid_miss_is_found_false() -> None:
    """A miss is HTTP 200 with found=false, never an exception."""
    local_uuid = uuid4()

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=_lookup_json("local_uuid", str(local_uuid), found=False))

    async with _client(handler) as client:
        result = await client.find_by_local_uuid(local_uuid, instance="primary")
    assert result.found is False
    assert result.matches == []


@pytest.mark.asyncio
async def test_find_by_captured_id_quotes_path_segment() -> None:
    """The opaque upstream id is percent-encoded into one path segment."""
    raw_value = "ids/with strange:chars"

    def handler(request: httpx.Request) -> httpx.Response:
        # httpx.URL.path decodes percent-escapes; the raw target must
        # carry the encoded single segment (no extra path separators).
        assert b"/v1/admin/uploads/by-captured-id/ids%2Fwith%20strange%3Achars" in (
            request.url.raw_path
        )
        return httpx.Response(200, json=_lookup_json("captured_file_id", raw_value, found=True))

    async with _client(handler) as client:
        result = await client.find_by_captured_id(raw_value)
    assert result.kind == "captured_file_id"
    assert result.value == raw_value
    assert result.matches[0].captured_file_id == raw_value


@pytest.mark.asyncio
async def test_find_by_captured_id_unconfigured_raises_bad_request() -> None:
    """lookup_not_configured maps to PhantomBadRequestError (HTTP 400)."""
    from phantom_client.errors import PhantomBadRequestError

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            400,
            json={
                "error": {
                    "code": "lookup_not_configured",
                    "message": "instance primary carries no admin_lookup binding",
                    "request_id": "r",
                    "instance_id": "primary",
                    "details": {"instances": ["primary"]},
                }
            },
        )

    async with _client(handler) as client:
        with pytest.raises(PhantomBadRequestError):
            await client.find_by_captured_id("anything")


@pytest.mark.asyncio
async def test_poll_group_until_finished_flips_mid_poll() -> None:
    """The client wrapper loops the rollup until all_finished flips true."""
    group_id = uuid4()
    calls = {"count": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        calls["count"] += 1
        assert request.url.path == f"/v1/admin/groups/{group_id}"
        finished = calls["count"] >= 3
        return httpx.Response(200, json=_group_status_json(group_id, all_finished=finished))

    async with _client(handler) as client:
        rollup = await client.poll_group_until_finished(group_id, initial_delay_seconds=0.0)
    assert calls["count"] == 3
    assert rollup.all_finished is True
